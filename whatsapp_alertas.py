"""
Módulo de alertas via WhatsApp + Pushover + Twilio para monitoramento de
galpões, status de grupos para os ESP32 de sirene, e alertas de
temperatura (push remoto) pros dois tipos de cliente.

Fluxo:
- O servidor principal chama `marcar_online(galpao_id, temperatura)` a
  cada checkin do ESP32.
- Uma rotina de verificação (chamada a cada poucos segundos) checa:
    1. a ESCALADA de alerta de queda de energia (ver tabela de tempos
       abaixo) -- vários estágios, cada um medido a partir do tempo real
       desde o último checkin bem-sucedido daquele galpão.
    2. se a temperatura de algum galpão ultrapassou o limite configurado
       -> push remoto (Expo) pro(s) dispositivo(s) responsável(is)
- Quando o galpão volta a responder, dispara "energia reestabelecida" e
  reseta toda a escalada de queda (sirene, Pushover, Twilio).
- `status_grupo(grupo_id)` é consultado pelos ESP32 de sirene.

Os dados de cliente (contatos, canais ativos, galpões, limites) NÃO ficam
mais num dict fixo aqui — vêm do módulo `clientes.py`, que lê/escreve os
JSONs no disco persistente.

===================== ESCALADA DE ALERTA DE QUEDA =====================

Linha do tempo, toda medida a partir do ÚLTIMO checkin bem-sucedido (não a
partir do instante em que o loop de monitoramento "percebe" a queda, que
só roda a cada 5s -- medir a partir do checkin real evita acumular atraso
a cada ciclo):

    300s   -> considera em queda (mínimo pra qualquer aviso: quedas de
              menos de 300s são ignoradas). Avisa o APP (push local), liga
              a SIRENE e faz o primeiro envio de WhatsApp + Pushover pros
              contatos/cliente (autoatendimento: só Pushover, não tem
              WhatsApp cadastrado).
    300s, 600s, 900s, ... -> Pushover REPETE a cada 300s (WhatsApp não
              repete) enquanto a queda continuar, até completar 120min.
    7200s (120min) -> mensagem final "queda prolongada" por WhatsApp +
              Pushover, encerrando o ciclo de repetição. A SIRENE continua
              ligada -- essa mensagem só para de insistir no celular, não
              significa que o problema acabou. O galpão passa a carregar
              a marca `queda_prolongada=True` (ver `status_galpoes_cliente`)
              pra uso futuro do app, se quiser exibir isso diferente.
    600s   -> Twilio começa a ligar (ver twilio_alertas.py — NIVEIS_LIGACAO
              = 600/1200/1800s = 10/20/30min), repete a cada 10min, até 3
              tentativas, parando antes se alguém atender.

Quando o galpão volta a responder (`marcar_online` detecta reconexão),
toda essa escalada é zerada (`_estado_queda.pop` + `twilio_alertas.
resetar_escalonamento`) — a próxima queda começa do zero.

===================== FILA DE ALERTAS (envio assíncrono) =====================

Todo envio de alerta externo (WhatsApp, Pushover, push Expo) passa por uma
fila processada numa thread de fundo dedicada, NUNCA direto na thread que
está atendendo a requisição HTTP do checkin. Motivo: cada chamada de API
externa tem timeout de até 10s, e `marcar_online()`/`checar_limites_*()`
são chamados DE DENTRO da rota `/checkin/<galpao_id>` — se esses envios
rodassem direto ali, um galpão voltando de queda com vários contatos
cadastrados (cada um podendo levar até 10s de WhatsApp + 10s de Pushover)
podia travar a resposta ao ESP32 por bem mais de 10s, e com só 1 worker/
thread no Gunicorn isso bloqueava TODOS os outros checkins até terminar.

Enfileirando, o checkin responde na hora pro ESP32, e o envio (lento,
sujeito a timeout de API externa) acontece em paralelo, fora do caminho
da requisição. `iniciar_fila_alertas()` é chamada pelo mesmo hook
`post_fork` do Gunicorn que já inicia as outras threads de fundo (ver
`_iniciar_threads_de_fundo()` em servidor.py) — pelo mesmo motivo de
sempre: precisa nascer no processo operário, não no mestre.
"""

import os
import time
import queue
import threading
import requests

import twilio_alertas
import clientes
import push_expo
import relatorio_conexoes

# ===================== CONFIGURAÇÃO =====================

WHATSAPP_TOKEN = os.environ["WHATSAPP_TOKEN"]
PHONE_NUMBER_ID = os.environ.get("PHONE_NUMBER_ID", "SEU_PHONE_NUMBER_ID_AQUI")
WHATSAPP_API_URL = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

PUSHOVER_API_TOKEN = os.environ.get("PUSHOVER_API_TOKEN", "")
PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"

# Ver a tabela de tempos completa no topo do arquivo.
TIMEOUT_QUEDA_SEGUNDOS = 300                        # considera em queda; mínimo pra qualquer aviso (app, sirene, WhatsApp, Pushover)
TIMEOUT_SIRENE_SEGUNDOS = 300                        # liga sirene + 1º WhatsApp/Pushover
INTERVALO_REENVIO_PUSHOVER_QUEDA_SEGUNDOS = 300      # Pushover repete nesse intervalo
TIMEOUT_QUEDA_PROLONGADA_SEGUNDOS = 120 * 60         # mensagem final, encerra repetição
# Log silencioso (sem alerta nenhum) de galpão que ficou SEM checkin entre
# esse limite e TIMEOUT_QUEDA_SEGUNDOS e voltou sozinho. Pra consultar no
# Render, buscar por: SEM_CONEXAO. Passou de TIMEOUT_QUEDA_SEGUNDOS já é
# queda de verdade, e aparece nos logs de queda, não nessa tag.
LIMITE_LOG_SEM_CONEXAO_SEGUNDOS = 180
# O início das ligações Twilio (600s) e os níveis seguintes (10/20/30min)
# estão em twilio_alertas.NIVEIS_LIGACAO — não precisam ser repetidos aqui.

# Grupos de sirene continuam configurados aqui (não fazem parte do
# cadastro de cliente — podem cobrir vários clientes ou um subconjunto,
# dependendo de onde a sirene física fica instalada).
GRUPOS_SIRENE = {
    "grupo_1": ["01", "02", "03", "04"],
    "grupo_2": ["05", "06", "07", "08"],
    "grupo_teste": ["TESTE01"],  # usado pra testar a sirene em bancada
    "grupo_dirlei": [
        "XV100CPM", "XV100O14", "XV101GDP", "XV103J1F",
        "XV104ZL8", "XV1062RY", "XV107XQ3",
    ],  # cliente Dirlei Lembeck
}


def _contatos_do_galpao(galpao_id: str):
    _, cliente = clientes.galpao_pertence_a(galpao_id)
    if cliente:
        return cliente.get("contatos", []), cliente
    return [], None


def _telefone_e164(contato: dict):
    numero = contato.get("telefone_twilio") or contato.get("whatsapp")
    if not numero:
        return None
    return numero if numero.startswith("+") else f"+{numero}"


# ===================== FILA DE ALERTAS =====================

_fila_alertas = queue.Queue()


def _processar_fila_alertas():
    """Roda pra sempre numa thread de fundo, consumindo um item por vez.
    Qualquer exceção (API externa fora do ar, timeout, etc) é só logada —
    não pode matar a thread, senão a fila para de ser processada pra
    sempre e os alertas seguintes acumulam sem nunca serem enviados."""
    while True:
        funcao, args, kwargs = _fila_alertas.get()
        try:
            funcao(*args, **kwargs)
        except Exception as e:
            print(f"[whatsapp_alertas] ERRO ao processar item da fila de alertas: {e}")
        finally:
            _fila_alertas.task_done()


def iniciar_fila_alertas():
    """Chame uma vez, a partir de `_iniciar_threads_de_fundo()` em
    servidor.py (que por sua vez é chamada pelo post_fork do Gunicorn —
    ver gunicorn.conf.py). NÃO chame na importação do módulo, pelo mesmo
    motivo documentado em servidor.py: nasceria no processo mestre."""
    threading.Thread(target=_processar_fila_alertas, daemon=True).start()


def _enfileirar(funcao, *args, **kwargs):
    """Agenda `funcao(*args, **kwargs)` pra rodar na thread de fundo da
    fila, em vez de rodar agora mesmo — usado em todo ponto que dispara
    envio de WhatsApp/Pushover/push, pra nunca bloquear quem chamou
    (a rota de checkin, a thread de monitoramento de queda, etc)."""
    _fila_alertas.put((funcao, args, kwargs))


# ===================== ESTADO INTERNO (em memória) =====================

_lock = threading.Lock()
_ultimo_checkin = {}       # galpao_id -> timestamp do último checkin
_em_queda = {}              # galpao_id -> bool (true a partir de TIMEOUT_QUEDA_SEGUNDOS)
_ultima_temperatura = {}    # galpao_id -> {"valor": float, "timestamp": float}
_ultimo_alerta_temp = {}    # galpao_id ou (codigo, push_token) -> bool (já alertado nessa violação?)
_historico_temperatura = {}  # galpao_id -> lista de {"valor": float, "timestamp": float}

# Controle da escalada de alerta de queda (ver tabela de tempos no topo do
# arquivo). galpao_id -> {
#   "notificado_app": bool,      -- já mandou o push de 300s pro app?
#   "sirene_disparada": bool,    -- já passou de 300s (sirene + 1º envio)?
#   "ultimo_pushover": float,    -- timestamp do último Pushover de queda
#   "prolongada": bool,          -- já passou de 120min (encerrou o ciclo)?
# }
# Populado/consultado só pela thread de monitoramento (verificar_quedas)
# e limpo em marcar_online() quando o galpão volta -- sempre sob _lock.
_estado_queda = {}

# Alerta de temperatura pendente de confirmação — SÓ pra clientes
# gerenciados (autoatendimento não tem botão "Resolvido" na tela, é
# self-service). galpao_id -> {"ultimo_envio": ts, "confirmado": bool}.
# Enquanto "confirmado" for False e a violação continuar ativa,
# `reenviar_alertas_pendentes()` reenvia o push a cada
# REENVIO_ALERTA_SEGUNDOS. Sai do dict quando a temperatura volta ao
# normal (não precisa mais confirmar nada) ou quando alguém aperta
# "Resolvido" no app.
_alerta_temp_pendente = {}
REENVIO_ALERTA_SEGUNDOS = 5 * 60

# O ESP32 manda checkin a cada 10s, mas guardar TODOS os pontos faria o
# histórico virar só uns 50min de dados. Em vez disso, guarda só 1 ponto a
# cada INTERVALO_HISTORICO_SEGUNDOS — com 5min de intervalo e 288 pontos,
# cobre as últimas 24h de verdade (bate com o texto "Últimas 24 horas" na
# tela de gráfico do app).
INTERVALO_HISTORICO_SEGUNDOS = 5 * 60
HISTORICO_MAX_PONTOS = 288


def _enviar_pushover(user_key: str, titulo: str, mensagem: str, prioridade_alta: bool = False):
    dados = {"token": PUSHOVER_API_TOKEN, "user": user_key, "title": titulo, "message": mensagem}
    if prioridade_alta:
        dados["priority"] = 2
        dados["retry"] = 60
        dados["expire"] = 600
    try:
        resp = requests.post(PUSHOVER_API_URL, data=dados, timeout=10)
        if resp.status_code != 200:
            print(f"[whatsapp_alertas] Erro Pushover para {user_key}: {resp.text}")
        return resp.ok
    except Exception as e:
        print(f"[whatsapp_alertas] Exceção Pushover para {user_key}: {e}")
        return False


def _enviar_template(numero_destino: str, nome_template: str, parametros: list[str]):
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": numero_destino,
        "type": "template",
        "template": {
            "name": nome_template,
            "language": {"code": "pt_BR"},
            "components": [{"type": "body", "parameters": [{"type": "text", "text": p} for p in parametros]}],
        },
    }
    try:
        resp = requests.post(WHATSAPP_API_URL, headers=headers, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"[whatsapp_alertas] Erro ao enviar para {numero_destino}: {resp.text}")
        return resp.ok
    except Exception as e:
        print(f"[whatsapp_alertas] Exceção ao enviar para {numero_destino}: {e}")
        return False


def _enviar_push_multicanal(push_tokens_com_canal: list, titulo: str, corpo: str, dados: dict | None = None) -> None:
    """`push_tokens_com_canal` é a lista guardada em cliente['push_tokens']
    — cada item é {"token": ..., "canal": ...} (o canal/som escolhido
    naquele aparelho especificamente, guardado localmente nele). Agrupa
    por canal e manda um lote por canal, pra cada celular tocar o som que
    escolheu — sem precisar de nenhuma preferência sincronizada no
    perfil do cliente."""
    grupos: dict[str, list[str]] = {}
    for item in push_tokens_com_canal or []:
        if isinstance(item, str):  # compatibilidade com registros antigos
            token, canal = item, "padrao1"
        else:
            token, canal = item.get("token"), item.get("canal", "padrao1")
        if not token:
            continue
        grupos.setdefault(canal, []).append(token)

    for canal, tokens in grupos.items():
        push_expo.enviar_push_varios(tokens, titulo, corpo, dados=dados, canal=canal)


def _notificar_app_queda(galpao_id: str):
    """300s: aviso pro app (push local), enviado uma única vez por queda,
    no mesmo instante em que sai o WhatsApp/Pushover (`_alertar_queda`)."""
    _, cliente = _contatos_do_galpao(galpao_id)
    if cliente:
        galpao = next((g for g in cliente.get("galpoes", []) if g["id"] == galpao_id), None)
        if galpao and galpao.get("notificar_queda"):
            _enviar_push_multicanal(
                cliente.get("push_tokens", []),
                titulo=f"⚠️ Sem sinal — {galpao['nome']}",
                corpo="O galpão parou de responder. Pode ser queda de energia — verificação necessária.",
                dados={"tipo": "queda_energia", "galpao_id": galpao_id},
            )

    registro = clientes.listar_autoatendimento().get(galpao_id)
    if registro:
        for dispositivo in registro["dispositivos"]:
            if not dispositivo.get("notificar_queda"):
                continue
            push_expo.enviar_push(
                dispositivo["push_token"],
                titulo=f"⚠️ Sem sinal — {galpao_id}",
                corpo="O equipamento parou de responder. Pode ser queda de energia — verificação necessária.",
                dados={"tipo": "queda_energia", "codigo": galpao_id},
            )


def _alertar_queda(galpao_id: str):
    """300s: primeiro envio de WhatsApp + Pushover pros contatos/cliente
    (a sirene já foi ligada nesse mesmo instante, controlada direto por
    `status_grupo` a partir de `_estado_queda`). Autoatendimento não tem
    WhatsApp cadastrado, então recebe só Pushover. Chamada só através da
    fila (`_enfileirar`)."""
    contatos, cliente = _contatos_do_galpao(galpao_id)
    if cliente:
        for contato in contatos:
            if cliente.get("whatsapp_ativo") and contato.get("whatsapp"):
                _enviar_template(contato["whatsapp"], "alerta_queda_energia", [galpao_id])
            if cliente.get("pushover_ativo") and contato.get("pushover"):
                _enviar_pushover(contato["pushover"], "⚠️ Queda de energia",
                                  f"Galpão {galpao_id} sem energia. Verificação necessária.", prioridade_alta=True)
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enviar_pushover(cliente["pushover_user_key"], "⚠️ Queda de energia",
                              f"Galpão {galpao_id} sem energia. Verificação necessária.", prioridade_alta=True)
        print(f"[whatsapp_alertas] Alerta de QUEDA (300s, 1º envio) disparado para galpão {galpao_id}")

    registro = clientes.listar_autoatendimento().get(galpao_id)
    if registro:
        for dispositivo in registro["dispositivos"]:
            if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
                _enviar_pushover(
                    dispositivo["pushover_user_key"], "⚠️ Queda de energia",
                    f"Equipamento {galpao_id} sem energia. Verificação necessária.", prioridade_alta=True,
                )


def _reenviar_pushover_queda(galpao_id: str):
    """Repetição a cada INTERVALO_REENVIO_PUSHOVER_QUEDA_SEGUNDOS (só
    Pushover — WhatsApp não repete), enquanto a queda continuar, até
    completar TIMEOUT_QUEDA_PROLONGADA_SEGUNDOS."""
    contatos, cliente = _contatos_do_galpao(galpao_id)
    if cliente:
        for contato in contatos:
            if cliente.get("pushover_ativo") and contato.get("pushover"):
                _enviar_pushover(contato["pushover"], "⚠️ Queda de energia (continua)",
                                  f"Galpão {galpao_id} ainda sem energia.", prioridade_alta=True)
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enviar_pushover(cliente["pushover_user_key"], "⚠️ Queda de energia (continua)",
                              f"Galpão {galpao_id} ainda sem energia.", prioridade_alta=True)

    registro = clientes.listar_autoatendimento().get(galpao_id)
    if registro:
        for dispositivo in registro["dispositivos"]:
            if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
                _enviar_pushover(
                    dispositivo["pushover_user_key"], "⚠️ Queda de energia (continua)",
                    f"Equipamento {galpao_id} ainda sem energia.", prioridade_alta=True,
                )


def _alertar_queda_prolongada(galpao_id: str):
    """120min: mensagem final, por WhatsApp + Pushover, encerrando o ciclo
    de repetição. A SIRENE continua ligada -- essa mensagem só para de
    insistir no celular, não indica que o problema acabou.

    IMPORTANTE: usa o template do WhatsApp "queda_energia_prolongada",
    que ainda PRECISA SER CRIADO no Meta Business Manager (mesmo processo
    dos templates existentes "alerta_queda_energia"/"energia_
    reestabelecida"/"confirmacao_semanal") -- sem isso, essa chamada vai
    falhar silenciosamente (só loga o erro, não trava nada)."""
    contatos, cliente = _contatos_do_galpao(galpao_id)
    if cliente:
        for contato in contatos:
            if cliente.get("whatsapp_ativo") and contato.get("whatsapp"):
                _enviar_template(contato["whatsapp"], "queda_energia_prolongada", [galpao_id])
            if cliente.get("pushover_ativo") and contato.get("pushover"):
                _enviar_pushover(contato["pushover"], "⚠️ Queda de energia prolongada",
                                  f"Galpão {galpao_id} sem energia há mais de 2 horas.", prioridade_alta=True)
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enviar_pushover(cliente["pushover_user_key"], "⚠️ Queda de energia prolongada",
                              f"Galpão {galpao_id} sem energia há mais de 2 horas.", prioridade_alta=True)
        print(f"[whatsapp_alertas] Alerta de QUEDA PROLONGADA (120min) disparado para galpão {galpao_id}")

    registro = clientes.listar_autoatendimento().get(galpao_id)
    if registro:
        for dispositivo in registro["dispositivos"]:
            if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
                _enviar_pushover(
                    dispositivo["pushover_user_key"], "⚠️ Queda de energia prolongada",
                    f"Equipamento {galpao_id} sem energia há mais de 2 horas.", prioridade_alta=True,
                )


def _alertar_reestabelecido(galpao_id: str, externo: bool = True, app: bool = True):
    """Chamada só através da fila, quando o galpão volta a responder.

    `externo`: True só se a queda chegou aos TIMEOUT_SIRENE_SEGUNDOS (300s),
    ou seja, se WhatsApp/Pushover de QUEDA chegaram a ser enviados. Só
    nesse caso os contatos recebem "energia reestabelecida" por
    WhatsApp/Pushover -- sem aviso de queda enviado, não faz sentido
    avisar que voltou. (Com TIMEOUT_QUEDA_SEGUNDOS == TIMEOUT_SIRENE_SEGUNDOS
    isso é sempre True; fica como proteção se os limites forem separados
    de novo.)

    `app`: True se o app recebeu o aviso de "sem sinal" -- aí o app
    também recebe o "sinal reestabelecido", pra fechar o par."""
    contatos, cliente = _contatos_do_galpao(galpao_id)
    if cliente:
        if externo:
            for contato in contatos:
                if cliente.get("whatsapp_ativo") and contato.get("whatsapp"):
                    _enviar_template(contato["whatsapp"], "energia_reestabelecida", [galpao_id])
                if cliente.get("pushover_ativo") and contato.get("pushover"):
                    _enviar_pushover(contato["pushover"], "✅ Energia reestabelecida",
                                      f"Galpão {galpao_id} operação normalizada.", prioridade_alta=False)
            if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
                _enviar_pushover(cliente["pushover_user_key"], "✅ Energia reestabelecida",
                                  f"Galpão {galpao_id} operação normalizada.", prioridade_alta=False)

        galpao = next((g for g in cliente.get("galpoes", []) if g["id"] == galpao_id), None)
        if app and galpao and galpao.get("notificar_queda"):
            _enviar_push_multicanal(
                cliente.get("push_tokens", []),
                titulo=f"✅ Sinal reestabelecido — {galpao['nome']}",
                corpo="O galpão voltou a responder normalmente.",
                dados={"tipo": "queda_reestabelecida", "galpao_id": galpao_id},
            )

        if externo:
            print(f"[whatsapp_alertas] Alerta de RESTABELECIMENTO disparado para galpão {galpao_id}")
        else:
            print(f"[whatsapp_alertas] RESTABELECIMENTO só no app (queda curta, abaixo de {TIMEOUT_SIRENE_SEGUNDOS}s) para galpão {galpao_id}")

    _alertar_reestabelecido_autoatendimento(galpao_id, externo=externo, app=app)


def _alertar_reestabelecido_autoatendimento(codigo: str, externo: bool = True, app: bool = True):
    registro = clientes.listar_autoatendimento().get(codigo)
    if not registro:
        return
    for dispositivo in registro["dispositivos"]:
        if not dispositivo.get("notificar_queda"):
            continue
        if app:
            push_expo.enviar_push(
                dispositivo["push_token"],
                titulo=f"✅ Sinal reestabelecido — {codigo}",
                corpo="O equipamento voltou a responder normalmente.",
                dados={"tipo": "queda_reestabelecida", "codigo": codigo},
            )
        if externo and dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
            _enviar_pushover(
                dispositivo["pushover_user_key"], "✅ Energia reestabelecida",
                f"Equipamento {codigo} operação normalizada.", prioridade_alta=False,
            )


def marcar_online(galpao_id: str, temperatura: float = None):
    with _lock:
        agora = time.time()
        estava_em_queda = _em_queda.get(galpao_id, False)
        ultimo_anterior = _ultimo_checkin.get(galpao_id)
        estado_queda_anterior = None
        _ultimo_checkin[galpao_id] = agora
        if temperatura is not None:
            _ultima_temperatura[galpao_id] = {"valor": temperatura, "timestamp": agora}

            pontos = _historico_temperatura.setdefault(galpao_id, [])
            if not pontos or (agora - pontos[-1]["timestamp"]) >= INTERVALO_HISTORICO_SEGUNDOS:
                pontos.append({"valor": temperatura, "timestamp": agora})
                if len(pontos) > HISTORICO_MAX_PONTOS:
                    del pontos[0]

        if estava_em_queda:
            _em_queda[galpao_id] = False
            # Zera toda a escalada (sirene, repetição de Pushover, marca de
            # "prolongada") -- a próxima queda começa do zero. O estado
            # antigo é guardado antes de apagar, pra decidir abaixo quem
            # deve receber o aviso de restabelecimento.
            estado_queda_anterior = _estado_queda.pop(galpao_id, None)

    # Estatística de conexões: soma 1 na faixa de 30s correspondente ao
    # tempo desde o checkin anterior (ver relatorio_conexoes.py). Só memória,
    # e nunca pode derrubar o checkin.
    if ultimo_anterior is not None:
        try:
            relatorio_conexoes.registrar_intervalo(galpao_id, agora - ultimo_anterior, agora)
        except Exception as e:
            print(f"[whatsapp_alertas] ERRO ao registrar intervalo de conexão: {e}")

    if estava_em_queda:
        estado_anterior = estado_queda_anterior or {}
        # WhatsApp/Pushover de restabelecimento só se a queda chegou aos
        # 300s (sirene_disparada) -- é quando WhatsApp/Pushover de QUEDA
        # foram enviados. O app recebe o par ("sem sinal" + volta).
        enviar_externo = bool(estado_anterior.get("sirene_disparada"))
        enviar_app = bool(estado_anterior.get("notificado_app", True))
        segundos_fora = (agora - ultimo_anterior) if ultimo_anterior else None
        if segundos_fora is not None:
            print(f"[whatsapp_alertas] Galpão {galpao_id} voltou após {segundos_fora:.0f}s sem checkin (era queda)")
        # Enfileirado, não chamado direto -- essa função roda DENTRO da
        # requisição HTTP do checkin (ver receber_checkin em servidor.py).
        # Chamar _alertar_reestabelecido direto aqui bloquearia a resposta
        # pro ESP32 pelo tempo de várias chamadas de API externa em
        # sequência (WhatsApp + Pushover por contato, até 10s cada).
        _enfileirar(_alertar_reestabelecido, galpao_id, externo=enviar_externo, app=enviar_app)
        twilio_alertas.resetar_escalonamento(galpao_id)
    elif ultimo_anterior is not None:
        # Não chegou a ser queda (não passou de TIMEOUT_QUEDA_SEGUNDOS). Se
        # ficou pelo menos LIMITE_LOG_SEM_CONEXAO_SEGUNDOS sem checkin,
        # registra com a tag SEM_CONEXAO -- só log, nenhum alerta é enviado.
        segundos_sem_checkin = agora - ultimo_anterior
        if segundos_sem_checkin >= LIMITE_LOG_SEM_CONEXAO_SEGUNDOS:
            print(f"[SEM_CONEXAO] Galpão {galpao_id} ficou {segundos_sem_checkin:.0f}s sem checkin "
                  f"e voltou sozinho (abaixo de {TIMEOUT_QUEDA_SEGUNDOS}s, sem alerta)")

    if temperatura is not None:
        _checar_limites_temperatura(galpao_id, temperatura)

        # Persistir no disco só pra clientes gerenciados — autoatendimento
        # continua só com as últimas 24h em memória, como já era.
        _, cliente = clientes.galpao_pertence_a(galpao_id)
        if cliente:
            clientes.adicionar_ponto_historico(galpao_id, temperatura, agora)


def temperatura_do_galpao(galpao_id: str):
    with _lock:
        return _ultima_temperatura.get(galpao_id)


def historico_do_galpao(galpao_id: str):
    """Retorna a lista de pontos {"valor", "timestamp"} das últimas ~24h
    (1 ponto a cada 5min). Usado pelo gráfico da tela de detalhe."""
    with _lock:
        return list(_historico_temperatura.get(galpao_id, []))


def status_galpoes_cliente(cliente: dict):
    """Monta a lista de galpões desse cliente já com temperatura atual e
    status online/offline — formato que a tela principal do app espera."""
    resultado = []
    with _lock:
        for galpao in cliente.get("galpoes", []):
            galpao_id = galpao["id"]
            ultimo = _ultimo_checkin.get(galpao_id)
            online = ultimo is not None and (time.time() - ultimo) <= TIMEOUT_QUEDA_SEGUNDOS
            temp = _ultima_temperatura.get(galpao_id)
            pendente = _alerta_temp_pendente.get(galpao_id)
            estado_queda = _estado_queda.get(galpao_id)
            resultado.append({
                "id": galpao_id,
                "nome": galpao["nome"],
                "online": online,
                "temperatura": temp["valor"] if (online and temp) else None,
                "limite_superior": galpao.get("limite_superior"),
                "limite_inferior": galpao.get("limite_inferior"),
                "modo_alerta": galpao.get("modo_alerta", "desativado"),
                "notificar_queda": galpao.get("notificar_queda", False),
                # true quando tem alerta de temperatura ativo que ninguém
                # confirmou ainda — é o que decide se o botão vermelho
                # "Resolvido" aparece no card, no app.
                "alerta_temperatura_pendente": bool(pendente and not pendente["confirmado"]),
                # true a partir de TIMEOUT_QUEDA_PROLONGADA_SEGUNDOS (120min)
                # de queda contínua -- ainda não usado na tela hoje, fica
                # disponível pro app exibir algo diferente nesse caso, se
                # quiser, num próximo passo.
                "queda_prolongada": bool(estado_queda and estado_queda.get("prolongada")),
            })
    return resultado


def _viola_limite(valor: float, limite_superior, limite_inferior, modo: str) -> bool:
    if modo == "desativado":
        return False
    if modo in ("superior", "ambos") and limite_superior is not None and valor > limite_superior:
        return True
    if modo in ("inferior", "ambos") and limite_inferior is not None and valor < limite_inferior:
        return True
    return False


def _checar_limites_temperatura(galpao_id: str, valor: float):
    """Checa se esse galpão pertence a um cliente gerenciado com limite
    configurado, e envia push pros push_tokens do cliente se ultrapassou.
    Autoatendimento é checado à parte, em `_checar_limites_autoatendimento`,
    já que a fonte de temperatura ali é a mesma rota pública, mas o limite
    é por dispositivo, não por galpão.

    A decisão (violando/já alertado) roda na hora, sem rede — só o ENVIO
    (que fala com API externa) é enfileirado, já que essa função também
    roda dentro da requisição HTTP do checkin."""
    cliente_id, cliente = clientes.galpao_pertence_a(galpao_id)
    if not cliente:
        return

    galpao = next((g for g in cliente["galpoes"] if g["id"] == galpao_id), None)
    if not galpao:
        return

    chave_alerta = f"gerenciado:{galpao_id}"
    violando = _viola_limite(valor, galpao.get("limite_superior"), galpao.get("limite_inferior"), galpao.get("modo_alerta", "desativado"))
    ja_alertado = _ultimo_alerta_temp.get(chave_alerta, False)

    if violando and not ja_alertado:
        _enfileirar(
            _enviar_push_multicanal,
            cliente.get("push_tokens", []),
            titulo=f"🌡️ Temperatura fora do limite — {galpao['nome']}",
            corpo=f"Leitura atual: {valor}°C",
            dados={"tipo": "limite_temperatura", "galpao_id": galpao_id},
        )
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enfileirar(
                _enviar_pushover,
                cliente["pushover_user_key"],
                f"🌡️ Temperatura fora do limite — {galpao['nome']}",
                f"Leitura atual: {valor}°C", prioridade_alta=True,
            )
        _ultimo_alerta_temp[chave_alerta] = True
        with _lock:
            _alerta_temp_pendente[galpao_id] = {"ultimo_envio": time.time(), "confirmado": False}
    elif not violando:
        _ultimo_alerta_temp[chave_alerta] = False
        # Voltou ao normal sozinho — não faz mais sentido continuar
        # cobrando confirmação de um alerta que já passou.
        with _lock:
            _alerta_temp_pendente.pop(galpao_id, None)


def reenviar_alertas_pendentes():
    """Chame periodicamente (ex: a cada 60s — a própria função decide
    quando já passou tempo suficiente pra reenviar, via
    REENVIO_ALERTA_SEGUNDOS). Reenvia o push de temperatura fora do
    limite pra clientes gerenciados que ainda não confirmaram
    ('Resolvido' no app) e cuja violação continua ativa.

    Já roda numa thread de fundo própria (não na requisição HTTP), mas
    os envios também são enfileirados -- assim um envio lento não atrasa
    a checagem dos OUTROS galpões pendentes no mesmo ciclo."""
    agora = time.time()
    with _lock:
        pendentes = [(g, dict(estado)) for g, estado in _alerta_temp_pendente.items() if not estado["confirmado"]]

    for galpao_id, estado in pendentes:
        if agora - estado["ultimo_envio"] < REENVIO_ALERTA_SEGUNDOS:
            continue

        _, cliente = clientes.galpao_pertence_a(galpao_id)
        if not cliente:
            continue
        galpao = next((g for g in cliente["galpoes"] if g["id"] == galpao_id), None)
        if not galpao:
            continue

        temp = _ultima_temperatura.get(galpao_id)
        valor = temp["valor"] if temp else None

        _enfileirar(
            _enviar_push_multicanal,
            cliente.get("push_tokens", []),
            titulo=f"🌡️ Ainda fora do limite — {galpao['nome']}",
            corpo=(f"Leitura atual: {valor}°C. Toque em 'Resolvido' quando verificar."
                   if valor is not None else "Toque em 'Resolvido' quando verificar."),
            dados={"tipo": "limite_temperatura", "galpao_id": galpao_id},
        )
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enfileirar(
                _enviar_pushover,
                cliente["pushover_user_key"],
                f"🌡️ Ainda fora do limite — {galpao['nome']}",
                f"Leitura atual: {valor}°C", prioridade_alta=True,
            )

        with _lock:
            if galpao_id in _alerta_temp_pendente and not _alerta_temp_pendente[galpao_id]["confirmado"]:
                _alerta_temp_pendente[galpao_id]["ultimo_envio"] = agora


def resolver_alerta_temperatura(galpao_id: str) -> None:
    """Chame quando a pessoa aperta 'Resolvido' no app — para de reenviar
    push pra esse galpão até a próxima violação nova (ou até a
    temperatura sair e voltar a entrar fora do limite)."""
    with _lock:
        if galpao_id in _alerta_temp_pendente:
            _alerta_temp_pendente[galpao_id]["confirmado"] = True


def checar_limites_autoatendimento(codigo: str, valor: float):
    """Chame sempre que `/temperatura/<codigo>` for consultado (ou, melhor
    ainda, de dentro do checkin do ESP32 se o código de autoatendimento
    for o mesmo galpao_id — depende de como o ESP32 identifica o galpão).
    Cada dispositivo daquele código tem seu próprio limite independente.

    Chamada direto da rota de checkin (ver servidor.py) -- por isso os
    envios aqui também são enfileirados, nunca diretos."""
    registro = clientes.listar_autoatendimento().get(codigo)
    if not registro:
        return

    for dispositivo in registro["dispositivos"]:
        chave_alerta = f"auto:{codigo}:{dispositivo['push_token']}"
        violando = _viola_limite(
            valor, dispositivo.get("limite_superior"), dispositivo.get("limite_inferior"),
            dispositivo.get("modo_alerta", "desativado"),
        )
        ja_alertado = _ultimo_alerta_temp.get(chave_alerta, False)

        if violando and not ja_alertado:
            _enfileirar(
                push_expo.enviar_push,
                dispositivo["push_token"],
                titulo=f"🌡️ Temperatura fora do limite — {codigo}",
                corpo=f"Leitura atual: {valor}°C",
                dados={"tipo": "limite_temperatura", "codigo": codigo},
            )
            if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
                _enfileirar(
                    _enviar_pushover,
                    dispositivo["pushover_user_key"],
                    f"🌡️ Temperatura fora do limite — {codigo}",
                    f"Leitura atual: {valor}°C", prioridade_alta=True,
                )
            _ultimo_alerta_temp[chave_alerta] = True
        elif not violando:
            _ultimo_alerta_temp[chave_alerta] = False


def verificar_quedas():
    """Chame periodicamente (5-10s). Gerencia toda a escalada de alerta de
    queda de energia -- ver a tabela de tempos no topo do arquivo. Cada
    estágio é calculado a partir do tempo real desde o último checkin bem-
    sucedido (não desde quando este loop "percebeu" a queda), pra não
    acumular atraso a cada ciclo de 5s."""
    agora = time.time()
    with _lock:
        checkins = dict(_ultimo_checkin)

        for galpao_id, ultimo in checkins.items():
            tempo_offline = agora - ultimo

            if tempo_offline < TIMEOUT_QUEDA_SEGUNDOS:
                if _em_queda.get(galpao_id):
                    _em_queda[galpao_id] = False
                continue

            _em_queda[galpao_id] = True

            estado = _estado_queda.setdefault(galpao_id, {
                "notificado_app": False,
                "sirene_disparada": False,
                "ultimo_pushover": 0.0,
                "prolongada": False,
            })

            # 300s: aviso pro app, uma única vez (mesmo instante da sirene).
            if not estado["notificado_app"]:
                estado["notificado_app"] = True
                print(f"[whatsapp_alertas] Galpão {galpao_id} sem checkin há {tempo_offline:.0f}s — em queda, avisando app, sirene e contatos")
                _enfileirar(_notificar_app_queda, galpao_id)

            if not estado["prolongada"]:
                # 300s: liga sirene + 1º WhatsApp/Pushover.
                if not estado["sirene_disparada"] and tempo_offline >= TIMEOUT_SIRENE_SEGUNDOS:
                    estado["sirene_disparada"] = True
                    estado["ultimo_pushover"] = agora
                    _enfileirar(_alertar_queda, galpao_id)
                # Repetição do Pushover a cada 300s, até dar 120min.
                elif estado["sirene_disparada"] and tempo_offline < TIMEOUT_QUEDA_PROLONGADA_SEGUNDOS:
                    if agora - estado["ultimo_pushover"] >= INTERVALO_REENVIO_PUSHOVER_QUEDA_SEGUNDOS:
                        estado["ultimo_pushover"] = agora
                        _enfileirar(_reenviar_pushover_queda, galpao_id)

                # 120min: mensagem final, encerra a repetição.
                if tempo_offline >= TIMEOUT_QUEDA_PROLONGADA_SEGUNDOS:
                    estado["prolongada"] = True
                    _enfileirar(_alertar_queda_prolongada, galpao_id)

    # Twilio: cada galpão já em queda (>= TIMEOUT_QUEDA_SEGUNDOS), contado
    # a partir do mesmo instante real (último checkin) -- não do momento
    # em que este loop passou a perceber a queda.
    for galpao_id, ultimo in checkins.items():
        tempo_offline = agora - ultimo
        if tempo_offline < TIMEOUT_QUEDA_SEGUNDOS:
            continue
        _, cliente = _contatos_do_galpao(galpao_id)
        if not cliente or not cliente.get("twilio_ativo"):
            continue
        contatos_fmt = []
        for c in cliente.get("contatos", []):
            telefone = _telefone_e164(c)
            if telefone:
                contatos_fmt.append({"telefone": telefone})
        if contatos_fmt:
            twilio_alertas.verificar_escalonamento_ligacoes(galpao_id, contatos_fmt, desde_quando_offline=ultimo)


def _algum_alerta_temperatura(galpoes: list[str]) -> list[str]:
    """Quais desses galpões (só clientes gerenciados — autoatendimento
    tem limite por dispositivo, não por galpão, então não entra aqui)
    estão com a temperatura fora do limite configurado agora."""
    em_alerta = []
    for galpao_id in galpoes:
        _, cliente = clientes.galpao_pertence_a(galpao_id)
        if not cliente:
            continue
        galpao = next((g for g in cliente["galpoes"] if g["id"] == galpao_id), None)
        if not galpao:
            continue
        temp = _ultima_temperatura.get(galpao_id)
        if not temp:
            continue
        if _viola_limite(
            temp["valor"], galpao.get("limite_superior"), galpao.get("limite_inferior"),
            galpao.get("modo_alerta", "desativado"),
        ):
            em_alerta.append(galpao_id)
    return em_alerta


def status_grupo(grupo_id: str):
    """A sirene só liga a partir de TIMEOUT_SIRENE_SEGUNDOS (300s) de
    queda (que agora é o mesmo limite de TIMEOUT_QUEDA_SEGUNDOS)."""
    galpoes = GRUPOS_SIRENE.get(grupo_id, [])
    with _lock:
        em_queda = [g for g in galpoes if _estado_queda.get(g, {}).get("sirene_disparada", False)]
    em_alerta_temp = _algum_alerta_temperatura(galpoes)
    return {
        "grupo": grupo_id,
        "algum_em_queda": len(em_queda) > 0,
        "galpoes_em_queda": em_queda,
        "algum_alerta_temperatura": len(em_alerta_temp) > 0,
        "galpoes_em_alerta_temperatura": em_alerta_temp,
        # true se QUALQUER um dos dois motivos pedir sirene ligada —
        # é o campo mais simples pro firmware da sirene usar
        "sirene_deve_ligar": len(em_queda) > 0 or len(em_alerta_temp) > 0,
        "total_galpoes": len(galpoes),
    }


def enviar_relatorio_semanal():
    """Uma vez por semana, cada cliente gerenciado recebe um relatório só
    com os próprios galpões."""
    for cliente_id, cliente in clientes.listar_gerenciados().items():
        if not cliente.get("whatsapp_ativo"):
            continue
        linhas = []
        for galpao in sorted(cliente["galpoes"], key=lambda g: g["id"]):
            em_queda = _em_queda.get(galpao["id"], False)
            status = "com problema, verificar" if em_queda else "operando normalmente"
            linhas.append(f"{galpao['nome']} - {status}")
        texto_status = "\n".join(linhas)
        for contato in cliente["contatos"]:
            if contato.get("whatsapp"):
                _enviar_template(contato["whatsapp"], "confirmacao_semanal", [texto_status])
        print(f"[whatsapp_alertas] Relatório semanal enviado para cliente '{cliente_id}'")
