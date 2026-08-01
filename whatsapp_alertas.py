"""
Módulo de alertas via WhatsApp + Pushover + Twilio para monitoramento de
galpões, status de grupos para os ESP32 de sirene, e alertas de
temperatura (push remoto) pros dois tipos de cliente.

Fluxo:
- O servidor principal chama `marcar_online(galpao_id, temperatura)` a
  cada checkin do ESP32.
- Uma rotina de verificação (chamada a cada poucos segundos) checa:
    1. se algum galpão está sem responder há mais de 60s -> alerta de
       queda (WhatsApp + Pushover imediato; Twilio escalona 10/20/30min)
    2. se a temperatura de algum galpão ultrapassou o limite configurado
       -> push remoto (Expo) pro(s) dispositivo(s) responsável(is)
- Quando o galpão volta a responder, dispara "energia reestabelecida" e
  reseta o escalonamento de ligações.
- `status_grupo(grupo_id)` é consultado pelos ESP32 de sirene.

Os dados de cliente (contatos, canais ativos, galpões, limites) NÃO ficam
mais num dict fixo aqui — vêm do módulo `clientes.py`, que lê/escreve os
JSONs no disco persistente.
"""

import os
import time
import requests
from threading import Lock

import twilio_alertas
import clientes
import push_expo

# ===================== CONFIGURAÇÃO =====================

WHATSAPP_TOKEN = os.environ["WHATSAPP_TOKEN"]
PHONE_NUMBER_ID = os.environ.get("PHONE_NUMBER_ID", "SEU_PHONE_NUMBER_ID_AQUI")
WHATSAPP_API_URL = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

PUSHOVER_API_TOKEN = os.environ.get("PUSHOVER_API_TOKEN", "")
PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"

TIMEOUT_SEGUNDOS = 60  # tempo sem checkin para considerar "queda"

# Grupos de sirene continuam configurados aqui (não fazem parte do
# cadastro de cliente — podem cobrir vários clientes ou um subconjunto,
# dependendo de onde a sirene física fica instalada).
GRUPOS_SIRENE = {
    "grupo_1": ["01", "02", "03", "04"],
    "grupo_2": ["05", "06", "07", "08"],
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

# ===================== ESTADO INTERNO (em memória) =====================

_lock = Lock()
_ultimo_checkin = {}       # galpao_id -> timestamp do último checkin
_em_queda = {}              # galpao_id -> bool
_ultima_temperatura = {}    # galpao_id -> {"valor": float, "timestamp": float}
_ultimo_alerta_temp = {}    # galpao_id ou (codigo, push_token) -> bool (já alertado nessa violação?)
_historico_temperatura = {}  # galpao_id -> lista de {"valor": float, "timestamp": float}

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


def _alertar_queda(galpao_id: str):
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
        print(f"[whatsapp_alertas] Alerta de QUEDA disparado para galpão {galpao_id}")

    _alertar_queda_autoatendimento(galpao_id)


def _alertar_reestabelecido(galpao_id: str):
    contatos, cliente = _contatos_do_galpao(galpao_id)
    if cliente:
        for contato in contatos:
            if cliente.get("whatsapp_ativo") and contato.get("whatsapp"):
                _enviar_template(contato["whatsapp"], "energia_reestabelecida", [galpao_id])
            if cliente.get("pushover_ativo") and contato.get("pushover"):
                _enviar_pushover(contato["pushover"], "✅ Energia reestabelecida",
                                  f"Galpão {galpao_id} operação normalizada.", prioridade_alta=False)
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enviar_pushover(cliente["pushover_user_key"], "✅ Energia reestabelecida",
                              f"Galpão {galpao_id} operação normalizada.", prioridade_alta=False)
        print(f"[whatsapp_alertas] Alerta de RESTABELECIMENTO disparado para galpão {galpao_id}")

    _alertar_reestabelecido_autoatendimento(galpao_id)


def _alertar_queda_autoatendimento(codigo: str):
    """Autoatendimento não tem contato cadastrado (sem login) — o alerta
    de queda vai direto pro(s) celular(is) que registraram esse código e
    ativaram 'notificar ociosidade' na tela de editar. Igual ao alerta de
    limite de temperatura, cada dispositivo decide se quer receber."""
    registro = clientes.listar_autoatendimento().get(codigo)
    if not registro:
        return
    algum_notificado = False
    for dispositivo in registro["dispositivos"]:
        if not dispositivo.get("notificar_queda"):
            continue
        algum_notificado = True
        push_expo.enviar_push(
            dispositivo["push_token"],
            titulo=f"⚠️ Sem sinal — {codigo}",
            corpo="O equipamento parou de responder. Pode ser queda de energia — verificação necessária.",
            dados={"tipo": "queda_energia", "codigo": codigo},
        )
        if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
            _enviar_pushover(
                dispositivo["pushover_user_key"], "⚠️ Queda de energia",
                f"Equipamento {codigo} sem energia. Verificação necessária.", prioridade_alta=True,
            )
    if algum_notificado:
        print(f"[whatsapp_alertas] Alerta de QUEDA (autoatendimento) disparado para {codigo}")


def _alertar_reestabelecido_autoatendimento(codigo: str):
    registro = clientes.listar_autoatendimento().get(codigo)
    if not registro:
        return
    for dispositivo in registro["dispositivos"]:
        if not dispositivo.get("notificar_queda"):
            continue
        push_expo.enviar_push(
            dispositivo["push_token"],
            titulo=f"✅ Sinal reestabelecido — {codigo}",
            corpo="O equipamento voltou a responder normalmente.",
            dados={"tipo": "queda_reestabelecida", "codigo": codigo},
        )
        if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
            _enviar_pushover(
                dispositivo["pushover_user_key"], "✅ Energia reestabelecida",
                f"Equipamento {codigo} operação normalizada.", prioridade_alta=False,
            )


def marcar_online(galpao_id: str, temperatura: float = None):
    with _lock:
        agora = time.time()
        estava_em_queda = _em_queda.get(galpao_id, False)
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

    if estava_em_queda:
        _alertar_reestabelecido(galpao_id)
        twilio_alertas.resetar_escalonamento(galpao_id)

    if temperatura is not None:
        _checar_limites_temperatura(galpao_id, temperatura)


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
            online = ultimo is not None and (time.time() - ultimo) <= TIMEOUT_SEGUNDOS
            temp = _ultima_temperatura.get(galpao_id)
            resultado.append({
                "id": galpao_id,
                "nome": galpao["nome"],
                "online": online,
                "temperatura": temp["valor"] if (online and temp) else None,
                "limite_superior": galpao.get("limite_superior"),
                "limite_inferior": galpao.get("limite_inferior"),
                "modo_alerta": galpao.get("modo_alerta", "desativado"),
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
    é por dispositivo, não por galpão."""
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
        push_expo.enviar_push_varios(
            cliente.get("push_tokens", []),
            titulo=f"🌡️ Temperatura fora do limite — {galpao['nome']}",
            corpo=f"Leitura atual: {valor}°C",
            dados={"tipo": "limite_temperatura", "galpao_id": galpao_id},
        )
        if cliente.get("pushover_ativo") and cliente.get("pushover_user_key"):
            _enviar_pushover(
                cliente["pushover_user_key"],
                f"🌡️ Temperatura fora do limite — {galpao['nome']}",
                f"Leitura atual: {valor}°C", prioridade_alta=True,
            )
        _ultimo_alerta_temp[chave_alerta] = True
    elif not violando:
        _ultimo_alerta_temp[chave_alerta] = False


def checar_limites_autoatendimento(codigo: str, valor: float):
    """Chame sempre que `/temperatura/<codigo>` for consultado (ou, melhor
    ainda, de dentro do checkin do ESP32 se o código de autoatendimento
    for o mesmo galpao_id — depende de como o ESP32 identifica o galpão).
    Cada dispositivo daquele código tem seu próprio limite independente."""
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
            push_expo.enviar_push(
                dispositivo["push_token"],
                titulo=f"🌡️ Temperatura fora do limite — {codigo}",
                corpo=f"Leitura atual: {valor}°C",
                dados={"tipo": "limite_temperatura", "codigo": codigo},
            )
            if dispositivo.get("pushover_ativo") and dispositivo.get("pushover_user_key"):
                _enviar_pushover(
                    dispositivo["pushover_user_key"],
                    f"🌡️ Temperatura fora do limite — {codigo}",
                    f"Leitura atual: {valor}°C", prioridade_alta=True,
                )
            _ultimo_alerta_temp[chave_alerta] = True
        elif not violando:
            _ultimo_alerta_temp[chave_alerta] = False


def verificar_quedas():
    """Chame periodicamente (5-10s). Detecta galpões sem checkin há mais
    de TIMEOUT_SEGUNDOS, dispara alerta de queda uma vez por queda, e
    escalona ligações via Twilio enquanto continuar offline."""
    novas_quedas = []
    with _lock:
        agora = time.time()
        for galpao_id, ultimo in list(_ultimo_checkin.items()):
            sem_resposta_ha = agora - ultimo
            ja_alertado = _em_queda.get(galpao_id, False)
            if sem_resposta_ha > TIMEOUT_SEGUNDOS and not ja_alertado:
                _em_queda[galpao_id] = True
                novas_quedas.append(galpao_id)
        galpoes_em_queda_agora = [g for g, v in _em_queda.items() if v]

    for galpao_id in novas_quedas:
        _alertar_queda(galpao_id)

    for galpao_id in galpoes_em_queda_agora:
        _, cliente = _contatos_do_galpao(galpao_id)
        if not cliente or not cliente.get("twilio_ativo"):
            continue
        contatos_fmt = []
        for c in cliente.get("contatos", []):
            telefone = _telefone_e164(c)
            if telefone:
                contatos_fmt.append({"telefone": telefone})
        if contatos_fmt:
            twilio_alertas.verificar_escalonamento_ligacoes(galpao_id, contatos_fmt)


def status_grupo(grupo_id: str):
    galpoes = GRUPOS_SIRENE.get(grupo_id, [])
    with _lock:
        em_queda = [g for g in galpoes if _em_queda.get(g, False)]
    return {
        "grupo": grupo_id,
        "algum_em_queda": len(em_queda) > 0,
        "galpoes_em_queda": em_queda,
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
