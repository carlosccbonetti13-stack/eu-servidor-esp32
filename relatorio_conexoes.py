"""
Estatística de conexões dos sensores + relatório diário por WhatsApp.

O QUE CONTA
-----------
A cada checkin, o servidor mede quanto tempo passou desde o checkin anterior
DAQUELE sensor e soma 1 na faixa correspondente (faixas de 30s):

    0-30s | 30-60s | 60-90s | ... | 270-300s | 300s+ (queda)

Exemplo de leitura: "0-30s: 500 / 30-60s: 120" quer dizer que 500 checkins
chegaram até 30s depois do anterior, e 120 chegaram entre 30s e 60s depois.
Quanto mais checkins nas faixas altas, pior a conexão daquele sensor.

O intervalo é contado no dia (fuso America/Sao_Paulo) em que o checkin CHEGOU.
O primeiro checkin de cada sensor depois que o servidor reinicia não tem
"anterior" pra comparar, então não entra na contagem.

RELATÓRIO DIÁRIO
----------------
Uma vez por dia, a partir de RELATORIO_CONEXOES_HORA (padrão 7h, horário de
Brasília), envia por WhatsApp (Evolution API) a contagem do DIA ANTERIOR
inteiro, uma seção por galpão. O dia já enviado é guardado em disco, então
reiniciar/redeployar o servidor não repete nem pula o envio. Se o envio
falhar, tenta de novo a cada 10 minutos.

VARIÁVEIS DE AMBIENTE (Render, no serviço do servidor Mecafix)
--------------------------------------------------------------
    EVOLUTION_API_URL            ex: https://meu-evolution-api.onrender.com
    EVOLUTION_API_KEY            a chave (apikey) da sua Evolution API
    EVOLUTION_INSTANCE           nome da instância (padrão: MECAFIX)
    RELATORIO_CONEXOES_NUMERO    padrão: 5548920004745
    RELATORIO_CONEXOES_HORA      padrão: 7
    RELATORIO_CONEXOES_TESTE     "1" = no próximo início, manda UMA mensagem
                                 de teste com os dados de hoje até agora.
                                 Depois de testar, APAGUE essa variável.
    DATA_DIR                     pasta de dados (padrão: /var/data)

Sem EVOLUTION_API_URL e EVOLUTION_API_KEY a contagem continua funcionando e
sendo guardada, só não envia nada.

IMPORTANTE (mesma regra das outras threads de fundo): `iniciar()` NÃO pode ser
chamada na importação do módulo, só a partir de `_iniciar_threads_de_fundo()`
em servidor.py (que o `post_fork` do Gunicorn chama no processo operário).
"""

import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone

import requests

import clientes

# ===================== CONFIGURAÇÃO =====================

PASSO_SEGUNDOS = 30
NUM_FAIXAS = 11            # 10 faixas de 30s (0 a 300s) + a faixa "300s+"

EVOLUTION_API_URL = os.environ.get("EVOLUTION_API_URL", "")
EVOLUTION_API_KEY = os.environ.get("EVOLUTION_API_KEY", "")
EVOLUTION_INSTANCE = os.environ.get("EVOLUTION_INSTANCE", "MECAFIX")
RELATORIO_NUMERO = os.environ.get("RELATORIO_CONEXOES_NUMERO", "5548920004745")
RELATORIO_TESTE = os.environ.get("RELATORIO_CONEXOES_TESTE", "") == "1"
try:
    RELATORIO_HORA = int(os.environ.get("RELATORIO_CONEXOES_HORA", "7"))
except ValueError:
    RELATORIO_HORA = 7

DIAS_GUARDADOS = 14
INTERVALO_SALVAR_SEGUNDOS = 60
TENTATIVA_NOVA_SEGUNDOS = 10 * 60
LIMITE_MENSAGEM = 3500     # caracteres por mensagem de WhatsApp


def _pasta_dados() -> str:
    pasta = os.environ.get("DATA_DIR", "/var/data")
    return pasta if os.path.isdir(pasta) else os.getcwd()


ARQUIVO = os.path.join(_pasta_dados(), "conexoes_intervalos.json")

try:
    from zoneinfo import ZoneInfo
    FUSO = ZoneInfo("America/Sao_Paulo")
except Exception:  # sem base de fusos no sistema: o Brasil não tem horário de verão desde 2019
    FUSO = timezone(timedelta(hours=-3))

# ===================== ESTADO (memória + disco) =====================

_lock = threading.Lock()
# {"dias": {"2026-10-18": {"<galpao_id>": [11 contadores]}}, "ultimo_envio": "2026-10-18" | None}
_dados = {"dias": {}, "ultimo_envio": None}
_sujo = False                 # tem coisa em memória que ainda não foi pro disco?
_proxima_tentativa = 0.0      # não tenta enviar de novo antes desse instante
_teste_feito = False


def faixa_do_intervalo(segundos: float) -> int:
    return min(int(segundos // PASSO_SEGUNDOS), NUM_FAIXAS - 1)


def _dia_local(ts: float) -> str:
    return datetime.fromtimestamp(ts, FUSO).date().isoformat()


def registrar_intervalo(galpao_id: str, segundos: float, agora: float | None = None) -> None:
    """Chamada a cada checkin (de dentro de marcar_online). Só mexe em
    memória, rápido, sem rede nem disco -- roda dentro da requisição."""
    global _sujo
    if segundos is None or segundos < 0:
        return
    agora = agora if agora is not None else time.time()
    dia = _dia_local(agora)
    with _lock:
        faixas = _dados["dias"].setdefault(dia, {}).setdefault(galpao_id, [0] * NUM_FAIXAS)
        faixas[faixa_do_intervalo(segundos)] += 1
        _sujo = True


def carregar() -> None:
    global _dados
    try:
        with open(ARQUIVO, encoding="utf-8") as f:
            lido = json.load(f)
        if isinstance(lido, dict) and isinstance(lido.get("dias"), dict):
            with _lock:
                _dados = {"dias": lido["dias"], "ultimo_envio": lido.get("ultimo_envio")}
        print(f"[relatorio_conexoes] Dados carregados de {ARQUIVO}")
    except FileNotFoundError:
        print(f"[relatorio_conexoes] Sem arquivo anterior ({ARQUIVO}) -- começando do zero")
    except Exception as e:
        print(f"[relatorio_conexoes] ERRO ao carregar {ARQUIVO}: {e} -- começando do zero")


def salvar() -> None:
    global _sujo
    with _lock:
        if not _sujo:
            return
        conteudo = json.dumps(_dados, ensure_ascii=False)
        _sujo = False
    try:
        tmp = ARQUIVO + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(conteudo)
        os.replace(tmp, ARQUIVO)
    except Exception as e:
        print(f"[relatorio_conexoes] ERRO ao salvar {ARQUIVO}: {e}")
        with _lock:
            _sujo = True


# ===================== MENSAGEM =====================

def _rotulo_faixa(i: int) -> str:
    if i == NUM_FAIXAS - 1:
        return f"{i * PASSO_SEGUNDOS}s+ (queda)"
    return f"{i * PASSO_SEGUNDOS}-{(i + 1) * PASSO_SEGUNDOS}s"


def _nome_galpao(galpao_id: str):
    try:
        _, cliente = clientes.galpao_pertence_a(galpao_id)
        if cliente:
            for g in cliente.get("galpoes", []):
                if g["id"] == galpao_id:
                    return g.get("nome")
    except Exception:
        pass
    return None


def _blocos_do_dia(dia: str) -> list[str]:
    with _lock:
        do_dia = {g: list(f) for g, f in _dados["dias"].get(dia, {}).items()}

    itens = []
    for galpao_id, faixas in do_dia.items():
        nome = _nome_galpao(galpao_id)
        itens.append(((nome or galpao_id).lower(), galpao_id, nome, faixas))
    itens.sort()

    blocos = []
    for _, galpao_id, nome, faixas in itens:
        # mostra da primeira faixa até a última que tem contagem (as do meio
        # aparecem mesmo zeradas, pra tabela ficar contínua)
        ultima = max((i for i, n in enumerate(faixas) if n), default=0)
        titulo = f"*{nome}* ({galpao_id})" if nome and nome != galpao_id else f"*{galpao_id}*"
        linhas = [titulo]
        linhas += [f"{_rotulo_faixa(i)}: {faixas[i]}" for i in range(ultima + 1)]
        linhas.append(f"Total: {sum(faixas)}")
        blocos.append("\n".join(linhas))
    return blocos


def montar_mensagens(dia: str, teste: bool = False) -> list[str]:
    data_br = datetime.strptime(dia, "%Y-%m-%d").strftime("%d/%m/%Y")
    sufixo = " (TESTE, dia em andamento)" if teste else ""
    cabecalho = (f"📊 *Conexões por intervalo* — {data_br}{sufixo}\n"
                 f"_Tempo entre checkins consecutivos de cada sensor_")
    blocos = _blocos_do_dia(dia)
    if not blocos:
        return [cabecalho + "\n\nNenhum checkin registrado nesse dia."]

    mensagens = []
    atual = cabecalho
    tem_bloco = False
    for bloco in blocos:
        if tem_bloco and len(atual) + 2 + len(bloco) > LIMITE_MENSAGEM:
            mensagens.append(atual)
            atual = f"📊 *Conexões por intervalo* — {data_br} (continuação)"
            tem_bloco = False
        atual += "\n\n" + bloco
        tem_bloco = True
    mensagens.append(atual)
    return mensagens


# ===================== ENVIO (Evolution API) =====================

def _enviar_whatsapp(texto: str) -> bool:
    if not (EVOLUTION_API_URL and EVOLUTION_API_KEY):
        print("[relatorio_conexoes] EVOLUTION_API_URL / EVOLUTION_API_KEY não definidas -- relatório não enviado")
        return False
    url = f"{EVOLUTION_API_URL.rstrip('/')}/message/sendText/{EVOLUTION_INSTANCE}"
    headers = {"apikey": EVOLUTION_API_KEY, "Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, json={"number": RELATORIO_NUMERO, "text": texto}, timeout=20)
        if 200 <= resp.status_code < 300:
            return True
        print(f"[relatorio_conexoes] Evolution API respondeu {resp.status_code}: {resp.text[:300]}")
    except Exception as e:
        print(f"[relatorio_conexoes] Exceção ao chamar a Evolution API: {e}")
    return False


def enviar_relatorio(dia: str, teste: bool = False) -> bool:
    for mensagem in montar_mensagens(dia, teste):
        if not _enviar_whatsapp(mensagem):
            return False
    return True


# ===================== AGENDAMENTO =====================

def _marcar_enviado(dia: str) -> None:
    global _sujo
    with _lock:
        _dados["ultimo_envio"] = dia
        _sujo = True
    salvar()


def _limpar_dias_antigos(hoje: str) -> None:
    global _sujo
    limite = (datetime.strptime(hoje, "%Y-%m-%d") - timedelta(days=DIAS_GUARDADOS)).date().isoformat()
    with _lock:
        antigos = [d for d in _dados["dias"] if d < limite]
        for d in antigos:
            del _dados["dias"][d]
        if antigos:
            _sujo = True


def _tick(agora: float | None = None) -> None:
    """Roda a cada ~30s numa thread de fundo: decide se é hora de enviar o
    relatório do dia anterior."""
    global _proxima_tentativa, _teste_feito
    agora = agora if agora is not None else time.time()
    local = datetime.fromtimestamp(agora, FUSO)
    hoje = local.date().isoformat()
    ontem = (local.date() - timedelta(days=1)).isoformat()

    _limpar_dias_antigos(hoje)

    if RELATORIO_TESTE and not _teste_feito:
        _teste_feito = True
        ok = enviar_relatorio(hoje, teste=True)
        print(f"[relatorio_conexoes] Mensagem de TESTE {'enviada' if ok else 'FALHOU'} "
              f"(apague RELATORIO_CONEXOES_TESTE no Render depois de testar)")

    with _lock:
        ultimo_envio = _dados.get("ultimo_envio")
        tem_dados_ontem = bool(_dados["dias"].get(ontem))

    if local.hour < RELATORIO_HORA or agora < _proxima_tentativa:
        return
    if ultimo_envio is not None and ultimo_envio >= ontem:
        return  # já enviou o relatório de ontem

    if ultimo_envio is None and not tem_dados_ontem:
        # primeira vez que o módulo roda: ainda não existe dia anterior pra
        # reportar, então só marca e espera o próximo dia
        _marcar_enviado(ontem)
        print("[relatorio_conexoes] Primeira execução: ainda sem dados de ontem, o 1º relatório sai amanhã")
        return

    if enviar_relatorio(ontem):
        _marcar_enviado(ontem)
        print(f"[relatorio_conexoes] Relatório de {ontem} enviado por WhatsApp")
    else:
        _proxima_tentativa = agora + TENTATIVA_NOVA_SEGUNDOS
        print(f"[relatorio_conexoes] Falha ao enviar o relatório de {ontem} -- nova tentativa em "
              f"{TENTATIVA_NOVA_SEGUNDOS // 60} min")


def _loop() -> None:
    ultimo_salvamento = 0.0
    while True:
        try:
            _tick()
        except Exception as e:
            # sem esse try/except um erro aqui mataria a thread pra sempre, em silêncio
            print(f"[relatorio_conexoes] ERRO no agendamento (thread seguiu rodando): {e}")
        try:
            if time.time() - ultimo_salvamento >= INTERVALO_SALVAR_SEGUNDOS:
                salvar()
                ultimo_salvamento = time.time()
        except Exception as e:
            print(f"[relatorio_conexoes] ERRO ao salvar (thread seguiu rodando): {e}")
        time.sleep(30)


def iniciar() -> None:
    """Chame uma vez, a partir de `_iniciar_threads_de_fundo()` em servidor.py."""
    carregar()
    if not (EVOLUTION_API_URL and EVOLUTION_API_KEY):
        print("[relatorio_conexoes] ATENÇÃO: EVOLUTION_API_URL/EVOLUTION_API_KEY não definidas -- "
              "a contagem funciona, mas o relatório diário não será enviado")
    threading.Thread(target=_loop, daemon=True).start()
