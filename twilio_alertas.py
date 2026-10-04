"""
twilio_alertas.py

Modulo de ligacoes de voz via Twilio para o sistema de alertas de queda de
energia. Liga apenas para o PRIMEIRO contato da lista de cada galpao, a
partir de 600s (10min) de queda continua, repetindo a cada 10min, por no
maximo 3 tentativas. Se a ligacao de um nivel for atendida, nenhuma ligacao
adicional e feita. Se nao for atendida, o proximo nivel tenta ligar
novamente para o mesmo primeiro contato.

O controle e resetado quando a energia volta.

Integra com o whatsapp_alertas.py existente:
- Chame `verificar_escalonamento_ligacoes(galpao_id, contatos, desde_quando_offline)`
  dentro do mesmo loop periodico que ja roda `verificar_quedas()` (a cada
  5-10s), apenas para galpoes que estao offline no momento.
  `desde_quando_offline` e o timestamp (time.time()) do ULTIMO checkin
  bem-sucedido daquele galpao - a mesma referencia de tempo usada para os
  demais estagios de alerta (app/sirene/pushover/whatsapp), para que todos
  os timeouts fiquem sincronizados a partir do mesmo instante real.
- Chame `resetar_escalonamento(galpao_id)` dentro de `marcar_online()`.
"""

import os
import time
from twilio.rest import Client

# ── Configuracao ──────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_FROM_NUMBER = os.environ["TWILIO_FROM_NUMBER"]  # numero comprado no Twilio, formato +55...

# Niveis de escalonamento em segundos, medidos a partir do ultimo checkin
# bem-sucedido (desde_quando_offline). Comeca em 600s (10min), repete a
# cada 10min, no maximo 3 tentativas: 600s, 1200s, 1800s.
NIVEIS_LIGACAO = [600, 1200, 1800]  # 10min, 20min, 30min

# Tempo de toque antes de considerar "nao atendeu" (segundos, min 5 / max 600 no Twilio)
TEMPO_TOQUE = 25

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# ── Estado em memoria ─────────────────────────────────────────────────────
# galpao_id -> {
#   "niveis_chamados": set(int),
#   "call_sid_pendente": str | None,
#   "atendida": bool,
# }
_estado_escalonamento = {}


def _montar_twiml(galpao_id: str, minutos: int) -> str:
    """Monta o TwiML falado em pt-BR. Repete a mensagem 2x para garantir
    que a pessoa entenda mesmo se atender no meio da fala."""
    mensagem = (
        f"Atencao. Alerta automatico do sistema de monitoramento. "
        f"Queda de energia no Galpao {galpao_id} ha {minutos} minutos, "
        f"sem restabelecimento. Verificacao necessaria."
    )
    return (
        '<Response>'
        f'<Say language="pt-BR" voice="Polly.Camila">{mensagem}</Say>'
        '<Pause length="1"/>'
        f'<Say language="pt-BR" voice="Polly.Camila">{mensagem}</Say>'
        '</Response>'
    )


def _ligar(telefone: str, galpao_id: str, minutos: int) -> str | None:
    """Dispara a ligacao. Retorna o SID da chamada ou None se falhar."""
    try:
        call = client.calls.create(
            to=telefone,
            from_=TWILIO_FROM_NUMBER,
            twiml=_montar_twiml(galpao_id, minutos),
            timeout=TEMPO_TOQUE,
        )
        print(f"[twilio] Ligando para {telefone} (galpao {galpao_id}, {minutos}min) - SID {call.sid}")
        return call.sid
    except Exception as e:
        print(f"[twilio] ERRO ao ligar para {telefone} (galpao {galpao_id}): {e}")
        return None


def _checar_ligacao_pendente(estado: dict) -> None:
    """Atualiza o estado com base no status atual da ligacao pendente,
    se houver uma. Marca 'atendida' se a chamada foi atendida (duracao > 0
    ou em andamento); libera para a proxima tentativa se nao foi atendida."""
    sid = estado.get("call_sid_pendente")
    if not sid:
        return

    try:
        call = client.calls(sid).fetch()
    except Exception as e:
        print(f"[twilio] ERRO ao consultar status da ligacao {sid}: {e}")
        return

    if call.status == "in-progress":
        estado["atendida"] = True
        estado["call_sid_pendente"] = None
    elif call.status == "completed":
        # completed com duracao > 0 = foi atendida em algum momento
        duracao = int(call.duration) if call.duration else 0
        if duracao > 0:
            estado["atendida"] = True
        estado["call_sid_pendente"] = None
    elif call.status in ("no-answer", "busy", "failed", "canceled"):
        estado["call_sid_pendente"] = None  # libera para tentar no proximo nivel
    # status "queued" ou "ringing": ainda em andamento, nao mexe em nada


def resetar_escalonamento(galpao_id: str) -> None:
    """Chame isso dentro de marcar_online() quando o galpao volta a
    responder. Limpa o controle para a proxima queda comecar do zero."""
    if galpao_id in _estado_escalonamento:
        del _estado_escalonamento[galpao_id]


def verificar_escalonamento_ligacoes(
    galpao_id: str,
    contatos_galpao: list[dict],
    desde_quando_offline: float,
) -> None:
    """Chame isso dentro do loop periodico (5-10s), apenas para galpoes
    offline no momento. contatos_galpao e a lista de contatos daquele
    galpao; SO O PRIMEIRO da lista recebe ligacao, ex:
    [{"nome": "Joao", "telefone": "+5511999999999"}, ...]

    `desde_quando_offline` e o timestamp (time.time()) do ultimo checkin
    bem-sucedido do galpao - o tempo de queda e calculado a partir dele,
    em vez de um timer proprio deste modulo, para ficar sincronizado com
    os outros estagios de alerta (app/sirene/pushover/whatsapp).
    """
    if not contatos_galpao:
        return

    agora = time.time()

    if galpao_id not in _estado_escalonamento:
        _estado_escalonamento[galpao_id] = {
            "niveis_chamados": set(),
            "call_sid_pendente": None,
            "atendida": False,
        }

    estado = _estado_escalonamento[galpao_id]

    if estado["atendida"]:
        return  # ja foi atendida, nao liga mais para essa queda

    _checar_ligacao_pendente(estado)

    if estado["atendida"] or estado["call_sid_pendente"]:
        return  # atendida agora, ou ainda tem ligacao em andamento

    tempo_offline = agora - desde_quando_offline
    primeiro_contato = contatos_galpao[0]

    for nivel_segundos in NIVEIS_LIGACAO:
        if tempo_offline >= nivel_segundos and nivel_segundos not in estado["niveis_chamados"]:
            minutos = nivel_segundos // 60
            sid = _ligar(primeiro_contato["telefone"], galpao_id, minutos)
            estado["call_sid_pendente"] = sid
            estado["niveis_chamados"].add(nivel_segundos)
            break  # so um nivel/uma ligacao por checagem
