"""
Módulo de alertas via WhatsApp para monitoramento de galpões.

Fluxo:
- O servidor principal chama `marcar_online(galpao_id)` a cada checkin do ESP32.
- Uma rotina de verificação (chamada a cada poucos segundos) checa se algum
  galpão está sem responder há mais de 60s, e dispara o alerta correspondente.
- Quando o galpão volta a responder, dispara automaticamente a mensagem de
  "energia reestabelecida".
"""

import json
import time
import requests
from pathlib import Path
from threading import Lock

# ===================== CONFIGURAÇÃO =====================

WHATSAPP_TOKEN = "EAATc4gySgYQBR7PnHIBnns3YPcdFSkcweDd8ZAEVPXsvlR2iowN6pNZAQ2tyjbaTJ3bDZBZCE4bPewJZAp0QmdVUnDRq7Q0OkSfZAT4EbuFTQvsfflpo1m6l7kdeWEZCcafesrGeskCNSb8VLMPbFumbkmPOoAdyx2AK8gRxN6GVOTrY7dQz8gyAx5I1My5pXt7LAZDZD"
PHONE_NUMBER_ID = "SEU_PHONE_NUMBER_ID_AQUI"
WHATSAPP_API_URL = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

# Pushover — alerta redundante mais chamativo (som alto, ignora silencioso)
PUSHOVER_API_TOKEN = "a17a8njo185rmfx23kjpj2pcn5fzxe"
PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"

TIMEOUT_SEGUNDOS = 60  # tempo sem checkin para considerar "queda"

# Arquivo de backup com os números de cada cliente/galpão (criado
# automaticamente se não existir, usando o dicionário abaixo como valor inicial)
ARQUIVO_CONTATOS = Path("contatos_clientes.json")

# Estrutura: cada CLIENTE tem uma lista de contatos (até 3 pessoas, cada uma
# com whatsapp opcional e/ou pushover opcional) e uma lista de GALPÕES (ids
# únicos no sistema todo). Os contatos de um cliente recebem alerta de
# qualquer galpão daquele cliente.
#
# Para adicionar um novo cliente: copie o bloco "cliente_X" e ajuste os
# galpoes_ids (devem ser únicos — sugestão: prefixar com a sigla do cliente,
# ex: "ACME-01", "ACME-02" para o cliente ACME).
CLIENTES = {
    "cliente_exemplo": {
        "nome": "Galpões Exemplo Ltda",
        "galpoes_ids": ["01", "02", "03", "04", "05", "06", "07", "08"],
        "contatos": [
            {
                "nome": "Responsável 1 (eu)",
                "whatsapp": "5548999990001",
                "pushover": "u99uknp91h811vg6c79f6vk8yjyrqd",
            },
            {
                "nome": "Responsável 2",
                "whatsapp": "5548999990002",
                "pushover": None,  # ainda não cadastrou Pushover
            },
            {
                "nome": "Responsável 3",
                "whatsapp": "5548999990003",
                "pushover": None,
            },
        ],
    },
    # "cliente_acme": {
    #     "nome": "ACME Armazéns",
    #     "galpoes_ids": ["ACME-01", "ACME-02", "ACME-03"],
    #     "contatos": [
    #         {"nome": "Fulano", "whatsapp": "55...", "pushover": "..."},
    #     ],
    # },
}


def _contatos_do_galpao(galpao_id: str):
    """Retorna a lista de contatos do cliente responsável por esse galpão."""
    for cliente in CLIENTES.values():
        if galpao_id in cliente["galpoes_ids"]:
            return cliente["contatos"]
    return []


def _todos_galpoes():
    """Retorna todos os ids de galpão cadastrados, de todos os clientes."""
    galpoes = []
    for cliente in CLIENTES.values():
        galpoes.extend(cliente["galpoes_ids"])
    return galpoes

# ===================== ESTADO INTERNO =====================

_lock = Lock()
_ultimo_checkin = {}  # galpao_id -> timestamp do último checkin
_em_queda = {}         # galpao_id -> bool (já alertamos a queda?)


def _salvar_contatos_backup():
    """Salva os clientes/contatos atuais em arquivo JSON, como backup/auditoria."""
    try:
        ARQUIVO_CONTATOS.write_text(
            json.dumps(CLIENTES, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"[whatsapp_alertas] Falha ao salvar backup de contatos: {e}")


def _carregar_contatos_backup():
    """Se o arquivo de backup existir, usa ele; senão cria a partir da lista fixa."""
    global CLIENTES
    if ARQUIVO_CONTATOS.exists():
        try:
            CLIENTES = json.loads(ARQUIVO_CONTATOS.read_text(encoding="utf-8"))
            return
        except Exception as e:
            print(f"[whatsapp_alertas] Falha ao ler backup, usando lista fixa: {e}")
    _salvar_contatos_backup()


def _enviar_pushover(user_key: str, titulo: str, mensagem: str, prioridade_alta: bool = False):
    """
    Envia uma notificação push via Pushover.
    prioridade_alta=True usa prioridade de emergência (som mais chamativo,
    repete até ser confirmado) — use só para alertas críticos de queda.
    """
    dados = {
        "token": PUSHOVER_API_TOKEN,
        "user": user_key,
        "title": titulo,
        "message": mensagem,
    }
    if prioridade_alta:
        # Prioridade 2 = emergência: repete o alerta até o usuário confirmar
        dados["priority"] = 2
        dados["retry"] = 60   # repete a cada 60s
        dados["expire"] = 600  # para de repetir após 10 minutos

    try:
        resp = requests.post(PUSHOVER_API_URL, data=dados, timeout=10)
        if resp.status_code != 200:
            print(f"[whatsapp_alertas] Erro Pushover para {user_key}: {resp.text}")
        return resp.ok
    except Exception as e:
        print(f"[whatsapp_alertas] Exceção Pushover para {user_key}: {e}")
        return False


def _enviar_template(numero_destino: str, nome_template: str, parametros: list[str]):
    """Envia uma mensagem usando um template aprovado do WhatsApp Business."""
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": numero_destino,
        "type": "template",
        "template": {
            "name": nome_template,
            "language": {"code": "pt_BR"},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": p} for p in parametros],
                }
            ],
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
    contatos = _contatos_do_galpao(galpao_id)

    for contato in contatos:
        if contato.get("whatsapp"):
            _enviar_template(contato["whatsapp"], "alerta_queda_energia", [galpao_id])
        if contato.get("pushover"):
            _enviar_pushover(
                contato["pushover"],
                titulo="⚠️ Queda de energia",
                mensagem=f"Galpão {galpao_id} sem energia. Verificação necessária.",
                prioridade_alta=True,
            )

    print(f"[whatsapp_alertas] Alerta de QUEDA disparado para galpão {galpao_id}")


def _alertar_reestabelecido(galpao_id: str):
    contatos = _contatos_do_galpao(galpao_id)

    for contato in contatos:
        if contato.get("whatsapp"):
            _enviar_template(contato["whatsapp"], "energia_reestabelecida", [galpao_id])
        if contato.get("pushover"):
            _enviar_pushover(
                contato["pushover"],
                titulo="✅ Energia reestabelecida",
                mensagem=f"Galpão {galpao_id} operação normalizada.",
                prioridade_alta=False,
            )

    print(f"[whatsapp_alertas] Alerta de RESTABELECIMENTO disparado para galpão {galpao_id}")


def marcar_online(galpao_id: str):
    """Chame esta função sempre que receber um checkin do ESP32 de um galpão."""
    with _lock:
        agora = time.time()
        estava_em_queda = _em_queda.get(galpao_id, False)
        _ultimo_checkin[galpao_id] = agora

        if estava_em_queda:
            _em_queda[galpao_id] = False
            _alertar_reestabelecido(galpao_id)


def verificar_quedas():
    """
    Chame esta função periodicamente (ex: a cada 5-10s) num loop ou
    scheduler do seu servidor. Ela detecta galpões sem checkin há mais
    de TIMEOUT_SEGUNDOS e dispara o alerta uma única vez por queda.
    """
    with _lock:
        agora = time.time()
        for galpao_id, ultimo in list(_ultimo_checkin.items()):
            sem_resposta_ha = agora - ultimo
            ja_alertado = _em_queda.get(galpao_id, False)

            if sem_resposta_ha > TIMEOUT_SEGUNDOS and not ja_alertado:
                _em_queda[galpao_id] = True
                _alertar_queda(galpao_id)


def enviar_relatorio_semanal():
    """
    Chame esta função uma vez por semana (ex: via scheduler) para enviar
    o status consolidado dos galpões para cada cliente — cada cliente recebe
    um relatório só com os próprios galpões, não os de outros clientes.
    """
    for cliente_id, cliente in CLIENTES.items():
        linhas = []
        for galpao_id in sorted(cliente["galpoes_ids"]):
            em_queda = _em_queda.get(galpao_id, False)
            status = "com problema, verificar" if em_queda else "operando normalmente"
            linhas.append(f"Galpão {galpao_id} - {status}")

        texto_status = "\n".join(linhas)

        for contato in cliente["contatos"]:
            if contato.get("whatsapp"):
                _enviar_template(contato["whatsapp"], "confirmacao_semanal", [texto_status])

        print(f"[whatsapp_alertas] Relatório semanal enviado para cliente '{cliente_id}'")


# Carrega os contatos (do backup, se existir) ao importar o módulo
_carregar_contatos_backup()
