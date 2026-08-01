"""
push_expo.py

Envio de push notification via Expo Push API — usado pra avisar o celular
da pessoa quando a temperatura de um galpão (ou equipamento de
autoatendimento) sai do limite configurado.

Não precisa de conta nem de chave de API pra uso básico: qualquer app
Expo pode receber push desde que você tenha o "Expo push token" dele
(começa com "ExponentPushToken[..." ou "ExpoPushToken[...", gerado no
próprio celular via `Notifications.getExpoPushTokenAsync()`).

Se quiser um pouco mais de segurança/prioridade nas entregas (opcional),
dá pra criar um Access Token em https://expo.dev/accounts/[sua-conta]/settings/access-tokens
e colocar na variável de ambiente EXPO_ACCESS_TOKEN — o código já usa se
existir, e funciona normalmente sem ela.
"""

import os
import requests

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
EXPO_ACCESS_TOKEN = os.environ.get("EXPO_ACCESS_TOKEN", "")

# A API do Expo aceita até 100 mensagens por request.
TAMANHO_LOTE = 100


def _token_valido(push_token: str) -> bool:
    if not push_token:
        return False
    return push_token.startswith("ExponentPushToken[") or push_token.startswith("ExpoPushToken[")


def _headers():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"
    return headers


def _enviar_lote(mensagens: list[dict]) -> None:
    if not mensagens:
        return
    try:
        resp = requests.post(EXPO_PUSH_URL, json=mensagens, headers=_headers(), timeout=10)
        if resp.status_code != 200:
            print(f"[push_expo] Erro HTTP {resp.status_code} ao enviar push: {resp.text}")
            return

        corpo = resp.json()
        # A resposta traz um "ticket" por mensagem, na mesma ordem enviada.
        # Um ticket com status "error" indica, por exemplo, token inválido
        # ou dispositivo que desinstalou o app (DeviceNotRegistered).
        for i, ticket in enumerate(corpo.get("data", [])):
            if ticket.get("status") == "error":
                token_relacionado = mensagens[i].get("to") if i < len(mensagens) else "?"
                print(f"[push_expo] Ticket com erro pra {token_relacionado}: {ticket.get('message')} ({ticket.get('details')})")
    except Exception as e:
        print(f"[push_expo] Exceção ao enviar push: {e}")


def enviar_push(push_token: str, titulo: str, corpo: str, dados: dict | None = None) -> None:
    """Manda push pra um único dispositivo."""
    if not _token_valido(push_token):
        print(f"[push_expo] Token inválido, ignorando: {push_token}")
        return

    _enviar_lote([{
        "to": push_token,
        "title": titulo,
        "body": corpo,
        "data": dados or {},
        "sound": "default",
        "priority": "high",
    }])


def enviar_push_varios(push_tokens: list[str], titulo: str, corpo: str, dados: dict | None = None) -> None:
    """Manda o mesmo push pra uma lista de dispositivos (ex: todos os
    celulares de um cliente gerenciado que já fizeram login). Filtra
    tokens inválidos/vazios e divide em lotes de 100, conforme o limite
    da API do Expo."""
    validos = [t for t in (push_tokens or []) if _token_valido(t)]
    if not validos:
        return

    for inicio in range(0, len(validos), TAMANHO_LOTE):
        lote = validos[inicio:inicio + TAMANHO_LOTE]
        mensagens = [
            {
                "to": token,
                "title": titulo,
                "body": corpo,
                "data": dados or {},
                "sound": "default",
                "priority": "high",
            }
            for token in lote
        ]
        _enviar_lote(mensagens)
