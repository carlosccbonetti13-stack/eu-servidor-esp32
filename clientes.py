"""
clientes.py

Camada de dados para os dois arquivos JSON que vivem no disco persistente
da Render:

    clientes_gerenciados.json     -> clientes com login (empresa), galpões,
                                      contatos, canais de alerta
    clientes_autoatendimento.json -> códigos de ESP32 auto-cadastrados,
                                      cada um com sua lista de dispositivos
                                      (push token) e limite próprio

Tudo passa por essas funções — nenhum outro módulo deve ler/escrever os
arquivos diretamente. Isso mantém o lock centralizado e evita corrupção
por escrita concorrente.

O disco persistente da Render deve estar montado no caminho apontado por
DISCO_PATH (variável de ambiente, com um padrão sensato pra rodar local).
"""

import json
import os
import secrets
import string
import time
from pathlib import Path
from threading import Lock

DISCO_PATH = Path(os.environ.get("DISCO_PATH", "."))
ARQ_GERENCIADOS = DISCO_PATH / "clientes_gerenciados.json"
ARQ_AUTOATENDIMENTO = DISCO_PATH / "clientes_autoatendimento.json"
ARQ_SESSOES = DISCO_PATH / "sessoes.json"

_lock = Lock()


# ===================== HELPERS DE ARQUIVO =====================

def _ler(caminho: Path) -> dict:
    if not caminho.exists():
        return {}
    try:
        return json.loads(caminho.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[clientes] ERRO lendo {caminho}: {e} — tratando como vazio")
        return {}


def _escrever_atomico(caminho: Path, dados: dict) -> None:
    """Escreve em arquivo temporário e renomeia — evita arquivo corrompido
    se o processo cair no meio da escrita."""
    tmp = caminho.with_suffix(".tmp")
    tmp.write_text(json.dumps(dados, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(caminho)


# ===================== CLIENTES GERENCIADOS =====================

def listar_gerenciados() -> dict:
    with _lock:
        return _ler(ARQ_GERENCIADOS)


def obter_gerenciado(cliente_id: str) -> dict | None:
    with _lock:
        return _ler(ARQ_GERENCIADOS).get(cliente_id)


def autenticar_gerenciado(cliente_id: str, senha: str) -> dict | None:
    cliente = obter_gerenciado(cliente_id)
    if cliente and cliente.get("senha") == senha:
        return cliente
    return None


def criar_gerenciado(cliente_id: str, nome: str, senha: str) -> dict:
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id in dados:
            raise ValueError("cliente_id já existe")
        dados[cliente_id] = {
            "nome": nome,
            "senha": senha,
            "whatsapp_ativo": False,
            "pushover_ativo": False,
            "pushover_user_key": None,
            "twilio_ativo": False,
            "galpoes": [],
            "contatos": [],
            "push_tokens": [],
        }
        _escrever_atomico(ARQ_GERENCIADOS, dados)
        return dados[cliente_id]


def atualizar_gerenciado(cliente_id: str, campos: dict) -> dict:
    """Atualiza campos simples do cliente (nome, senha, toggles). Não usar
    pra galpões/contatos — use as funções específicas abaixo, que fazem
    append/remove em vez de sobrescrever a lista inteira sem querer."""
    permitidos = {"nome", "senha", "whatsapp_ativo", "pushover_ativo", "pushover_user_key", "twilio_ativo"}
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        for chave, valor in campos.items():
            if chave in permitidos:
                dados[cliente_id][chave] = valor
        _escrever_atomico(ARQ_GERENCIADOS, dados)
        return dados[cliente_id]


def atualizar_pushover_gerenciado(cliente_id: str, campos: dict) -> dict:
    """Igual a atualizar_gerenciado, mas restrito só aos campos de
    Pushover — usado pelo endpoint que o próprio cliente logado no app
    pode chamar (diferente do endpoint do admin, que permite editar tudo,
    inclusive senha)."""
    permitidos = {"pushover_user_key", "pushover_ativo"}
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        for chave, valor in campos.items():
            if chave in permitidos:
                dados[cliente_id][chave] = valor
        _escrever_atomico(ARQ_GERENCIADOS, dados)
        return dados[cliente_id]


def remover_gerenciado(cliente_id: str) -> None:
    """Apaga o cliente e tudo que está embaixo dele (galpões, contatos,
    histórico de configuração) — não tem como desfazer. As sessões de
    login antigas desse cliente são revogadas junto, pra ninguém
    continuar logado num cliente que não existe mais."""
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        del dados[cliente_id]
        _escrever_atomico(ARQ_GERENCIADOS, dados)
    revogar_sessoes_de("cliente", cliente_id)


def adicionar_galpao(cliente_id: str, galpao_id: str, nome: str) -> dict:
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        dados[cliente_id]["galpoes"].append({
            "id": galpao_id,
            "nome": nome,
            "limite_superior": None,
            "limite_inferior": None,
            "modo_alerta": "desativado",
            "notificar_queda": False,
        })
        _escrever_atomico(ARQ_GERENCIADOS, dados)
        return dados[cliente_id]


def atualizar_galpao(cliente_id: str, galpao_id: str, campos: dict) -> dict:
    """Usado tanto pelo admin (renomear) quanto pelo cliente (ajustar
    limite/modo_alerta)."""
    permitidos = {"nome", "limite_superior", "limite_inferior", "modo_alerta", "notificar_queda"}
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        for galpao in dados[cliente_id]["galpoes"]:
            if galpao["id"] == galpao_id:
                for chave, valor in campos.items():
                    if chave in permitidos:
                        galpao[chave] = valor
                _escrever_atomico(ARQ_GERENCIADOS, dados)
                return dados[cliente_id]
        raise ValueError("galpão não encontrado")


def adicionar_contato(cliente_id: str, contato: dict) -> dict:
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        dados[cliente_id]["contatos"].append({
            "nome": contato.get("nome", ""),
            "whatsapp": contato.get("whatsapp"),
            "telefone_twilio": contato.get("telefone_twilio"),
            "pushover": contato.get("pushover"),
        })
        _escrever_atomico(ARQ_GERENCIADOS, dados)
        return dados[cliente_id]


def registrar_push_token_gerenciado(cliente_id: str, push_token: str) -> None:
    """Chame quando o app loga com sucesso — guarda o token pra poder
    mandar push remoto de limite de temperatura pra esse celular."""
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
        if cliente_id not in dados:
            raise ValueError("cliente não encontrado")
        tokens = dados[cliente_id].setdefault("push_tokens", [])
        if push_token not in tokens:
            tokens.append(push_token)
        _escrever_atomico(ARQ_GERENCIADOS, dados)


def galpao_pertence_a(galpao_id: str) -> tuple[str, dict] | tuple[None, None]:
    """Acha em qual cliente gerenciado está esse galpão. Retorna
    (cliente_id, cliente) ou (None, None)."""
    with _lock:
        dados = _ler(ARQ_GERENCIADOS)
    for cliente_id, cliente in dados.items():
        for galpao in cliente["galpoes"]:
            if galpao["id"] == galpao_id:
                return cliente_id, cliente
    return None, None


# ===================== AUTOATENDIMENTO =====================

CARACTERES_CODIGO = string.ascii_uppercase + string.digits


def gerar_codigo_autoatendimento() -> str:
    """8 caracteres, letras maiúsculas + números — 36^8 combinações."""
    while True:
        codigo = "".join(secrets.choice(CARACTERES_CODIGO) for _ in range(8))
        with _lock:
            dados = _ler(ARQ_AUTOATENDIMENTO)
        if codigo not in dados:
            return codigo


def registrar_dispositivo_autoatendimento(codigo: str, push_token: str) -> dict:
    """Chame quando o app adiciona esse código na lista local. Cria o
    registro do código se ainda não existir (auto-cadastro)."""
    with _lock:
        dados = _ler(ARQ_AUTOATENDIMENTO)
        if codigo not in dados:
            dados[codigo] = {"dispositivos": []}

        dispositivos = dados[codigo]["dispositivos"]
        existente = next((d for d in dispositivos if d["push_token"] == push_token), None)
        if not existente:
            dispositivos.append({
                "push_token": push_token,
                "limite_superior": None,
                "limite_inferior": None,
                "modo_alerta": "desativado",
                # "ociosidade" = o equipamento parou de mandar checkin (provável
                # queda de energia). Fica desativado por padrão — a pessoa liga
                # na tela de editar se quiser.
                "notificar_queda": False,
                # Redundância opcional: além do push do próprio app, manda
                # também via Pushover se a pessoa colar a User Key dela e
                # deixar ativado.
                "pushover_user_key": None,
                "pushover_ativo": False,
                "adicionado_em": time.time(),
            })
        _escrever_atomico(ARQ_AUTOATENDIMENTO, dados)
        return dados[codigo]


def atualizar_limite_autoatendimento(codigo: str, push_token: str, campos: dict) -> dict:
    permitidos = {
        "limite_superior", "limite_inferior", "modo_alerta",
        "notificar_queda", "pushover_user_key", "pushover_ativo",
    }
    with _lock:
        dados = _ler(ARQ_AUTOATENDIMENTO)
        if codigo not in dados:
            raise ValueError("código não encontrado")
        for dispositivo in dados[codigo]["dispositivos"]:
            if dispositivo["push_token"] == push_token:
                for chave, valor in campos.items():
                    if chave in permitidos:
                        dispositivo[chave] = valor
                _escrever_atomico(ARQ_AUTOATENDIMENTO, dados)
                return dados[codigo]
        raise ValueError("dispositivo não encontrado nesse código")


def listar_autoatendimento() -> dict:
    with _lock:
        return _ler(ARQ_AUTOATENDIMENTO)


# ===================== SESSÕES (TOKEN) =====================
#
# Token não expira sozinho — fica salvo no disco persistente (não só em
# memória), então sobrevive a restart/deploy do servidor. Combinado com
# "lembrar de mim" no app (senha salva localmente de forma segura), a
# sessão se comporta como permanente: se por algum motivo raro o token
# parar de ser aceito, o app faz login de novo sozinho, sem pedir senha
# pra pessoa de novo.

def _gerar_token() -> str:
    return secrets.token_urlsafe(32)


def criar_sessao(tipo: str, identificador: str) -> str:
    """tipo: 'admin' ou 'cliente'. identificador: 'admin' fixo, ou o
    cliente_id, respectivamente. Retorna o token gerado."""
    with _lock:
        sessoes = _ler(ARQ_SESSOES)
        token = _gerar_token()
        sessoes[token] = {"tipo": tipo, "identificador": identificador, "criado_em": time.time()}
        _escrever_atomico(ARQ_SESSOES, sessoes)
        return token


def validar_sessao(token: str) -> dict | None:
    """Retorna {"tipo":..., "identificador":...} ou None se o token não
    existir (nunca expira sozinho, só se for revogado)."""
    if not token:
        return None
    with _lock:
        sessoes = _ler(ARQ_SESSOES)
    return sessoes.get(token)


def revogar_sessao(token: str) -> None:
    with _lock:
        sessoes = _ler(ARQ_SESSOES)
        if token in sessoes:
            del sessoes[token]
            _escrever_atomico(ARQ_SESSOES, sessoes)


def revogar_sessoes_de(tipo: str, identificador: str) -> None:
    """Útil pra quando a senha de um cliente é trocada pelo admin — invalida
    todas as sessões antigas daquele cliente de uma vez."""
    with _lock:
        sessoes = _ler(ARQ_SESSOES)
        restantes = {
            tok: s for tok, s in sessoes.items()
            if not (s["tipo"] == tipo and s["identificador"] == identificador)
        }
        if len(restantes) != len(sessoes):
            _escrever_atomico(ARQ_SESSOES, restantes)
