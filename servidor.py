"""
Servidor principal — checkins dos ESP32, monitoramento de queda em
segundo plano, status de grupo pra sirene, e agora também: login,
administração de clientes gerenciados, e autoatendimento com push remoto
de limite de temperatura.
"""

import os
import threading
import time

from flask import Flask, jsonify, request

import whatsapp_alertas as alertas
import clientes
import backup_git

app = Flask(__name__)

ADMIN_SENHA = os.environ["ADMIN_SENHA"]

# ===================== controle de tentativas de login =====================
MAX_TENTATIVAS = 5
_tentativas_falhas = {}  # cliente_id -> {"contagem": int, "bloqueado_ate": float}


def _login_bloqueado(identificador: str) -> bool:
    info = _tentativas_falhas.get(identificador)
    if not info:
        return False
    return info["contagem"] >= MAX_TENTATIVAS and time.time() < info.get("bloqueado_ate", 0)


def _registrar_falha_login(identificador: str):
    info = _tentativas_falhas.setdefault(identificador, {"contagem": 0, "bloqueado_ate": 0})
    info["contagem"] += 1
    if info["contagem"] >= MAX_TENTATIVAS:
        info["bloqueado_ate"] = time.time() + 15 * 60  # 15min de bloqueio


def _limpar_falhas_login(identificador: str):
    _tentativas_falhas.pop(identificador, None)


def _exige_admin():
    """Retorna None se autenticado, ou uma tupla (response, status) de
    erro pra retornar direto na rota. Autenticação por token (X-Token),
    gerado no /login — não reenvia mais a senha a cada chamada."""
    token = request.headers.get("X-Token")
    sessao = clientes.validar_sessao(token)
    if not sessao or sessao["tipo"] != "admin":
        return jsonify({"erro": "não autenticado"}), 401
    return None


def _cliente_autenticado():
    """Retorna o cliente_id da sessão válida no header X-Token, ou None."""
    token = request.headers.get("X-Token")
    sessao = clientes.validar_sessao(token)
    if not sessao or sessao["tipo"] != "cliente":
        return None
    return sessao["identificador"]


# ===================== threads de fundo =====================

def monitorar_quedas():
    while True:
        alertas.verificar_quedas()
        time.sleep(5)


# ===================== rotas de ESP32 =====================

@app.route("/checkin/<galpao_id>", methods=["GET", "POST"])
def receber_checkin(galpao_id):
    temp_str = request.args.get("temp")
    temperatura = None
    if temp_str is not None:
        try:
            temperatura = float(temp_str)
        except ValueError:
            temperatura = None

    alertas.marcar_online(galpao_id, temperatura=temperatura)

    # se esse galpao_id também for usado como código de autoatendimento
    # por algum dispositivo, checa o limite dele a cada checkin (a cada
    # 10s), não só quando alguém abre o app e consulta /temperatura —
    # assim não depende de ninguém "ativar" a verificação manualmente.
    if temperatura is not None:
        alertas.checar_limites_autoatendimento(galpao_id, temperatura)

    return jsonify({"status": "recebido", "galpao": galpao_id, "temperatura": temperatura}), 200


@app.route("/temperatura/<galpao_id>", methods=["GET"])
def consultar_temperatura(galpao_id):
    dado = alertas.temperatura_do_galpao(galpao_id)
    if dado is None:
        return jsonify({"galpao": galpao_id, "temperatura": None, "mensagem": "sem leitura ainda"}), 200

    return jsonify({"galpao": galpao_id, "temperatura": dado["valor"], "timestamp": dado["timestamp"]}), 200


@app.route("/status_grupo/<grupo_id>", methods=["GET"])
def consultar_status_grupo(grupo_id):
    return jsonify(alertas.status_grupo(grupo_id)), 200


# ===================== login =====================

@app.route("/login", methods=["POST"])
def login():
    corpo = request.get_json(force=True) or {}
    tipo = corpo.get("tipo")  # "cliente" ou "admin"

    if tipo == "admin":
        if _login_bloqueado("admin"):
            return jsonify({"erro": "muitas tentativas, tente novamente mais tarde"}), 429
        if corpo.get("senha") != ADMIN_SENHA:
            _registrar_falha_login("admin")
            return jsonify({"erro": "senha inválida"}), 401
        _limpar_falhas_login("admin")
        token = clientes.criar_sessao("admin", "admin")
        return jsonify({"ok": True, "tipo": "admin", "token": token}), 200

    if tipo == "cliente":
        cliente_id = corpo.get("cliente_id")
        senha = corpo.get("senha")
        if _login_bloqueado(cliente_id):
            return jsonify({"erro": "muitas tentativas, tente novamente mais tarde"}), 429

        cliente = clientes.autenticar_gerenciado(cliente_id, senha)
        if not cliente:
            _registrar_falha_login(cliente_id)
            return jsonify({"erro": "usuário ou senha inválidos"}), 401

        _limpar_falhas_login(cliente_id)

        push_token = corpo.get("push_token")
        if push_token:
            clientes.registrar_push_token_gerenciado(cliente_id, push_token)

        token = clientes.criar_sessao("cliente", cliente_id)

        return jsonify({
            "ok": True,
            "tipo": "cliente",
            "token": token,
            "cliente_id": cliente_id,
            "nome": cliente["nome"],
            "galpoes": cliente["galpoes"],
            "whatsapp_ativo": cliente["whatsapp_ativo"],
            "pushover_ativo": cliente["pushover_ativo"],
            "twilio_ativo": cliente["twilio_ativo"],
        }), 200

    return jsonify({"erro": "tipo de login inválido"}), 400


# ===================== admin: gerenciamento de clientes =====================

@app.route("/admin/clientes", methods=["GET"])
def admin_listar_clientes():
    erro = _exige_admin()
    if erro:
        return erro
    return jsonify(clientes.listar_gerenciados()), 200


@app.route("/admin/cliente", methods=["POST"])
def admin_criar_cliente():
    erro = _exige_admin()
    if erro:
        return erro
    corpo = request.get_json(force=True) or {}
    try:
        cliente = clientes.criar_gerenciado(corpo["cliente_id"], corpo["nome"], corpo["senha"])
        return jsonify(cliente), 201
    except (KeyError, ValueError) as e:
        return jsonify({"erro": str(e)}), 400


@app.route("/admin/cliente/<cliente_id>", methods=["PUT"])
def admin_editar_cliente(cliente_id):
    erro = _exige_admin()
    if erro:
        return erro
    corpo = request.get_json(force=True) or {}
    try:
        cliente = clientes.atualizar_gerenciado(cliente_id, corpo)
        return jsonify(cliente), 200
    except ValueError as e:
        return jsonify({"erro": str(e)}), 404


@app.route("/admin/cliente/<cliente_id>/galpao", methods=["POST"])
def admin_adicionar_galpao(cliente_id):
    erro = _exige_admin()
    if erro:
        return erro
    corpo = request.get_json(force=True) or {}
    try:
        cliente = clientes.adicionar_galpao(cliente_id, corpo["galpao_id"], corpo.get("nome", corpo["galpao_id"]))
        return jsonify(cliente), 201
    except (KeyError, ValueError) as e:
        return jsonify({"erro": str(e)}), 400


@app.route("/admin/cliente/<cliente_id>/contato", methods=["POST"])
def admin_adicionar_contato(cliente_id):
    erro = _exige_admin()
    if erro:
        return erro
    corpo = request.get_json(force=True) or {}
    try:
        cliente = clientes.adicionar_contato(cliente_id, corpo)
        return jsonify(cliente), 201
    except ValueError as e:
        return jsonify({"erro": str(e)}), 404


# ===================== cliente gerenciado: ajustar limite do próprio galpão =====================

@app.route("/cliente/<cliente_id>/galpao/<galpao_id>/limite", methods=["PUT"])
def cliente_ajustar_limite(cliente_id, galpao_id):
    cliente_id_autenticado = _cliente_autenticado()
    if cliente_id_autenticado != cliente_id:
        return jsonify({"erro": "não autenticado"}), 401

    corpo = request.get_json(force=True) or {}
    try:
        resultado = clientes.atualizar_galpao(cliente_id, galpao_id, corpo)
        return jsonify(resultado), 200
    except ValueError as e:
        return jsonify({"erro": str(e)}), 404


@app.route("/app/galpoes", methods=["GET"])
def app_listar_galpoes():
    cliente_id = _cliente_autenticado()
    if not cliente_id:
        return jsonify({"erro": "não autenticado"}), 401
    cliente = clientes.obter_gerenciado(cliente_id)
    if not cliente:
        return jsonify({"erro": "cliente não encontrado"}), 404
    return jsonify(alertas.status_galpoes_cliente(cliente)), 200


@app.route("/app/galpoes/<galpao_id>/historico", methods=["GET"])
def app_historico_galpao(galpao_id):
    cliente_id = _cliente_autenticado()
    if not cliente_id:
        return jsonify({"erro": "não autenticado"}), 401
    cliente = clientes.obter_gerenciado(cliente_id)
    if not cliente or not any(g["id"] == galpao_id for g in cliente.get("galpoes", [])):
        return jsonify({"erro": "galpão não encontrado para esse cliente"}), 404
    return jsonify(alertas.historico_do_galpao(galpao_id)), 200


@app.route("/admin/clientes/<cliente_id>/entrar", methods=["POST"])
def admin_entrar_como_cliente(cliente_id):
    erro = _exige_admin()
    if erro:
        return erro
    cliente = clientes.obter_gerenciado(cliente_id)
    if not cliente:
        return jsonify({"erro": "cliente não encontrado"}), 404
    token = clientes.criar_sessao("cliente", cliente_id)
    return jsonify({"token": token, "nome_cliente": cliente["nome"]}), 200


# ===================== autoatendimento =====================

@app.route("/autoatendimento/gerar_codigo", methods=["POST"])
def autoatendimento_gerar_codigo():
    """Chamado quando o usuário pede um ESP32 novo de autoatendimento
    (fora do fluxo principal, mas fica pronto caso vire self-service)."""
    codigo = clientes.gerar_codigo_autoatendimento()
    return jsonify({"codigo": codigo}), 201


@app.route("/autoatendimento/<codigo>/registrar", methods=["POST"])
def autoatendimento_registrar(codigo):
    """Chamado pelo app toda vez que o usuário adiciona esse código na
    lista local — registra (ou re-registra) o push token daquele
    celular pra esse código específico."""
    corpo = request.get_json(force=True) or {}
    push_token = corpo.get("push_token")
    if not push_token:
        return jsonify({"erro": "push_token é obrigatório"}), 400
    registro = clientes.registrar_dispositivo_autoatendimento(codigo, push_token)
    return jsonify(registro), 200


@app.route("/autoatendimento/<codigo>/limite", methods=["PUT"])
def autoatendimento_ajustar_limite(codigo):
    corpo = request.get_json(force=True) or {}
    push_token = corpo.get("push_token")
    if not push_token:
        return jsonify({"erro": "push_token é obrigatório"}), 400
    try:
        registro = clientes.atualizar_limite_autoatendimento(codigo, push_token, corpo)
        return jsonify(registro), 200
    except ValueError as e:
        return jsonify({"erro": str(e)}), 404


@app.route("/", methods=["GET"])
def status():
    return jsonify({"status": "online"}), 200


if __name__ == "__main__":
    threading.Thread(target=monitorar_quedas, daemon=True).start()
    threading.Thread(target=backup_git.loop_backup_diario, daemon=True).start()

    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
