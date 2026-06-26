"""
Servidor principal — recebe checkins dos ESP32 (um por galpão) e mantém a
rotina de verificação de quedas em segundo plano.

Cada ESP32 deve mandar um GET ou POST periódico (a cada 10s) para:
    /checkin/<galpao_id>

Exemplo: ESP32 do galpão 01 chama http://seu-servidor/checkin/01
"""

import threading
import time

from flask import Flask, jsonify

import whatsapp_alertas as alertas

app = Flask(__name__)


def monitorar_quedas():
    """Thread em segundo plano: checa periodicamente se algum galpão caiu."""
    while True:
        alertas.verificar_quedas()
        time.sleep(5)  # verifica a cada 5s (TIMEOUT_SEGUNDOS é 60s, então sobra margem)


@app.route("/checkin/<galpao_id>", methods=["GET", "POST"])
def receber_checkin(galpao_id):
    """Rota que cada ESP32 chama a cada 10s, identificando seu próprio galpão na URL."""
    alertas.marcar_online(galpao_id)
    return jsonify({"status": "recebido", "galpao": galpao_id}), 200


@app.route("/", methods=["GET"])
def status():
    """Rota simples para confirmar que o servidor está no ar."""
    return jsonify({"status": "online"}), 200


if __name__ == "__main__":
    thread_monitor = threading.Thread(target=monitorar_quedas, daemon=True)
    thread_monitor.start()

    app.run(host="0.0.0.0", port=5000)
