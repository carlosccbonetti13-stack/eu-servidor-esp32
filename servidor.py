"""
Servidor principal — recebe checkins dos ESP32 (um por galpão), mantém a
rotina de verificação de quedas em segundo plano (WhatsApp + Pushover +
Twilio), e expõe o status de cada grupo de sirene para os ESP32
"verificadores" acionarem o relé externo.

Cada ESP32 de galpão manda um GET ou POST periódico (a cada 10s) para:
    /checkin/<galpao_id>
Exemplo: ESP32 do galpão 01 chama http://seu-servidor/checkin/01

Cada ESP32 de sirene (até 2 por grupo, para redundância) consulta
periodicamente (ex: a cada 8-10s):
    /status_grupo/<grupo_id>
e aciona o relé de estado sólido se "algum_em_queda" vier true.
"""

import os
import threading
import time

from flask import Flask, jsonify, request

import whatsapp_alertas as alertas

app = Flask(__name__)


def monitorar_quedas():
    """Thread em segundo plano: checa periodicamente se algum galpão caiu
    e escalona os alertas (WhatsApp/Pushover imediato, Twilio 10/20/30min)."""
    while True:
        alertas.verificar_quedas()
        time.sleep(5)  # verifica a cada 5s (TIMEOUT_SEGUNDOS é 60s, então sobra margem)


@app.route("/checkin/<galpao_id>", methods=["GET", "POST"])
def receber_checkin(galpao_id):
    """Rota que cada ESP32 de galpão chama a cada 10s, identificando seu
    próprio galpão na URL. Aceita opcionalmente ?temp=23.5 com a última
    leitura do sensor DS18B20."""
    temp_str = request.args.get("temp")
    temperatura = None
    if temp_str is not None:
        try:
            temperatura = float(temp_str)
        except ValueError:
            temperatura = None

    alertas.marcar_online(galpao_id, temperatura=temperatura)
    return jsonify({"status": "recebido", "galpao": galpao_id, "temperatura": temperatura}), 200


@app.route("/temperatura/<galpao_id>", methods=["GET"])
def consultar_temperatura(galpao_id):
    """Retorna a última temperatura conhecida desse galpão."""
    dado = alertas.temperatura_do_galpao(galpao_id)
    if dado is None:
        return jsonify({"galpao": galpao_id, "temperatura": None, "mensagem": "sem leitura ainda"}), 200
    return jsonify({"galpao": galpao_id, "temperatura": dado["valor"], "timestamp": dado["timestamp"]}), 200


@app.route("/status_grupo/<grupo_id>", methods=["GET"])
def consultar_status_grupo(grupo_id):
    """Rota que cada ESP32 de sirene consulta periodicamente para saber se
    deve acionar o relé externo (sirene) daquele grupo de galpões."""
    return jsonify(alertas.status_grupo(grupo_id)), 200


@app.route("/", methods=["GET"])
def status():
    """Rota simples para confirmar que o servidor está no ar."""
    return jsonify({"status": "online"}), 200


if __name__ == "__main__":
    thread_monitor = threading.Thread(target=monitorar_quedas, daemon=True)
    thread_monitor.start()

    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
