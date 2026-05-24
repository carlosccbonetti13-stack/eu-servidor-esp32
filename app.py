import os
import time
from flask import Flask, jsonify
from twilio.rest import Client

app = Flask(__name__)

# --- CREDENCIAIS SEGURAS ---
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER = os.environ.get('TWILIO_NUMBER', 'whatsapp:+14155238886')
SEU_WHATSAPP = os.environ.get('SEU_WHATSAPP', 'whatsapp:+5548920004745')

ARQUIVO_PING = "ultimo_ping.txt"
ARQUIVO_ALERTA = "status_alerta.txt"

def salvar_estado(arquivo, valor):
    with open(arquivo, "w") as f:
        f.write(str(valor))

def ler_estado(arquivo, padrao):
    if not os.path.exists(arquivo):
        return padrao
    with open(arquivo, "r") as f:
        return f.read().strip()

def enviar_whatsapp():
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        mensagem = client.messages.create(
            from_=TWILIO_NUMBER,
            to=SEU_WHATSAPP,
            body='QUEDA DE ELETRICIDADE GALPÃO 01'
        )
        print(f"[TWILIO] Mensagem enviada! SID: {mensagem.sid}")
    except Exception as e:
        print(f"[ERRO TWILIO] Falha ao enviar: {e}")

# ROTA DO ESP32
@app.route('/', methods=['POST', 'GET'])
def receber_ping():
    # Salva o momento atual no arquivo
    salvar_estado(ARQUIVO_PING, time.time())
    
    # Se havia um alerta ativo, reseta o estado porque o ESP32 voltou
    if ler_estado(ARQUIVO_ALERTA, "false") == "true":
        print("[SISTEMA] ESP32 voltou a responder!")
        salvar_estado(ARQUIVO_ALERTA, "false")
        
    return jsonify({"status": "recebido"}), 200

# ROTA DE CHECAGEM DO RENDER (CRON/MONITOR)
@app.route('/verificar', methods=['GET'])
def verificar_queda():
    agora = time.time()
    ultimo_ping = float(ler_estado(ARQUIVO_PING, agora))
    alerta_enviado = ler_estado(ARQUIVO_ALERTA, "false") == "true"
    
    tempo_sem_resposta = agora - ultimo_ping
    
    if tempo_sem_resposta > 15.0 and not alerta_enviado:
        print(f"[ALERTA] ESP32 offline há {tempo_sem_resposta:.1f}s!")
        enviar_whatsapp()
        salvar_estado(ARQUIVO_ALERTA, "true")
        return jsonify({"status": "alerta_disparado", "tempo": tempo_sem_resposta}), 200

    return jsonify({"status": "ok", "tempo_sem_resposta": tempo_sem_resposta}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0')
