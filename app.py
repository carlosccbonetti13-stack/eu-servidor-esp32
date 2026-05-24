import os
import time
import threading
from flask import Flask, jsonify
from twilio.rest import Client

app = Flask(__name__)

# --- AGORA AS CREDENCIAIS SÃO PUXADAS DA MEMÓRIA DO SERVIDOR DE FORMA SEGURA ---
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER = os.environ.get('TWILIO_NUMBER', 'whatsapp:+14155238886')
SEU_WHATSAPP = os.environ.get('SEU_WHATSAPP', 'whatsapp:+5548920004745')

# --- O RESTANTE DO CÓDIGO CONTINUA EXATAMENTE IGUAL ---
ultimo_ping = time.time()  
alerta_enviado = False     

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

def monitorar_esp32():
    global alerta_enviado
    while True:
        tempo_sem_resposta = time.time() - ultimo_ping
        if tempo_sem_resposta > 10.0 and not alerta_enviado:
            print(f"[ALERTA] ESP32 offline há {tempo_sem_resposta:.1f}s!")
            enviar_whatsapp()
            alerta_enviado = True  
        time.sleep(1)

@app.route('/ping', methods=['POST', 'GET'])
def receber_ping():
    global ultimo_ping, alerta_enviado
    ultimo_ping = time.time()  
    if alerta_enviado:
        print("[SISTEMA] ESP32 voltou a responder!")
        alerta_enviado = False  
    return jsonify({"status": "recebido"}), 200

if __name__ == '__main__':
    thread_monitor = threading.Thread(target=monitorar_esp32, daemon=True)
    thread_monitor.start()
    app.run(host='0.0.0.0')
