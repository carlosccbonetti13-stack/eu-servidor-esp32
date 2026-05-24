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

# Guarda o horário do último ping na memória estável do processo
VAR_ULTIMO_PING = time.time()

def enviar_whatsapp():
    """Dispara o alerta via Twilio"""
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        mensagem = client.messages.create(
            from_=TWILIO_NUMBER,
            to=SEU_WHATSAPP,
            body='QUEDA DE ELETRICIDADE GALPÃO 01'
        )
        print(f"[TWILIO] Mensagem de alerta enviada! SID: {mensagem.sid}")
    except Exception as e:
        print(f"[ERRO TWILIO] Falha ao enviar: {e}")

@app.route('/', methods=['POST', 'GET'])
def receber_ping():
    global VAR_ULTIMO_PING
    agora = time.time()
    
    # Calcula quantos segundos se passaram desde o último sinal que o ESP32 mandou
    tempo_decorrido = agora - VAR_ULTIMO_PING
    
    # Se o tempo sem sinal for maior que 10 segundos, significa que houve uma queda!
    if tempo_decorrido > 10.0:
        print(f"[ALERTA DETECTADO] O ESP32 ficou desligado por {tempo_decorrido:.1f} segundos!")
        enviar_whatsapp()
    else:
        print(f"[SISTEMA] Ping recebido normalmente. Intervalo: {tempo_decorrido:.1f}s")
        
    # Atualiza o relógio para a próxima checagem
    VAR_ULTIMO_PING = agora
    return jsonify({"status": "recebido", "intervalo_anterior": tempo_decorrido}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
