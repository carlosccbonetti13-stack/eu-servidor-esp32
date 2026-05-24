import os
import time
import threading
from flask import Flask, jsonify
from twilio.rest import Client

app = Flask(__name__)

# --- CREDENCIAIS SEGURAS ---
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER = os.environ.get('TWILIO_NUMBER', 'whatsapp:+14155238886')
SEU_WHATSAPP = os.environ.get('SEU_WHATSAPP', 'whatsapp:+5548920004745')

# --- VARIÁVEIS DE CONTROLE ---
ultimo_ping = time.time()  
alerta_enviado = False     

def enviar_whatsapp():
    """Dispara o alerta via Twilio"""
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
    """Thread que roda em segundo plano checando o tempo de silêncio de 60s"""
    global alerta_enviado
    while True:
        tempo_sem_resposta = time.time() - ultimo_ping
        
        # SÓ DISPARA SE FICAR MAIS DE 60 SEGUNDOS SEM NENHUM PING
        if tempo_sem_resposta > 60.0 and not alerta_enviado:
            print(f"[ALERTA] Galpão sem comunicação há {tempo_sem_resposta:.1f}s!")
            enviar_whatsapp()
            alerta_enviado = True  # Bloqueia envios repetidos na mesma queda
            
        time.sleep(1)

@app.route('/', methods=['POST', 'GET'])
def receber_ping():
    """Rota que o ESP32 chama a cada 10 segundos"""
    global ultimo_ping, alerta_enviado
    ultimo_ping = time.time()  # Reseta o cronômetro para zero toda vez que chega um ping
    
    if alerta_enviado:
        print("[SISTEMA] O ESP32 voltou a responder!")
        alerta_enviado = False  # Permite novos alertas se ele cair de novo no futuro
        
    return jsonify({"status": "recebido"}), 200

if __name__ == '__main__':
    # Inicia o monitor em segundo plano que fica contando os segundos
    thread_monitor = threading.Thread(target=monitorar_esp32, daemon=True)
    thread_monitor.start()
    
    app.run(host='0.0.0.0', port=5000)
