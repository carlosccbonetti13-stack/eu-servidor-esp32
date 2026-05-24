import os
import time
import threading
from flask import Flask, jsonify
from twilio.rest import Client

app = Flask(__name__)

# --- CREDENCIAIS SEGURAS (VARIÁVEIS DE AMBIENTE) ---
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER = os.environ.get('TWILIO_NUMBER', 'whatsapp:+14155238886')
SEU_WHATSAPP = os.environ.get('SEU_WHATSAPP', 'whatsapp:+5548920004745')

# --- VARIÁVEIS DE CONTROLE ---
ultimo_ping = time.time()  
alerta_enviado = False     

def enviar_whatsapp():
    """Dispara o alerta via Twilio com a mensagem do Galpão"""
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
    """Thread que checa se o ESP32 sumiu por mais de 15 segundos"""
    global alerta_enviado
    while True:
        tempo_sem_resposta = time.time() - ultimo_ping
        
        # Alerta configurado para disparar exatamente com 15 segundos de ausência
        if tempo_sem_resposta > 15.0 and not alerta_enviado:
            print(f"[ALERTA] ESP32 offline há {tempo_sem_resposta:.1f}s!")
            enviar_whatsapp()
            alerta_enviado = True  # Evita repetir o envio na mesma queda
            
        time.sleep(1)

# ROTA AJUSTADA: Captura o sinal direto na raiz "/" enviada pelo seu ESP32
@app.route('/', methods=['POST', 'GET'])
def receber_ping():
    """Rota que recebe o sinal do ESP32 de 10 em 10 segundos"""
    global ultimo_ping, alerta_enviado
    ultimo_ping = time.time()  # Reseta o cronômetro com o tempo atual
    
    if alerta_enviado:
        print("[SISTEMA] ESP32 voltou a responder!")
        alerta_enviado = False  # Reseta o estado para permitir novos alertas no futuro
        
    return jsonify({"status": "recebido"}), 200

if __name__ == '__main__':
    # Inicia o monitor em segundo plano
    thread_monitor = threading.Thread(target=monitorar_esp32, daemon=True)
    thread_monitor.start()
    
    # Roda o servidor Flask
    app.run(host='0.0.0.0')
