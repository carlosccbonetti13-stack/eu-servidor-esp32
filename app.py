from flask import Flask, request, jsonify
import threading
import time
import requests

app = Flask(__name__)

# ==========================================
#   CONFIGURAÇÃO DOS SEUS DADOS (SALVO)
# ==========================================
TELEFONE = "5548920028472"  
API_KEY = "3838771"  
# ==========================================

# Controle de estado do dispositivo
ULTIMO_SINAL = time.time()
DISPOSITIVO_ALERTA_DISPARADO = False
TEMPO_LIMITE_QUEDA = 45 

def enviar_whatsapp(mensagem):
    url = f"https://callmebot.com{TELEFONE}&text={requests.utils.quote(mensagem)}&apikey={API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print("[WHATSAPP] Alerta enviado com sucesso!")
        else:
            print(f"[WHATSAPP] Falha ao enviar: {response.text}")
    except Exception as e:
        print(f"[WHATSAPP] Erro de rede: {e}")

def monitorar_status():
    global ULTIMO_SINAL, DISPOSITIVO_ALERTA_DISPARADO
    while True:
        tempo_decorrido = time.time() - ULTIMO_SINAL
        
        if tempo_decorrido > TEMPO_LIMITE_QUEDA and not DISPOSITIVO_ALERTA_DISPARADO:
            print(f"[ALERTA] O ESP32 parou de responder há {int(tempo_decorrido)} segundos!")
            enviar_whatsapp("⚠️ ATENÇÃO: O seu Arduino/ESP32 parou de responder e pode estar offline!")
            DISPOSITIVO_ALERTA_DISPARADO = True
            
        time.sleep(5)

@app.route('/ping/', methods=['POST'])
def ping():
    global ULTIMO_SINAL, DISPOSITIVO_ALERTA_DISPARADO
    dados = request.json
    print(f"[SERVER] Sinal recebido do dispositivo: {dados.get('device')}")
    
    ULTIMO_SINAL = time.time()
    
    if DISPOSITIVO_ALERTA_DISPARADO:
        enviar_whatsapp("✅ O seu Arduino/ESP32 voltou a funcionar e está online novamente.")
        DISPOSITIVO_ALERTA_DISPARADO = False
        
    return jsonify({"status": "recebido"}), 200

if __name__ == '__main__':
    threading.Thread(target=monitorar_status, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
