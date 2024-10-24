import requests
import time

def send_request():
    for i in range(10):  # Envia 10 requisições como exemplo
        try:
            response = requests.post('http://api:5000/compra', json={"id_usuario": i})
            print(response.status_code, response.json())
        except requests.exceptions.ConnectionError:
            print("Aguardando API...")
            time.sleep(5)

def response():
    for i in range(10):
        try:
            # Corrigido para passar o id_usuario na URL
            response = requests.get(f'http://api:5000/resultado/{i}')
            print(response.status_code, response.json())
        except requests.exceptions.ConnectionError:
            print("Aguardando API...")
            time.sleep(5)

while True:  # Loop infinito para manter o contêiner ativo
    send_request()
    response()
    print("Aguardando para enviar mais requisições...")
    time.sleep(10)  # Pausa de 10 segundos antes de enviar mais
