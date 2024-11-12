import requests
import time


def send_request(num_requests):
    """Função para enviar requisições de compra de ingresso para a API, apenas uma vez por ingresso."""
    for i in range(num_requests):  # Envia um número específico de requisições
        try:
            response = requests.post(
                'http://api:5000/compra', json={"usuario_id": i, "evento_id": 100, "ingresso_id": i})
            print(
                f"[Cliente] Requisição de compra enviada para Ingresso ID: {i}, Usuário ID: {i}")
            print("Resposta da API:", response.status_code, response.json())
        except requests.exceptions.ConnectionError:
            print("[Cliente] Aguardando API...")
            time.sleep(5)


def check_status(num_requests):
    """Função para consultar o status de cada ingresso apenas até que o processamento esteja completo."""
    completed = set()  # Conjunto para armazenar IDs de usuários com status finalizado
    while len(completed) < num_requests:
        for i in range(num_requests):
            if i in completed:
                continue  # Pula se já obteve resposta final para este ingresso

            try:
                response = requests.get(f'http://api:5000/resultado/{i}')
                data = response.json()
                print(
                    f"[Cliente] Consulta de status para Ingresso ID: {i}, Usuário ID: {i}")
                print("Resposta da API:", response.status_code, data)

                # Verifica se o status do ingresso está finalizado
                if data.get("status") in ["vendido", "esgotado"]:
                    # Adiciona ao conjunto de ingressos completados
                    completed.add(i)
                    print(
                        f"[Cliente] Ingresso ID {i} processado com status final: {data.get('status')}")

            except requests.exceptions.ConnectionError:
                print("[Cliente] Aguardando API...")
                time.sleep(5)

        # Pausa antes da próxima consulta para evitar excesso de chamadas
        time.sleep(10)


if __name__ == "__main__":
    num_requests = 150  # Número total de ingressos para solicitar
    send_request(num_requests)  # Envia as requisições de compra apenas uma vez
    # Consulta o status até que todos estejam finalizados
    check_status(num_requests)
    print("[Cliente] Todas as requisições foram processadas.")
