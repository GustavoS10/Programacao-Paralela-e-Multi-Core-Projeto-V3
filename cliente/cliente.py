import requests
import time


def send_request(num_requests):
    """Função para enviar requisições de compra de ingresso para a API, apenas uma vez por ingresso."""
    for i in range(num_requests):  # Envia um número específico de requisições
        try:
            response = requests.post(
                'http://api:5000/compra', json={"usuario_id": i, "evento_id": 100, "ingresso_id": i})
            print(
                f"[Cliente] Requisição de compra enviada para Ingresso ID: {i}, Usuário ID: {i}", flush=True)
            print("Resposta da API:", response.status_code,
                  response.json(), flush=True)
        except requests.exceptions.ConnectionError:
            print("[Cliente] Aguardando API...", flush=True)
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

                # Adiciona um controle para verificar se a resposta é válida
                if response.status_code != 200:
                    print(
                        f"[Cliente] Erro na resposta da API para Ingresso ID: {i}. Status Code: {response.status_code}", flush=True)
                    continue

                data = response.json()
                print(
                    f"[Cliente] Consulta de status para Ingresso ID: {i}, Usuário ID: {i}", flush=True)
                print("Resposta da API:", response.status_code, data, flush=True)

                # Verifica se o status do ingresso está finalizado
                if data.get("status") == "vendido":
                    completed.add(i)
                    print(
                        f"[Cliente] Ingresso ID {i} processado com status final: Vendido", flush=True)

                elif data.get("status") == "esgotado":
                    completed.add(i)
                    print(
                        f"[Cliente] Ingresso ID {i} não pode ser processado: Ingressos Esgotados", flush=True)

            except requests.exceptions.ConnectionError:
                print("[Cliente] Aguardando API...", flush=True)
                time.sleep(5)
            except ValueError:
                print(
                    f"[Cliente] Erro ao decodificar a resposta da API para Ingresso ID: {i}.", flush=True)
                continue

        # Pausa antes da próxima consulta para evitar excesso de chamadas
        time.sleep(1)


if __name__ == "__main__":
    num_requests = 150  # Número total de ingressos para solicitar
    send_request(num_requests)  # Envia as requisições de compra apenas uma vez
    # Consulta o status até que todos estejam finalizados
    check_status(num_requests)
    print("[Cliente] Todas as requisições foram processadas.", flush=True)
