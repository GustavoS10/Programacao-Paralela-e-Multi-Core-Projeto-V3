import requests
import time
import json


def send_request(num_requests, delay=0.1):
    """Função para enviar requisições de compra de ingresso para a API."""
    for i in range(num_requests):
        try:
            response = requests.post(
                'http://api:5000/compra',
                json={"usuario_id": i, "evento_id": 100, "ingresso_id": i})
            print(
                f"[Cliente] Requisição de compra enviada para Usuário ID: {i}, Evento ID: {100}, Ingresso ID: {i}",
                flush=True,
            )
            print("Resposta da API:", response.status_code,
                  response.json(), flush=True)
        except requests.exceptions.ConnectionError:
            print("[Cliente] Aguardando API...", flush=True)

        # Adiciona delay entre requisições
        time.sleep(delay)


def check_status(num_requests, delay=0.5):
    """Função para consultar o status de cada ingresso até que o processamento esteja completo."""
    pending = set(range(num_requests))
    completed = set()

    while pending:
        print(
            f"[DEBUG] Pendentes: {pending}, Processados: {completed}", flush=True)

        for user_id in list(pending):
            try:
                print(
                    f"[DEBUG] Verificando status para Usuário ID: {user_id}", flush=True)
                response = requests.get(f'http://api:5000/resultado/{user_id}')

                if response.status_code != 200:
                    print(
                        f"[Cliente] Erro na resposta da API para Usuário ID: {user_id}. Status Code: {response.status_code}",
                        flush=True,
                    )
                    continue

                data = response.json()
                print("Data: ", data)
                if data.get("usuario_comprador_id") == user_id:
                    ingresso_id = data.get("ingresso_id")
                    status = data.get("status")
                    pending.remove(user_id)
                    completed.add(user_id)
                    print(
                        f"[Cliente] Usuário ID {user_id} processado com status final: {status}, Ingresso ID: {ingresso_id}",
                        flush=True,
                    )
                elif data.get("status") == "Aguardando processamento":
                    print(
                        f"[Cliente] Usuário ID {user_id} ainda está aguardando processamento.", flush=True)
                elif data.get("status") == "Esgotado":
                    pending.remove(user_id)
                    completed.add(user_id)
                    print(
                        f"[Cliente] Usuário ID {user_id} informado que os ingressos estão esgotados. Removendo das verificações.",
                        flush=True,
                    )
                else:
                    print(
                        f"[Cliente] Resposta inesperada para Usuário ID {user_id}: {data}", flush=True)

            except requests.exceptions.ConnectionError:
                print("[Cliente] Aguardando API...", flush=True)
            except ValueError as e:
                print(
                    f"[Cliente] Erro ao decodificar a resposta da API para Usuário ID: {user_id}. Erro: {e}", flush=True)
                continue

        # Adiciona delay entre verificações de status
        time.sleep(delay)

        if pending:
            print(
                f"[DEBUG] {len(pending)} usuários ainda pendentes", flush=True)

    print("[DEBUG] Todos os usuários foram processados.", flush=True)


if __name__ == "__main__":
    num_requests = 2000
    send_request(num_requests, delay=0)  # Delay de 200ms entre envios
    # Delay de 1 segundo entre verificações
    check_status(num_requests, delay=1)
    print("[Cliente] Todas as requisições foram processadas.", flush=True)
