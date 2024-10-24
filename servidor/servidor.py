import pika
import threading
import time

def criar_conexao():
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials, heartbeat=60)
    return pika.BlockingConnection(parameters)

def consumir_fila_reserva():
    while True:
        try:
            connection = criar_conexao()  # Criar conexão separada para cada thread
            channel = connection.channel()

            while True:
                method_frame, header_frame, body = channel.basic_get(queue='FilaReserva', auto_ack=True)
                if body:
                    print(f"Recebendo da Fila Reserva: {body}")
                    # Envia a mensagem para a fila de processamento
                    channel.basic_publish(exchange='', routing_key='FilaProcessamento', body=body)
                    print(f"Mensagem enviada para a Fila Processamento: {body.decode()}")
                else:
                    print("Nenhuma mensagem na Fila Reserva.")
                time.sleep(3)

        except pika.exceptions.StreamLostError:
            print("Conexão perdida! Tentando reconectar...")
            time.sleep(5)

def consumir_fila_processamento_processo():
    while True:
        try:
            connection = criar_conexao()  # Criar conexão separada para cada thread
            channel = connection.channel()

            while True:
                method_frame, header_frame, body = channel.basic_get(queue='FilaProcessamento', auto_ack=True)
                if body:
                    print(f"Recebendo da Fila Processamento: {body}")
                    # Envia a mensagem para a fila de saída
                    channel.basic_publish(exchange='', routing_key='FilaSaida', body=body)
                    print(f"Mensagem enviada para a Fila Saida: {body.decode()}")
                else:
                    print("Nenhuma mensagem na Fila Processamento.")
                time.sleep(3)

        except pika.exceptions.StreamLostError:
            print("Conexão perdida! Tentando reconectar...")
            time.sleep(5)

if __name__ == "__main__":
    # Criar threads
    thread_fila_reserva = threading.Thread(target=consumir_fila_reserva)
    thread_processamento = threading.Thread(target=consumir_fila_processamento_processo)

    # Iniciar as threads
    thread_fila_reserva.start()
    thread_processamento.start()

    # Esperar as threads terminarem
    thread_fila_reserva.join()
    thread_processamento.join()
