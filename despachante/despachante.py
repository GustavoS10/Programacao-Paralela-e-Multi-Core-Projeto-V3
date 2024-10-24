import pika
import threading
import time

def criar_conexao():
    # Definir credenciais (se necessário, substitua pelo seu usuário e senha)
    credentials = pika.PlainCredentials('guest', 'guest')

    # Configurar heartbeat para evitar timeouts (30 segundos, ajustável)
    parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials, heartbeat=30)

    # Estabelecer conexão
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Garantir que as filas existem e são duráveis
    channel.queue_declare(queue='FilaEntrada', durable=True)  # Adicionada a fila de entrada
    channel.queue_declare(queue='FilaReserva', durable=True)
    channel.queue_declare(queue='FilaProcessamento', durable=True)
    channel.queue_declare(queue='FilaSaida', durable=True)
    
    return connection, channel  # Retorna tanto a conexão quanto o canal

def consumir_fila_entrada(channel):
    while True:
        method_frame, header_frame, body = channel.basic_get(queue='FilaEntrada', auto_ack=True)
        if body:
            print(f"Recebendo da Fila Entrada: {body}")
            # Envia a mensagem para a fila de reserva
            channel.basic_publish(exchange='', routing_key='FilaReserva', body=body)
            print(f"Mensagem enviada para a Fila Reserva: {body}")
        else:
            print("Nenhuma mensagem na Fila Entrada.")
        time.sleep(1)

# def reservar(channel):
#     while True:
#         method_frame, header_frame, body = channel.basic_get(queue='FilaReserva', auto_ack=True)
#         if body:
#             print(f"Reserva recebida: {body}")
#             channel.basic_publish(exchange='', routing_key='FilaProcessamento', body=body)
#             print(f"Mensagem enviada para a Fila Processamento: {body}")
#         else:
#             print("Nenhuma mensagem na fila de reserva.")
#         time.sleep(2)

# def processar(channel):
#     while True:
#         method_frame, header_frame, body = channel.basic_get(queue='FilaProcessamento', auto_ack=True)
#         if body:
#             print(f"Processando compra: {body}")
#             channel.basic_publish(exchange='', routing_key='FilaSaida', body=f"{body} - Compra realizada")
#             print(f"Mensagem enviada para a Fila Saida: {body}")
#         else:
#             print("Nenhuma mensagem na fila de processamento.")
#         time.sleep(2)

if __name__ == "__main__":
    connection, channel = criar_conexao()
    print("Despachante iniciado. Conexão estabelecida com RabbitMQ.")

    # Criar threads
    thread_fila_entrada = threading.Thread(target=consumir_fila_entrada, args=(channel,))
    # thread_reserva = threading.Thread(target=reservar, args=(channel,))
    # thread_processamento = threading.Thread(target=processar, args=(channel,))

    # Iniciar as threads
    thread_fila_entrada.start()
    # thread_reserva.start()
    # thread_processamento.start()

    # Esperar as threads terminarem
    thread_fila_entrada.join()
    # thread_reserva.join()
    # thread_processamento.join()
