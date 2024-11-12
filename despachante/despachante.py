import pika
import time

NUM_FILAS_RESERVA = 2  # Defina o número de filas de reserva


def criar_conexao():
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        'rabbitmq', 5672, '/', credentials, heartbeat=30)
    connection = pika.BlockingConnection(parameters)
    return connection


def despachante():
    connection = criar_conexao()
    channel = connection.channel()
    channel.queue_declare(queue='FilaEntrada', durable=True)

    for i in range(1, NUM_FILAS_RESERVA + 1):
        channel.queue_declare(queue=f'FilaReserva{i}', durable=True)

    while True:
        method_frame, header_frame, body = channel.basic_get(
            queue='FilaEntrada', auto_ack=True)
        if body:
            # Verifique o número de mensagens em cada fila de reserva
            fila_menos_ocupada = None
            menor_tamanho = float('inf')
            for i in range(1, NUM_FILAS_RESERVA + 1):
                queue = channel.queue_declare(
                    queue=f'FilaReserva{i}', durable=True, passive=True)
                if queue.method.message_count < menor_tamanho:
                    menor_tamanho = queue.method.message_count
                    fila_menos_ocupada = f'FilaReserva{i}'

            # Enviar a mensagem para a fila menos ocupada
            if fila_menos_ocupada:
                channel.basic_publish(
                    exchange='', routing_key=fila_menos_ocupada, body=body)
                print(f"Mensagem enviada para a {fila_menos_ocupada}: {body}")
        time.sleep(1)


if __name__ == "__main__":
    despachante()
