import pika
import threading
import time
import json

NUM_FILAS_RESERVA = 2  # Defina o número de filas de reserva
NUM_FILAS_PROCESSAMENTO = 2  # Defina o número de filas de processamento


class Ingresso:
    def __init__(self, ingresso_id, evento_id):
        self.ingresso_id = ingresso_id
        self.evento_id = evento_id
        self.usuario_reserva_id = None
        self.usuario_comprador_id = None

    def reservar(self, usuario_id):
        if self.usuario_reserva_id is None:
            self.usuario_reserva_id = usuario_id
            print(
                f"[Servidor - Reserva] Ingresso {self.ingresso_id} reservado pelo usuário {usuario_id}.")
            return True
        print(
            f"[Servidor - Reserva] Ingresso {self.ingresso_id} já foi reservado pelo usuário {self.usuario_reserva_id}.")
        return False

    def vender(self, usuario_id):
        if self.usuario_reserva_id == usuario_id:
            self.usuario_comprador_id = usuario_id
            print(
                f"[Servidor - Venda] Ingresso {self.ingresso_id} comprado pelo usuário {usuario_id}.")
            return True
        print(
            f"[Servidor - Venda] Ingresso {self.ingresso_id} não pode ser vendido ao usuário {usuario_id}.")
        return False


class ServidorIngressos:
    def __init__(self):
        self.contador_ingressos_disponiveis = 500
        self.contador_ingressos_reservados = 0
        self.contador_lock = threading.Lock()
        self.ingressos_reservados = {}
        self.locks = {}

    def criar_conexao(self):
        while True:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters('rabbitmq', 5672))
                print("[Servidor] Conexão estabelecida com o RabbitMQ.")
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                print("[Servidor] Erro ao conectar com o RabbitMQ:", e)
                time.sleep(5)

    def get_lock(self, ingresso_id):
        if ingresso_id not in self.locks:
            self.locks[ingresso_id] = threading.Lock()
        return self.locks[ingresso_id]

    def consumir_fila_reserva(self, fila_id):
        connection = self.criar_conexao()
        channel = connection.channel()
        channel.queue_declare(queue=f'FilaReserva{fila_id}', durable=True)
        thread_id = threading.get_ident()

        while True:
            method_frame, header_frame, body = channel.basic_get(
                queue=f'FilaReserva{fila_id}', auto_ack=True)
            if body:
                ingresso_data = body.decode().split(',')
                ingresso_id, evento_id, usuario_id = map(int, ingresso_data)

                lock = self.get_lock(ingresso_id)
                with lock:
                    if ingresso_id not in self.ingressos_reservados:
                        ingresso = Ingresso(ingresso_id, evento_id)
                        if ingresso.reservar(usuario_id):
                            self.ingressos_reservados[ingresso_id] = ingresso
                            with self.contador_lock:
                                self.contador_ingressos_disponiveis -= 1
                                self.contador_ingressos_reservados += 1
                            print(
                                f"[Servidor - Reserva] FilaReserva{fila_id} - Thread {thread_id}: Ingresso {ingresso_id} reservado e enviado para processamento.")

                fila_processamento_menos_ocupada = None
                menor_tamanho = float('inf')
                for i in range(1, NUM_FILAS_PROCESSAMENTO + 1):
                    queue = channel.queue_declare(
                        queue=f'FilaProcessamento{i}', durable=True, passive=True)
                    if queue.method.message_count < menor_tamanho:
                        menor_tamanho = queue.method.message_count
                        fila_processamento_menos_ocupada = f'FilaProcessamento{i}'

                if fila_processamento_menos_ocupada:
                    channel.basic_publish(
                        exchange='', routing_key=fila_processamento_menos_ocupada, body=body)
                    print(
                        f"[Servidor - Reserva] FilaReserva{fila_id} - Thread {thread_id}: Ingresso {ingresso_id} enviado para {fila_processamento_menos_ocupada}")
            time.sleep(1)

    def consumir_fila_processamento(self, fila_id):
        connection = self.criar_conexao()
        channel = connection.channel()
        channel.queue_declare(
            queue=f'FilaProcessamento{fila_id}', durable=True)
        thread_id = threading.get_ident()

        while True:
            method_frame, header_frame, body = channel.basic_get(
                queue=f'FilaProcessamento{fila_id}', auto_ack=True)
            if body:
                ingresso_data = body.decode().split(',')
                ingresso_id, evento_id, usuario_id = map(int, ingresso_data)

                lock = self.get_lock(ingresso_id)
                with lock:
                    ingresso = self.ingressos_reservados.get(ingresso_id)
                    if ingresso and ingresso.vender(usuario_id):
                        with self.contador_lock:
                            self.contador_ingressos_reservados -= 1
                            if self.contador_ingressos_reservados == 0 and self.contador_ingressos_disponiveis == 0:
                                print(
                                    "[Servidor - Processamento] Todos os ingressos foram vendidos e finalizados.")

                        resposta = {
                            "ingresso_id": ingresso_id,
                            "evento_id": evento_id,
                            "usuario_reserva_id": ingresso.usuario_reserva_id,
                            "usuario_comprador_id": ingresso.usuario_comprador_id,
                            "status": "vendido"
                        }
                        print(
                            f"[Servidor - Processamento] FilaProcessamento{fila_id} - Thread {thread_id}: Ingresso {ingresso_id} vendido e finalizado.")
                        channel.basic_publish(
                            exchange='', routing_key='FilaSaida', body=json.dumps(resposta))
                    else:
                        print(
                            f"[Servidor - Processamento] FilaProcessamento{fila_id} - Thread {thread_id}: Ingresso {ingresso_id} não pode ser vendido ao usuário {usuario_id}.")
            time.sleep(1)


if __name__ == "__main__":
    servidor = ServidorIngressos()

    # Inicia as threads de reserva com filas dedicadas
    for i in range(1, NUM_FILAS_RESERVA + 1):
        threading.Thread(
            target=servidor.consumir_fila_reserva, args=(i,)).start()

    # Inicia as threads de processamento com filas dedicadas
    for i in range(1, NUM_FILAS_PROCESSAMENTO + 1):
        threading.Thread(
            target=servidor.consumir_fila_processamento, args=(i,)).start()
