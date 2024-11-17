import pika
import threading
import time
import json

NUM_FILAS_RESERVA = 2
NUM_FILAS_PROCESSAMENTO = 2


class Ingresso:
    def __init__(self, ingresso_id, evento_id):
        self.ingresso_id = ingresso_id
        self.evento_id = evento_id
        self.usuario_reserva_id = None
        self.usuario_comprador_id = None

    def reservar(self, usuario_id):
        if self.usuario_reserva_id is None:
            self.usuario_reserva_id = usuario_id
            return True
        return False

    def vender(self, usuario_id):
        if self.usuario_reserva_id == usuario_id:
            self.usuario_comprador_id = usuario_id
            return True
        return False


class ServidorIngressos:
    def __init__(self):
        self.contador_ingressos_disponiveis = 1000
        self.contador_ingressos_reservados = 0
        self.contador_lock = threading.Lock()
        self.ingressos_reservados = {}
        self.locks = {}
        self.reservas_paradas = False

    def criar_conexao(self):
        while True:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters('rabbitmq', 5672)
                )
                print("[SERVIDOR] Conexão estabelecida com o RabbitMQ.")
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                print(f"[SERVIDOR] Erro ao conectar com o RabbitMQ: {e}")
                time.sleep(5)

    def get_lock(self, ingresso_id):
        if ingresso_id not in self.locks:
            self.locks[ingresso_id] = threading.Lock()
        return self.locks[ingresso_id]

    def consumir_fila_reserva(self, fila_id):
        connection = self.criar_conexao()
        channel = connection.channel()
        channel.queue_declare(queue=f'FilaReserva{fila_id}', durable=True)

        while True:
            method_frame, header_frame, body = channel.basic_get(
                queue=f'FilaReserva{fila_id}', auto_ack=True)

            if body:
                ingresso_data = json.loads(body.decode())
                usuario_id = ingresso_data.get("usuario_id")
                ingresso_id = ingresso_data.get("ingresso_id")
                evento_id = ingresso_data.get("evento_id")

                print(
                    f"[SERVIDOR] Mensagem recebida na FilaReserva{fila_id}: {ingresso_data}")

                with self.contador_lock:
                    if self.contador_ingressos_disponiveis > 0:
                        lock = self.get_lock(ingresso_id)
                        with lock:
                            if ingresso_id not in self.ingressos_reservados:
                                self.contador_ingressos_disponiveis -= 1
                                self.contador_ingressos_reservados += 1
                                ingresso = Ingresso(ingresso_id, evento_id)
                                if ingresso.reservar(usuario_id):
                                    self.ingressos_reservados[ingresso_id] = ingresso
                                    print(
                                        f"[SERVIDOR] Ingresso {ingresso_id} reservado para o usuário {usuario_id}.")
                                    fila_processamento = self.obter_fila_processamento_menos_ocupada(
                                        channel)
                                    if fila_processamento:
                                        channel.basic_publish(
                                            exchange='', routing_key=fila_processamento, body=json.dumps(ingresso_data)
                                        )
                                        print(
                                            f"[SERVIDOR] Ingresso {ingresso_id} enviado para {fila_processamento}.")
                    else:
                        mensagem = {
                            "status": "Esgotado",
                            "mensagem": "Ingressos esgotados - requisição não processada.",
                            "usuario_comprador_id": usuario_id,
                        }
                        channel.basic_publish(
                            exchange='', routing_key='FilaSaida', body=json.dumps(mensagem)
                        )
                        print(
                            f"[SERVIDOR] Ingressos esgotados. Mensagem enviada para o usuário {usuario_id}.")
            else:
                time.sleep(2)

    def obter_fila_processamento_menos_ocupada(self, channel):
        menor_tamanho = float('inf')
        fila_processamento_menos_ocupada = None
        for i in range(1, NUM_FILAS_PROCESSAMENTO + 1):
            queue = channel.queue_declare(
                queue=f'FilaProcessamento{i}', durable=True, passive=True)
            if queue.method.message_count < menor_tamanho:
                menor_tamanho = queue.method.message_count
                fila_processamento_menos_ocupada = f'FilaProcessamento{i}'
        return fila_processamento_menos_ocupada

    def consumir_fila_processamento(self, fila_id):
        connection = self.criar_conexao()
        channel = connection.channel()
        channel.queue_declare(
            queue=f'FilaProcessamento{fila_id}', durable=True)

        while True:
            method_frame, header_frame, body = channel.basic_get(
                queue=f'FilaProcessamento{fila_id}', auto_ack=True)
            if body:
                ingresso_data = json.loads(body.decode())
                ingresso_id = ingresso_data.get("ingresso_id")
                usuario_id = ingresso_data.get("usuario_id")

                lock = self.get_lock(ingresso_id)
                with lock:
                    ingresso = self.ingressos_reservados.get(ingresso_id)
                    if ingresso and ingresso.vender(usuario_id):
                        with self.contador_lock:
                            self.contador_ingressos_reservados -= 1
                        resposta = {
                            "ingresso_id": ingresso_id,
                            "evento_id": ingresso.evento_id,
                            "usuario_reserva_id": ingresso.usuario_reserva_id,
                            "usuario_comprador_id": ingresso.usuario_comprador_id,
                            "status": "Vendido"
                        }
                        channel.basic_publish(
                            exchange='', routing_key='FilaSaida', body=json.dumps(resposta))
                        print(
                            f"[SERVIDOR] Ingresso {ingresso_id} vendido ao usuário {usuario_id}.")

            time.sleep(3)


if __name__ == "__main__":
    servidor = ServidorIngressos()

    # Threads para filas de reserva
    for i in range(1, NUM_FILAS_RESERVA + 1):
        threading.Thread(
            target=servidor.consumir_fila_reserva, args=(i,)).start()

    # Threads para filas de processamento
    for i in range(1, NUM_FILAS_PROCESSAMENTO + 1):
        threading.Thread(
            target=servidor.consumir_fila_processamento, args=(i,)).start()
