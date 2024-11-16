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
        self.contador_ingressos_disponiveis = 50
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
            with self.contador_lock:
                if self.contador_ingressos_disponiveis <= 0:
                    print(
                        f"[SERVIDOR] Todos os ingressos foram reservados ou esgotados.")
                    self.reservas_paradas = True
                    break

            method_frame, header_frame, body = channel.basic_get(
                queue=f'FilaReserva{fila_id}', auto_ack=True)
            if body:
                ingresso_data = json.loads(body.decode())
                ingresso_id = ingresso_data.get("ingresso_id")
                evento_id = ingresso_data.get("evento_id")
                usuario_id = ingresso_data.get("usuario_id")

                lock = self.get_lock(ingresso_id)
                with lock:
                    if ingresso_id not in self.ingressos_reservados:
                        with self.contador_lock:
                            if self.contador_ingressos_disponiveis > 0:
                                self.contador_ingressos_disponiveis -= 1
                                self.contador_ingressos_reservados += 1
                                ingresso = Ingresso(ingresso_id, evento_id)
                                if ingresso.reservar(usuario_id):
                                    self.ingressos_reservados[ingresso_id] = ingresso
                                    print(
                                        f"[SERVIDOR] Ingresso {ingresso_id} sendo reservado na FilaReserva{fila_id}.")
                                    fila_processamento = self.obter_fila_processamento_menos_ocupada(
                                        channel)
                                    if fila_processamento:
                                        channel.basic_publish(
                                            exchange='', routing_key=fila_processamento, body=json.dumps(ingresso_data)
                                        )
                                        print(
                                            f"[SERVIDOR] Ingresso {ingresso_id} enviado para {fila_processamento}.")
            time.sleep(2)

        self.verificar_ingressos(channel, fila_id)

    def verificar_ingressos(self, channel, fila_id):
        while True:
            with self.contador_lock:
                if self.contador_ingressos_disponiveis == 0 and self.contador_ingressos_reservados == 0:
                    self.enviar_mensagem_esgotado(channel)
                    self.mover_requisicoes_pendentes_para_fila_saida(
                        channel, fila_id)
                    break
            print(
                f"[SERVIDOR] FilaReserva{fila_id} aguardando disponibilidade de ingressos.")
            time.sleep(5)

    def enviar_mensagem_esgotado(self, channel):
        mensagem = {"status": "Esgotado",
                    "mensagem": "Todos os ingressos foram vendidos."}
        channel.basic_publish(
            exchange='', routing_key='FilaSaida', body=json.dumps(mensagem))
        print("[SERVIDOR] Mensagem de esgotamento enviada para FilaSaida.")

    def mover_requisicoes_pendentes_para_fila_saida(self, channel, fila_id):
        print(
            f"[SERVIDOR] Movendo requisições pendentes de FilaReserva{fila_id} para FilaSaida.")
        while True:
            method_frame, header_frame, body = channel.basic_get(
                queue=f'FilaReserva{fila_id}', auto_ack=True)
            if not body:
                break
            mensagem = {
                "status": "Esgotado",
                "mensagem": "Ingressos esgotados - requisição não processada."
            }
            channel.basic_publish(
                exchange='', routing_key='FilaSaida', body=json.dumps(mensagem))
            print(f"[SERVIDOR] Requisição movida para FilaSaida: {mensagem}.")

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
                            "status": "vendido"
                        }
                        channel.basic_publish(
                            exchange='', routing_key='FilaSaida', body=json.dumps(resposta))
                        print(
                            f"[SERVIDOR] Ingresso {ingresso_id} sendo processado na FilaProcessamento{fila_id}.")
            time.sleep(3)


if __name__ == "__main__":
    servidor = ServidorIngressos()

    for i in range(1, NUM_FILAS_RESERVA + 1):
        threading.Thread(
            target=servidor.consumir_fila_reserva, args=(i,)).start()

    for i in range(1, NUM_FILAS_PROCESSAMENTO + 1):
        threading.Thread(
            target=servidor.consumir_fila_processamento, args=(i,)).start()
