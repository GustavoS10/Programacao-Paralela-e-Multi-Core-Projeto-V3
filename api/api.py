import pika
import json
from flask import Flask, request, jsonify
import time
from collections import defaultdict
import threading

app = Flask(__name__)


def connect_rabbitmq():
    for _ in range(5):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('rabbitmq')
            )
            print("[API] Conexão estabelecida com o RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("[API] Aguardando RabbitMQ ficar disponível...")
            time.sleep(5)
    raise Exception("[API] Não foi possível conectar ao RabbitMQ.")


connection = connect_rabbitmq()
channel = connection.channel()
channel.queue_declare(queue='FilaEntrada', durable=True)
channel.queue_declare(queue='FilaSaida', durable=True)


@app.route('/compra', methods=['POST'])
def compra():
    """Recebe dados de compra e publica na fila de entrada."""
    data = request.get_json()
    usuario_id = data.get('usuario_id')
    ingresso_id = data.get('ingresso_id')
    evento_id = data.get('evento_id')

    if usuario_id is not None and ingresso_id is not None and evento_id is not None:
        mensagem = json.dumps({
            "usuario_id": usuario_id,
            "ingresso_id": ingresso_id,
            "evento_id": evento_id
        })
        channel.basic_publish(
            exchange='', routing_key='FilaEntrada', body=mensagem
        )
        print(f"[API] Mensagem enviada para a fila: {mensagem}")
        return jsonify({"status": "Requisição enviada", "mensagem": mensagem})
    else:
        print("[API] Dados incompletos na requisição.")
        return jsonify({"erro": "Dados incompletos na requisição"}), 400


mensagens_cache = defaultdict(list)
mensagens_lock = threading.Lock()


@app.route('/resultado/<int:usuario_id>', methods=['GET'])
def resultado(usuario_id):
    """Consulta mensagens na fila de saída para um usuário específico."""
    print(
        f"[API] Requisição recebida no endpoint /resultado para usuario_id: {usuario_id}")

    with mensagens_lock:
        # Primeiro, verifica se já temos a mensagem no cache
        if mensagens_cache[usuario_id]:
            mensagem = mensagens_cache[usuario_id].pop(0)
            print(
                f"[API] Mensagem encontrada no cache para o usuário {usuario_id}: {mensagem}")
            return jsonify(mensagem)

    # Caso não haja mensagem no cache, verifica na fila
    method_frame, header_frame, body = channel.basic_get(
        queue='FilaSaida', auto_ack=False)

    if body:
        try:
            print(f"[API] Mensagem recebida da fila: {body.decode()}")
            mensagem = json.loads(body.decode())

            if mensagem.get("usuario_comprador_id") == usuario_id:
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                print(
                    f"[API] Mensagem encontrada para o usuário {usuario_id}: {mensagem}")
                return jsonify(mensagem)
            else:
                # Armazena no cache para outros usuários
                with mensagens_lock:
                    mensagens_cache[mensagem["usuario_comprador_id"]].append(
                        mensagem)

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                print(
                    f"[API] Mensagem adicionada ao cache para outro usuário: {mensagem}")

        except json.JSONDecodeError:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            print("[API] Mensagem corrompida encontrada na fila. Ignorando.")
            return jsonify({"erro": "Mensagem corrompida na fila"}), 500

    print(f"[API] Nenhuma mensagem encontrada para o usuário {usuario_id}.")
    return jsonify({"status": "Aguardando processamento"})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
