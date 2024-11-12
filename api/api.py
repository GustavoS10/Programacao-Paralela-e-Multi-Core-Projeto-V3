import pika
from flask import Flask, request, jsonify
import time

app = Flask(__name__)

# Função para conectar ao RabbitMQ


def connect_rabbitmq():
    for _ in range(5):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('rabbitmq'))
            print("Conexão estabelecida com o RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            print("Aguardando RabbitMQ ficar disponível...")
            time.sleep(5)
    raise Exception("Não foi possível conectar ao RabbitMQ.")


# Conexão e canal do RabbitMQ
connection = connect_rabbitmq()
channel = connection.channel()
channel.queue_declare(queue='FilaEntrada', durable=True)
channel.queue_declare(queue='FilaSaida', durable=True)


@app.route('/compra', methods=['POST'])
def compra():
    data = request.get_json()
    usuario_id = data.get('usuario_id')
    ingresso_id = data.get('ingresso_id')
    evento_id = data.get('evento_id')

    # Verifique se todos os campos necessários estão presentes
    if usuario_id is not None and ingresso_id is not None and evento_id is not None:
        # Formate a mensagem corretamente com os três valores separados por vírgula
        mensagem = f"{ingresso_id},{evento_id},{usuario_id}"
        channel.basic_publish(
            exchange='', routing_key='FilaEntrada', body=mensagem)
        print(f"[API] Mensagem enviada para a fila: {mensagem}")
        return jsonify({"status": "Requisição enviada", "mensagem": mensagem})
    else:
        return jsonify({"erro": "Dados incompletos na requisição"}), 400


@app.route('/resultado/<int:usuario_id>', methods=['GET'])
def resultado(usuario_id):
    method_frame, header_frame, body = channel.basic_get(
        queue='FilaSaida', auto_ack=True)
    if body:
        return jsonify({"usuario_id": usuario_id, "id_ingresso": body.decode()})
    return jsonify({"status": "Aguardando processamento"})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
