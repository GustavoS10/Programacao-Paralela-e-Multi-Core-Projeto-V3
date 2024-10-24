import pika
from flask import Flask, request, jsonify
import time

app = Flask(__name__)

# Conexão com RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

# Declaração das filas
# channel.queue_declare(queue='FilaEntrada', durable=True)
# channel.queue_declare(queue='FilaSaida', durable=True)

@app.route('/compra', methods=['POST'])
def compra():
    data = request.get_json()
    id_usuario = data.get('id_usuario')
    channel.basic_publish(exchange='', routing_key='FilaEntrada', body=str(id_usuario))
    return jsonify({"status": "Requisição enviada", "id_usuario": id_usuario})

@app.route('/resultado/<int:id_usuario>', methods=['GET'])
def resultado(id_usuario):
    method_frame, header_frame, body = channel.basic_get(queue='FilaSaida', auto_ack=True)
    if body:
        return jsonify({"id_usuario": id_usuario, "id_ingresso": body.decode()})
    return jsonify({"status": "Aguardando processamento"})

# Função para tentar a conexão com RabbitMQ com retries
def connect_rabbitmq():
    for _ in range(5):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            print("Aguardando RabbitMQ ficar disponível...")
            time.sleep(5)
    raise Exception("Não foi possível conectar ao RabbitMQ.")

if __name__ == "__main__":
    connection = connect_rabbitmq()
    channel = connection.channel()
    # channel.queue_declare(queue='FilaEntrada')
    app.run(host='0.0.0.0', port=5000)
