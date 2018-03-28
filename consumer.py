from flask import Flask, Response
from kafka import KafkaConsumer

# connect to Kafka server and pass the topic we want to consume
consumer = KafkaConsumer('my-topic', group_id='view', bootstrap_servers=['192.168.2.102:9092', '192.168.2.102:9093', '192.168.2.102:9094'])

# Continuously listen to the connection and print messages as recieved
app = Flask(__name__)


@app.route('/')
def index():
    # return a multipart response
    return Response(kafkastream(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


def kafkastream():
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + msg.value + b'\r\n\r\n')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
