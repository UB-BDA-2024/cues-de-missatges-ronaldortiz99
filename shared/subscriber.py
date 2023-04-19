import pika
import time

from shared.publisher import QUEUE_NAME

class Subscriber:
    def __init__(self):
        credentials = pika.PlainCredentials('guest', 'guest')
        # Change the host to rabbitmq
        parameters = pika.ConnectionParameters('localhost',
                                       5672,
                                       '/',
                                       credentials)
        try:
            self.conn = pika.BlockingConnection(parameters)
        except Exception as e:
            time.sleep(10)
            self.conn = pika.BlockingConnection(parameters)
        self.channel = self.conn.channel()


    def subscribe(self, callback):
        result = self.channel.queue_declare(queue=QUEUE_NAME)
        self.channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def close(self):
        self.conn.close()

    