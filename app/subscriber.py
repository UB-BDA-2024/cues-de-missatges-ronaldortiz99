import pika
import time

class Subscriber:
    def __init__(self):
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('rabbitmq',
                                       5672,
                                       '/',
                                       credentials)
        try:
            self.conn = pika.BlockingConnection(parameters)
        except Exception as e:
            time.sleep(10)
            self.conn = pika.BlockingConnection(parameters)
        self.channel = self.conn.channel()
        ##TODO delcare exchanges
        ##self.channel.exchange_declare(exchange='redis', exchange_type='fanout')
        ##...

    def subscribe(self, exchange, callback):
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def close(self):
        self.conn.close()

    