import pika
import time

class Publisher:
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
    
    def publish(self, exchange, message):
        self.channel.basic_publish(exchange=exchange, routing_key='', body=message)
        print(" [x] Sent %r" % message)
    
    def close(self):
        self.conn.close()
