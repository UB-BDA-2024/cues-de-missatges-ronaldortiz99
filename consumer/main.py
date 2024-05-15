import json

from shared.subscriber import Subscriber

subscriber = Subscriber()


def callback(ch, method, properties, body):
    data = json.loads(body)
    print("Received data:", data)


subscriber.subscribe(callback)
