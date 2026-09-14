import pika

from .middleware import MessageMiddlewareExchange, MessageMiddlewareQueue


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        pass
