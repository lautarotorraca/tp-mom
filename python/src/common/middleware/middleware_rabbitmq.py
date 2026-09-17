import pika

from .middleware import MessageMiddlewareExchange, MessageMiddlewareQueue


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.consuming = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def send(self, message):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )

    def start_consuming(self, on_message_callback):
        def callback(channel, method, properties, body):
            tag = method.delivery_tag
            ack = lambda: channel.basic_ack(delivery_tag=tag)
            nack = lambda: channel.basic_nack(delivery_tag=tag, requeue=True)
            on_message_callback(body, ack, nack)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=False,
        )
        self.consuming = True
        try:
            self.channel.start_consuming()
        finally:
            self.consuming = False

    def stop_consuming(self):
        if self.consuming:
            self.channel.stop_consuming()

    def close(self):
        self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        self.connection.close()
