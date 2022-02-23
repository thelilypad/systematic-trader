import abc
import pika

from executors.message_helper import MessageHelper


class SimpleExecutor:
    """
    Simple implementation of a RabbitMQ pubsub process which can be ran independently, listening and receiving messages.
    """
    def __init__(self, rabbit_mq_host: str, exchange: str, exchange_type: str, queue: str):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_mq_host)
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange, exchange_type=exchange_type)
        r = self.channel.queue_declare(queue=queue)
        self.channel.queue_bind(r.method.queue, exchange=exchange)
        self.channel.basic_consume(queue=queue,
                                   on_message_callback=self.on_message_consumption,
                                   auto_ack=True)
        # This looks like magic, but it should properly reflect the name of the superclass extending this
        self.message_helper = MessageHelper(self.channel, self.__class__.__name__)
        print(' [*] Waiting for messages. To exit press CTRL+C')

    @abc.abstractmethod
    def on_message_consumption(self, ch, method, properties, body):
        pass

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.connection.close()