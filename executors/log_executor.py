import json

import pika
import time
from loguru import logger
from config import Config
import message_constants as msg
from utils.utils import simple_pluck_dict
from datetime import datetime

class LogConsumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap())
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=msg.POSITION_EXCHANGE, exchange_type='fanout')
        r = self.channel.queue_declare(queue=msg.LOG_QUEUE)
        self.channel.queue_bind(r.method.queue, msg.POSITION_EXCHANGE)
        self.channel.basic_consume(queue=msg.LOG_QUEUE,
                              on_message_callback=self.on_message,
                              auto_ack=True)
        logger.add(f"../logs/execution__{int(time.time())}.log", enqueue=True)

    def run(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def stop(self):
        print('Terminating position scheduling process...')
        self.connection.close()

    def on_message(self, ch, method, properties, body):
        b = json.loads(body)
        message_type, message, sender, other_data = simple_pluck_dict(b, ['message_type', 'message', 'sender', 'other_data'])
        msg = f"[{sender}] {message} {other_data if other_data else ''}"
        if message_type == 'INFO':
            logger.info(msg)
        elif message_type == 'WARN':
            logger.warning(msg)
        elif message_type == 'ERROR':
            logger.error(msg)
        elif message_type == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
            self.stop()
        else:
            logger.info(msg)


if __name__ == "__main__":
    LogConsumer().run()