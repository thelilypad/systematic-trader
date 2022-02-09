import json

import time
from loguru import logger
from pika.exchange_type import ExchangeType

from config import Config
import message_constants as msg
from executors.simple_executor import SimpleExecutor
from utils.utils import simple_pluck_dict

class LogConsumer(SimpleExecutor):
    def __init__(self):
        super().__init__(rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap(),
                         exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.fanout,
                         queue=msg.LOG_QUEUE)
        logger.add(f"../logs/execution__{int(time.time())}.log", enqueue=True)

    def on_message_consumption(self, ch, method, properties, body):
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