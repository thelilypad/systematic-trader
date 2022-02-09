import json

from pika.exchange_type import ExchangeType

from accessors.db_accessor import DbAccessor, DbWriteException
from config import Config
from executors.simple_executor import SimpleExecutor
import message_constants as msg


class DbWriterExecutor(SimpleExecutor):
    """
    Executor to handle DB writes without blocking the main logic for various
    execution tasks (e.g. position fills)
    """

    def __init__(self):
        super().__init__(rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", "localhost").unwrap(),
                         exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.fanout,
                         queue=msg.WATCHER_QUEUE)
        self.db_accessor = DbAccessor()

    def on_message_consumption(self, ch, method, properties, body):
        b = json.loads(body)
        try:
            if b['message_type'] == 'mark_position_fill':
                self.db_accessor.mark_strategy_filled(b['positions'])
            elif b['message_type'] == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
                self.stop()
        except DbWriteException as dbwe:
            self.on_db_write_error(dbwe)

    def on_db_write_error(self, error: DbWriteException):
        # Implement some handling for db write errors to log and shut down
        # stuff gracefully (?)
        pass
