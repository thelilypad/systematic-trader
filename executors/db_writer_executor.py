import json

from models.order_data import OrderData
from simple_executor import SimpleExecutor
import sys
from pika.exchange_type import ExchangeType

sys.path.append('..')
from accessors.db_accessor import DbAccessor, DbWriteException
from accessors.wrapped_ftx_client import WrappedFtxClient
from config import Config
import message_constants as msg
import typing


class DbWriterExecutor(SimpleExecutor):
    """
    Executor to handle DB writes without blocking the main logic for various
    execution tasks (e.g. position fills)
    """

    def __init__(self, rabbit_mq_host: str = None, api_key: str = None, api_secret: str = None, subaccount: str = None):
        super().__init__(rabbit_mq_host=rabbit_mq_host,
                         exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.fanout,
                         queue=msg.WATCHER_QUEUE)
        self.db_accessor = DbAccessor()
        self.api_key = api_key
        self.api_secret = api_secret
        self.subaccount = subaccount

    def on_message_consumption(self, ch, method, properties, body):
        b = json.loads(body)
        try:
            if b['message_type'] == 'mark_strategy_filled':
                self.mark_strategy_filled(b['position_ids'])
            elif b['message_type'] == 'update_historical_data':
                self.update_historical_data()
            elif b['message_type'] == 'record_fills':
                self.on_record_fills(OrderData(**b['order_data']))
            elif b['message_type'] == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
                self.stop()
        except DbWriteException as dbwe:
            self.on_db_write_error(dbwe)

    def mark_strategy_filled(self, position_ids: typing.List):
        self.db_accessor.mark_strategy_filled(position_ids)

    def update_historical_data(self):
        rest_client = WrappedFtxClient(api_key=self.api_key, api_secret=self.api_secret,
                                       subaccount_name=self.subaccount)
        rest_client.run_update_all_funding()
        rest_client = WrappedFtxClient(api_key=self.api_key, api_secret=self.api_secret,
                                       subaccount_name=self.subaccount)
        rest_client.run_update_all_prices()

    def on_record_fills(self, order_data):
        self.db_accessor.write_successful_fill(order_data)

    def on_db_write_error(self, error: DbWriteException):
        # Implement some handling for db write errors to log and shut down
        # stuff gracefully (?)
        pass


if __name__ == "__main__":
    DbWriterExecutor(rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap(),
                     api_key=Config.get_property("FTX_API_KEY").unwrap(),
                     api_secret=Config.get_property("FTX_API_SECRET").unwrap(),
                     subaccount=Config.get_property("FTX_SUBACCOUNT_NAME").unwrap()).on_record_fills({})
