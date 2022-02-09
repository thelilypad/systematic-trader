

"""
Simple class implementation of a 'watcher' that observes current market conditions, and triggers
certain actions if certain conditions are met.
"""
from pika.exchange_type import ExchangeType

from accessors.wrapped_ftx_client import WrappedFtxClient
from config import Config
from executors.simple_executor import SimpleExecutor
import message_constants as msg

class WatcherExecutor(SimpleExecutor):
    def __init__(self):
        super().__init__(rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", "localhost").unwrap(),
                         exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.fanout,
                         queue=msg.WATCHER_QUEUE)
        self.rest_client = WrappedFtxClient(api_key=Config.get_property("FTX_API_KEY").unwrap(),
            api_secret=Config.get_property("FTX_API_SECRET").unwrap(),
            subaccount_name=Config.get_property("FTX_SUBACCOUNT_NAME").unwrap())

    def poll_market_state(self):
        return self.rest_client.get

if __name__ == '__main__':
    print(WatcherExecutor().poll_market_state())