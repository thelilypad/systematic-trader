
"""
Simple class implementation of a 'watcher' that observes current market conditions, and triggers
certain actions if certain conditions are met.
"""
import typing

from pika.exchange_type import ExchangeType

from accessors.ftx_web_socket import FtxWebsocketClient
from accessors.wrapped_ftx_client import WrappedFtxClient
from config import Config
from executors.simple_executor import SimpleExecutor
import message_constants as msg

def as_ticker_subscription(market: str):
    return {'channel': 'ticker', 'market': market}


class WatcherExecutor(SimpleExecutor):
    def __init__(self, rabbit_mq_host: str = None, api_key: str = None, api_secret: str = None, subaccount: str = None):
        super().__init__(rabbit_mq_host=rabbit_mq_host,
                         exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.fanout,
                         queue=msg.WATCHER_QUEUE)
        self.rest_client = WrappedFtxClient(api_key=api_key,
            api_secret=api_secret,
            subaccount_name=subaccount)
        self.websocket_client = FtxWebsocketClient(api_key=api_key,
                                                   api_secret=api_secret,
                                                   subaccount=subaccount,
                                                   ticker_handler=self.on_ticker,
                                                   fill_handler=self.on_active_fill)
        self.active_markets = []
        self.cached_ticker_info =


    def on_ticker(self, message):
        pass

    def on_active_fill(self, _):
        prior_active_markets = self.active_markets.copy()
        self.active_markets = self.__create_markets_from_positions()
        unsubscribe_markets = [as_ticker_subscription(unsub) for unsub in prior_active_markets.difference(self.active_markets)]
        subscribe_markets = self.active_markets.difference(prior_active_markets)
        for unsub in unsubscribe_markets:
            self.websocket_client.unsubscribe(unsub)
        for market in subscribe_markets:
            self.websocket_client.get_ticker(market)
        self.message_helper.log_info_message(
            message='Watcher market subscriptions changed on new fill',
            other_data={'unsubscribes': unsubscribe_markets, 'subscribes': subscribe_markets}
        )

    def __create_markets_from_positions(self) -> typing.Set[str]:
        positions = []
        for position in list(self.rest_client.get_notional_exposures().keys()):
            # We aren't subscribing to the USD market
            if position == 'USD':
                continue
            # If the format of our position is not X-Y or X/Y we can assume it is a spot position
            # and append /USD
            if '-' not in position and '/' not in position:
                positions.append(f"{position}/USD")
            else:
                positions.append(position)
        return set(positions)

    def run(self):
        self.websocket_client.get_fills()
        self.active_markets = self.__create_markets_from_positions()
        for market in self.active_markets:
            self.websocket_client.get_ticker(market)
        super().run()

if __name__ == '__main__':
    WatcherExecutor(
        rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap(),
        api_key=Config.get_property("FTX_API_KEY").unwrap(),
        api_secret=Config.get_property("FTX_API_SECRET").unwrap(),
        subaccount=Config.get_property("FTX_SUBACCOUNT_NAME").unwrap(),
    ).run()
