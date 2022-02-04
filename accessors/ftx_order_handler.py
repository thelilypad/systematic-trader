import collections
from datetime import datetime
import time
from config import Config
from accessors.ftx_web_socket import FtxWebsocketClient
from accessors.wrapped_ftx_client import WrappedFtxClient
import typing
from utils.utils import simple_pluck_dict

EXPECTED_WAIT = 1E-2  # We should poll every one hundredth of a second
EPS = 1E-3


class FtxOrderHandler:
    def __init__(self, api_key: str = None, api_secret: str = None, subaccount: str = None):
        self.rest_client = WrappedFtxClient(api_key=api_key, api_secret=api_secret, subaccount_name=subaccount)
        self.websocket_client = FtxWebsocketClient(api_key=api_key, api_secret=api_secret, subaccount=subaccount,
                                                   orderbook_handler=self.on_orderbook_event,
                                                   fill_handler=self.on_active_fill,
                                                   orders_handler=self.on_handle_order)
        self.min_sizes = {market: minOrderSize for (market, minOrderSize) in [simple_pluck_dict(d, ['name', 'minProvideSize']) for d in self.rest_client.client.get_markets()]}
        self.websocket_client.get_fills()
        self._reset_state()


    def _reset_state(self):
        self.best_mid = None
        self.best_bid = None
        self.best_ask = None
        self._remaining_size = 0
        self._order_size_type = 'QUOTE'
        self._fees_paid = 0
        self.last_fetch_time = 0
        self._fills = []
        self._open_orders = collections.defaultdict(dict)

    def on_handle_order(self, message):
        if not message['channel'] or message['channel'] != 'orders':
            raise Exception('Message malformed')
        data = message['data']
        if data['status'] == 'closed':
            print(f"Closed {data['id']}")
            del self._open_orders[data['market']][data['id']]
        else:
            print(f"Opened {data['id']}")
            self._open_orders[data['market']][data['id']] = data

    def on_active_fill(self, message):
        if not message['channel'] or message['channel'] != 'fills':
            raise Exception('Message malformed')
        data = message['data']
        print(message)
        if self._order_size_type == 'BASE':
            # If we're trying to fill a certain amount of base units, we should find the true notional
            # which is size * price
            self._remaining_size = self._remaining_size - (data['size'] * data['price'])
        else:
            # If we're trying to fill a certain amount of quote units, we should treat it normally
            self._remaining_size = self._remaining_size - data['size']
        self.last_fetch_time = datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
        self._fills.append(data)

    def on_orderbook_event(self, message, orderbook):
        self.best_ask = orderbook['asks'][0][0]
        self.best_bid = orderbook['bids'][0][0]
        self.best_mid = (self.best_bid + self.best_ask) / 2

    def fill_limit_order_in_base(self, market: str,
                                 side: str,
                                 size_in_base: float,
                                 reduce_only: bool = False,
                                 ioc: bool = False,
                                 post_only: bool = False,
                                 client_id: typing.Optional[str] = None):
        self._reset_state()
        self._order_size_type = 'BASE'
        # Enable websocket updates of the market orderbook so we can continually stream in our strategy's best bid/ask
        self.websocket_client.get_orderbook(market)
        self.websocket_client.get_orders()
        # Maintain this order due to asynchronicity - it is possible our order instantly fills even as a limit
        self._remaining_size = size_in_base
        # Hold this thread until we receive the first orderbook update
        while not self.best_mid:
            continue
        self.__warn_if_high_slippage(market, side, self._remaining_size)

        start_order_time = time.time()
        # While we still have remaining size to fill
        print(f"Placing order at {self.best_mid} for {self._remaining_size/self.best_mid}")
        self.rest_client.client.place_order(market, side, self.best_mid, self._remaining_size/self.best_mid, 'limit', reduce_only,
                                            ioc, post_only,
                                            client_id)
        # While we still have remaining size to fill
        while self._remaining_size/self.best_mid > self.min_sizes[market]:
            # Check if 1/100th of a second has elapsed since we last tried to fill best_mid
            if time.time() - self.last_fetch_time < EXPECTED_WAIT:
                # We should pause for 1/100th of a second to check if the order has been filled before progressing
                continue
            if len(self._open_orders[market]):
                if len(self._open_orders[market]) > 1:
                    # pipe out an error
                    print('Multiple orders open at once')
                current_price = self._open_orders[market][list(self._open_orders[market].keys())[0]]['price']
                if current_price == self.best_mid:
                    continue
                else:
                    if self._remaining_size/self.best_mid > self.min_sizes[market]:
                        print(self._remaining_size / self.best_mid, self.min_sizes[market])
                        self.rest_client.cancel_orders(market)
                        # cache the new size post order cancellation, just in case the cancel is filled
                        if self._remaining_size == 0:
                            break
                        while len(self._open_orders[market].keys()):
                            continue
                        print(f"Placing order at {self.best_mid} for {self._remaining_size/self.best_mid}")
                        self.rest_client.client.place_order(market, side, self.best_mid, self._remaining_size/self.best_mid, 'limit',
                                                            reduce_only, ioc, post_only, client_id)
                    else:
                        while len(self._open_orders[market].keys()):
                            continue
        end_time = time.time()
        self.websocket_client.unsubscribe({'channel': 'orderbook', 'market': market})
        return (end_time - start_order_time) * 1000, self._fills, size



    def fill_limit_order_at_best(self, market: str,
                                 side: str,
                                 size: float,
                                 reduce_only: bool = False,
                                 ioc: bool = False,
                                 post_only: bool = False,
                                 client_id: typing.Optional[str] = None):
        self._reset_state()
        self.__warn_if_high_slippage(market, side, size)
        # Enable websocket updates of the market orderbook so we can continually stream in our strategy's best bid/ask
        self.websocket_client.get_orderbook(market)
        self.websocket_client.get_orders()
        # Maintain this order due to asynchronicity - it is possible our order instantly fills even as a limit
        self._remaining_size = size
        # Hold this thread until we receive the first orderbook update
        while not self.best_mid:
            continue

        start_order_time = time.time()
        # Place the initial order. This is a very very deadly simple approach - just put a limit order at best bid
        print(f"Placing order at {self.best_mid} for {self._remaining_size}")
        self.rest_client.client.place_order(market, side, self.best_mid, self._remaining_size, 'limit', reduce_only,
                                            ioc, post_only,
                                            client_id)
        # While we still have remaining size to fill
        while self._remaining_size > EPS:
            # Check if 1/100th of a second has elapsed since we last tried to fill best_mid
            if time.time() - self.last_fetch_time < EXPECTED_WAIT:
                # We should pause for 1/100th of a second to check if the order has been filled before progressing
                continue
            if len(self._open_orders[market]):
                if len(self._open_orders[market]) > 1:
                    # pipe out an error
                    print('Multiple orders open at once')
                current_price = self._open_orders[market][list(self._open_orders[market].keys())[0]]['price']
                if current_price == self.best_mid:
                    continue
                else:
                    self.rest_client.cancel_orders(market)
                    # cache the new size post order cancellation, just in case the cancel is filled
                    if self._remaining_size == 0:
                        break
                    while len(self._open_orders[market].keys()):
                        continue
                    print(f"Placing order at {self.best_mid} for {self._remaining_size}")
                    self.rest_client.client.place_order(market, side, self.best_mid, self._remaining_size, 'limit',
                                                        reduce_only, ioc, post_only, client_id)
        end_time = time.time()
        self.websocket_client.unsubscribe({'channel': 'orderbook', 'market': market})
        return (end_time - start_order_time) * 1000, self._fills, size

    def __warn_if_high_slippage(self, market, side, size):
        try:
            size_with_sign = -size if side == 'sell' else size
            total_fill_avg_price, slippage_ratio, best_price = self.rest_client.project_slippage(market, size_with_sign)
        except:
            print('Slippage clears out best 100 levels on orderbook')

    def market_order(self, market: str,
                     side: str,
                     size: float,
                     reduce_only: bool = False,
                     ioc: bool = False,
                     post_only: bool = False,
                     client_id: typing.Optional[str] = None):
        self.__warn_if_high_slippage(market, side, size)
        return self.rest_client.client.place_order(market=market, side=side, price=None, size=size, type='market',
                                                   reduce_only=reduce_only, ioc=ioc, post_only=post_only,
                                                   client_id=client_id)

    def close(self):
        self.websocket_client.close()


if __name__ == '__main__':
    s = FtxOrderHandler(api_key=Config.get_property('FTX_API_KEY').unwrap(),
                        api_secret=Config.get_property('FTX_API_SECRET').unwrap(),
                        subaccount=Config.get_property('FTX_SUBACCOUNT_NAME').unwrap())
    s.fill_limit_order_in_base('AVAX/USD', 'sell', size_in_base=50)