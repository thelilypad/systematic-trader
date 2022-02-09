import collections
from datetime import datetime
import time
from config import Config
from accessors.ftx_web_socket import FtxWebsocketClient
from accessors.wrapped_ftx_client import WrappedFtxClient
import typing
from utils.utils import simple_pluck_dict
import math
from dataclasses import dataclass

EXPECTED_WAIT = 1E-2  # We should poll every one hundredth of a second


def round_to_n(number: float, n):
    return math.floor(number / n) * n

@dataclass
class OrderData:
    def __init__(self, market: str, start_timestamp: float, end_timestamp: float,
                 best_price: float, fill_average_price: float, slippage_ratio: float,
                 fill_data: typing.List, order_quantity: float, quantity_type: str,
                 unfilled_quantity: float, order_type: str):
        self.market = market
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.best_price = best_price
        self.fill_average_price = fill_average_price
        self.slippage_ratio = slippage_ratio
        self.fill_data = fill_data
        self.order_quantity = order_quantity
        self.quantity_type = quantity_type
        self.unfilled_quantity = unfilled_quantity
        self.order_type = order_type


class FtxOrderHandler:
    def __init__(self, api_key: str = None, api_secret: str = None, subaccount: str = None):
        self.rest_client = WrappedFtxClient(api_key=api_key, api_secret=api_secret, subaccount_name=subaccount)
        self.websocket_client = FtxWebsocketClient(api_key=api_key, api_secret=api_secret, subaccount=subaccount,
                                                   orderbook_handler=self.on_orderbook_event,
                                                   fill_handler=self.on_active_fill,
                                                   orders_handler=self.on_handle_order)
        self.min_sizes = {market: minOrderSize for (market, minOrderSize) in
                          [simple_pluck_dict(d, ['name', 'minProvideSize']) for d in
                           self.rest_client.client.get_markets()]}
        self.websocket_client.get_fills()
        self._reset_state()

    def _reset_state(self):
        self.best_mid = None
        self.best_bid = None
        self.best_ask = None
        self.aggression = [0.5, 0.5]
        self._remaining_size = 0
        self._order_size_type = 'BASE'
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
        print(message)

        if not message['channel'] or message['channel'] != 'fills':
            raise Exception('Message malformed')
        data = message['data']
        print(message)
        if self._order_size_type == 'QUOTE':
            # If we're trying to fill a certain amount of quote units, we should find the true notional
            # which is size * price
            self._remaining_size = self._remaining_size - (data['size'] * data['price'])
        else:
            # If we're trying to fill a certain amount of quote units, we should treat it normally
            self._remaining_size = self._remaining_size - data['size']
        # Record the last fetch time an order was filled
        self.last_fetch_time = datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
        self._fills.append(data)

    def on_orderbook_event(self, message, orderbook):
        """
        Event that triggers from websocket orderbook updates.
        We should cache the best_bid and best_ask from the book, and compute the best_mid in order to quote.
        :param message: A websocket message (passthrough from FtxWebSocket)
        :param orderbook: the orderbook state passed through from the web socket
        :return: {None}
        """
        self.best_ask = orderbook['asks'][0][0]
        self.best_bid = orderbook['bids'][0][0]
        self.best_mid = self.best_bid * self.aggression[0] + self.best_ask * self.aggression[1]

    def get_size_in_base(self) -> float:
        """
        Return the total size of the order remaining to fill.
        If we are looking to fill X in quote units (and our _remaining_size represents quote units),
        we need to calculate size by dividing by best_mid (the current mid price on the orderbook).
        Otherwise, if we are looking to fill X in base units, we should assume that _remaining_size represents
        base units (e.g. 0.1 of AVAX) and return that
        :return: the size in base units
        """
        if self._order_size_type == 'QUOTE':
            return self._remaining_size / self.best_mid
        else:
            return self._remaining_size

    def fill_limit_order_in_quote_units(self, market: str,
                                        side: str,
                                        size_in_quote: float,
                                        aggression: float = 0.5,
                                        reduce_only: bool = False,
                                        ioc: bool = False,
                                        post_only: bool = True,
                                        client_id: typing.Optional[str] = None):
        self._reset_state()
        self._order_size_type = 'QUOTE'
        return self.__fill_limit_order_at_best(market, side, size_in_quote, aggression, reduce_only, ioc, post_only,
                                               client_id)

    def fill_limit_order_in_base_units(self, market: str,
                                       side: str,
                                       size_in_base: float,
                                       aggression: float = 0.5,
                                       reduce_only: bool = False,
                                       ioc: bool = False,
                                       post_only: bool = True,
                                       client_id: typing.Optional[str] = None):
        self._reset_state()
        return self.__fill_limit_order_at_best(market, side, size_in_base, aggression, reduce_only, ioc, post_only,
                                               client_id)

    def __fill_limit_order_at_best(self, market: str,
                                   side: str,
                                   size: float,
                                   aggression: float,
                                   reduce_only: bool = False,
                                   ioc: bool = False,
                                   post_only: bool = True,
                                   client_id: typing.Optional[str] = None) -> OrderData:
        print(f'Trying to fill {market} {side} {size}')
        if aggression >= 1 or aggression <= 0:
            raise Exception(f'Aggression coefficient must be between (non-inclusive) 0 and 1, provided {aggression}')
        # Create a skew of how we quote in the spread -- we should use the provided aggression coefficient
        # to create a skew according to side - if aggression is 0.6 and this is a sell, we should return [0.6, 0.4]
        # (skewing to best bid); if this is a buy we should return [0.4, 0.6]
        self.aggression = [1 - aggression, aggression] if side == 'buy' else [aggression, 1 - aggression]
        # Enable websocket updates of the market orderbook so we can continually stream in our strategy's best bid/ask
        self.websocket_client.get_orderbook(market)
        self.websocket_client.get_orders()
        # Maintain this order due to asynchronicity - it is possible our order instantly fills even as a limit
        self._remaining_size = size
        # Hold this thread until we receive the first orderbook update
        while not self.best_mid:
            continue
        # compute original best price by assuming a market order
        original_best_price = self.best_ask if side == 'buy' else self.best_bid
        min_available_size = self.min_sizes[market]

        if self.get_size_in_base() < min_available_size:
            raise Exception(f'Provided size is smaller than min size {min_available_size} supported by FTX')

        self.__warn_if_high_slippage(market, side, self.get_size_in_base())
        start_order_time = time.time()
        # Place the initial order. This is a very very deadly simple approach - just put a limit order at best bid
        print(f"Placing order at {self.best_mid} for {self.get_size_in_base()}")
        self.rest_client.client.place_order(market, side, self.best_mid, self.get_size_in_base(), 'limit', reduce_only,
                                            ioc, post_only,
                                            client_id)
        # While we still have remaining size to fill
        while self.get_size_in_base() > min_available_size:
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
                    if self.get_size_in_base() > min_available_size:
                        self.rest_client.cancel_orders(market)
                        # cache the new size post order cancellation, just in case the cancel is filled
                        if self.get_size_in_base() < min_available_size:
                            break
                        while len(self._open_orders[market].keys()):
                            continue
                        print(f"Placing order at {self.best_mid} for {self.get_size_in_base()}")
                        self.rest_client.client.place_order(market, side, self.best_mid, self.get_size_in_base(),
                                                            'limit',
                                                            reduce_only, ioc, post_only, client_id)
        end_time = time.time()
        self.websocket_client.unsubscribe({'channel': 'orderbook', 'market': market})
        fill_total_price = sum([f['size'] * f['price'] for f in self._fills])
        fill_avg_price = fill_total_price / sum([f['size'] for f in self._fills])
        slippage_ratio = fill_avg_price / original_best_price - 1
        return OrderData(
            market=market,
            start_timestamp=start_order_time * 1000,
            end_timestamp=end_time * 1000,
            best_price=original_best_price,
            fill_average_price=fill_avg_price,
            slippage_ratio=slippage_ratio,
            fill_data=self._fills,
            order_quantity=size,
            quantity_type=self._order_size_type,
            unfilled_quantity=self._remaining_size,
            order_type="LIMIT",
        )

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
    f = FtxOrderHandler(
        api_key=Config.get_property("FTX_API_KEY").unwrap(),
        api_secret=Config.get_property("FTX_API_SECRET").unwrap(),
        subaccount=Config.get_property("FTX_SUBACCOUNT_NAME").unwrap(),
    )
    print(f.fill_limit_order_in_quote_units('AVAX/USD', 'buy', size_in_quote=3000))
