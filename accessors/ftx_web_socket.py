import json
import time
import typing
from threading import Thread, Lock
from websocket import WebSocketApp
import hmac
import zlib
from collections import defaultdict, deque
from itertools import zip_longest
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional
from gevent.event import Event

from config import Config
from accessors.wrapped_ftx_client import WrappedFtxClient

"""
Simple implementation of a generic WebSocket manager in Python.
Implementation copied from https://github.com/ftexchange/ftx/blob/master/websocket/websocket_manager.py
"""
class WebsocketManager:
    _CONNECT_TIMEOUT_S = 5

    def __init__(self):
        self.connect_lock = Lock()
        self.ws = None
        self.permanent_stop = False

    def _get_url(self):
        raise NotImplementedError()

    def _on_message(self, ws, message):
        raise NotImplementedError()

    def send(self, message):
        self.connect()
        self.ws.send(message)

    def send_json(self, message):
        self.send(json.dumps(message))

    def _connect(self):
        assert not self.ws, "ws should be closed before attempting to connect"
        self.permanent_stop = False
        self.ws = WebSocketApp(
            self._get_url(),
            on_message=self._wrap_callback(self._on_message),
            on_close=self._wrap_callback(self._on_close),
            on_error=self._wrap_callback(self._on_error),
        )

        wst = Thread(target=self._run_websocket, args=(self.ws,))
        wst.daemon = True
        wst.start()
        print('daemon has started')

        # Wait for socket to connect
        ts = time.time()
        while self.ws and (not self.ws.sock or not self.ws.sock.connected):
            if time.time() - ts > self._CONNECT_TIMEOUT_S:
                self.ws = None
                return
            time.sleep(0.1)

    def _wrap_callback(self, f):
        def wrapped_f(ws, *args, **kwargs):
            if ws is self.ws:
                try:
                    f(ws, *args, **kwargs)
                except Exception as e:
                    raise Exception(f'Error running websocket callback: {e}')
        return wrapped_f

    def _run_websocket(self, ws):
        try:
            ws.run_forever()
        except Exception as e:
            raise Exception(f'Unexpected error while running websocket: {e}')
        finally:
            self._reconnect(ws)

    def on_reconnect(self):
        pass

    def _reconnect(self, ws):
        assert ws is not None, '_reconnect should only be called with an existing ws'
        if ws is self.ws and not self.permanent_stop:
            self.ws = None
            ws.close()
            self.connect()
            self.on_reconnect()

    def connect(self):
        if self.ws:
            return
        with self.connect_lock:
            while not self.ws:
                self._connect()
                if self.ws:
                    return

    def close(self):
        self.permanent_stop = True
        if self.ws:
            self.ws.close()

    def _on_close(self, ws, foo, bar):
        self._reconnect(ws)

    def _on_error(self, ws, error):
        print(error)
        print('Websocket error has occurred')
        self._reconnect(ws)

    def reconnect(self) -> None:
        if self.ws is not None:
            self._reconnect(self.ws)

# Simple no-op method
NOOP = lambda *a, **k: None

"""
Simple implementation of the FTX websocket client, largely copied from https://github.com/ftexchange/ftx/blob/master/websocket/client.py
with some slight modifications:
1) You can pass in handlers for each type of websocket message, which passes the message (except for orderbook handler) directly to the method
2) For orderbook messages, we first compute the partial messaging (using the existing FTX client code above) and only send it to our handler
if the message is not malformed
"""
class FtxWebsocketClient(WebsocketManager):
    _ENDPOINT = 'wss://ftx.com/ws/'

    def __init__(self, api_key=None, api_secret=None, subaccount=None,
                 trade_handler: typing.Callable=NOOP,
                 orderbook_handler: typing.Callable=NOOP,
                 ticker_handler: typing.Callable=NOOP,
                 fill_handler: typing.Callable=NOOP,
                 orders_handler: typing.Callable=NOOP,
                 ) -> None:
        super().__init__()
        self._trades: DefaultDict[str, Deque] = defaultdict(lambda: deque([], maxlen=10000))
        self._fills: Deque = deque([], maxlen=10000)
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount = subaccount
        self.channel_event_handlers = {
            'trades': trade_handler,
            'orderbook': orderbook_handler,
            'ticker': ticker_handler,
            'fills': fill_handler,
            'orders': orders_handler,
        }
        self._orderbook_update_events: DefaultDict[str, Event] = defaultdict(Event)
        self._reset_data()

    def _on_open(self, ws):
        self._reset_data()

    def _reset_data(self) -> None:
        self._subscriptions: List[Dict] = []
        self._orders: DefaultDict[int, Dict] = defaultdict(dict)
        self._tickers: DefaultDict[str, Dict] = defaultdict(dict)
        self._orderbook_timestamps: DefaultDict[str, float] = defaultdict(float)
        self._orderbook_update_events.clear()
        self._orderbooks: DefaultDict[str, Dict[str, DefaultDict[float, float]]] = defaultdict(
            lambda: {side: defaultdict(float) for side in {'bids', 'asks'}})
        self._orderbook_timestamps.clear()
        self._logged_in = False
        self._last_received_orderbook_data_at: float = 0.0

    def _reset_orderbook(self, market: str) -> None:
        if market in self._orderbooks:
            del self._orderbooks[market]
        if market in self._orderbook_timestamps:
            del self._orderbook_timestamps[market]

    def _get_url(self) -> str:
        return self._ENDPOINT

    def _login(self) -> None:
        ts = int(time.time() * 1000)
        args = {
            'key': self._api_key,
            'sign': hmac.new(
                self._api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(),
            'time': ts,
        }
        if self._subaccount:
            args['subaccount'] = self._subaccount
        self.send_json({'op': 'login', 'args': args})
        self._logged_in = True

    def _subscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'subscribe', **subscription})
        self._subscriptions.append(subscription)

    def unsubscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'unsubscribe', **subscription})
        while subscription in self._subscriptions:
            self._subscriptions.remove(subscription)

    def on_reconnect(self):
        self._login()
        for subscription in self._subscriptions:
            self.send_json({'op': 'subscribe', **subscription})

    def get_fills(self) -> List[Dict]:
        if not self._logged_in:
            self._login()
        subscription = {'channel': 'fills'}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return list(self._fills.copy())

    def get_orders(self) -> Dict[int, Dict]:
        if not self._logged_in:
            self._login()
        subscription = {'channel': 'orders'}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return dict(self._orders.copy())

    def get_trades(self, market: str) -> List[Dict]:
        subscription = {'channel': 'trades', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return list(self._trades[market].copy())

    def get_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        subscription = {'channel': 'orderbook', 'market': market}
        if subscription not in self._subscriptions:
            time.sleep(2)
            self._subscribe(subscription)
        if self._orderbook_timestamps[market] == 0:
            self.wait_for_orderbook_update(market, 5)
        return {
            side: sorted(
                [(price, quantity) for price, quantity in list(self._orderbooks[market][side].items())
                 if quantity],
                key=lambda order: order[0] * (-1 if side == 'bids' else 1)
            )
            for side in {'bids', 'asks'}
        }


    def get_orderbook_timestamp(self, market: str) -> float:
        return self._orderbook_timestamps[market]

    def wait_for_orderbook_update(self, market: str, timeout: Optional[float]) -> None:
        subscription = {'channel': 'orderbook', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        self._orderbook_update_events[market].wait(timeout)

    def get_ticker(self, market: str) -> Dict:
        subscription = {'channel': 'ticker', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return self._tickers[market]

    def on_ticker_update(self, tickers):
        c = []
        for t in tickers.keys():
            if not len(tickers[t]):
                c.append(t)
        print(c)

    def _handle_orderbook_message(self, message: Dict) -> None:
        market = message['market']
        subscription = {'channel': 'orderbook', 'market': market}
        if subscription not in self._subscriptions:
            return
        data = message['data']
        if data['action'] == 'partial':
            self._reset_orderbook(market)
        for side in {'bids', 'asks'}:
            book = self._orderbooks[market][side]
            for price, size in data[side]:
                if size:
                    book[price] = size
                else:
                    del book[price]
            self._orderbook_timestamps[market] = data['time']
        checksum = data['checksum']
        orderbook = self.get_orderbook(market)
        checksum_data = [
            ':'.join([f'{float(order[0])}:{float(order[1])}' for order in (bid, offer) if order])
            for (bid, offer) in zip_longest(orderbook['bids'][:100], orderbook['asks'][:100])
        ]
        computed_result = int(zlib.crc32(':'.join(checksum_data).encode()))
        if computed_result != checksum:
            self._last_received_orderbook_data_at = 0
            self._reset_orderbook(market)
            self.unsubscribe({'market': market, 'channel': 'orderbook'})
            self._subscribe({'market': market, 'channel': 'orderbook'})
        else:
            self.channel_event_handlers['orderbook'](message, orderbook)
            self._orderbook_update_events[market].set()
            self._orderbook_update_events[market].clear()

    def _handle_trades_message(self, message: Dict) -> None:
        self._trades[message['market']].append(message['data'])
        self.channel_event_handlers['trades'](message)

    def _handle_ticker_message(self, message: Dict) -> None:
        self._tickers[message['market']] = message['data']
        self.on_ticker_update(self._tickers)
        self.channel_event_handlers['ticker'](message)

    def _handle_fills_message(self, message: Dict) -> None:
        self._fills.append(message['data'])
        self.channel_event_handlers['fills'](message)

    def _handle_orders_message(self, message: Dict) -> None:
        data = message['data']
        self._orders.update({data['id']: data})
        self.channel_event_handlers['orders'](message)

    def _on_message(self, ws, raw_message: str) -> None:
        message = json.loads(raw_message)
        message_type = message['type']
        if message_type in {'subscribed', 'unsubscribed'}:
            return
        elif message_type == 'info':
            if message['code'] == 20001:
                return self.reconnect()
        elif message_type == 'error':
            raise Exception(message)
        channel = message['channel']
        if channel == 'orderbook':
            self._handle_orderbook_message(message)
        elif channel == 'trades':
            self._handle_trades_message(message)
        elif channel == 'ticker':
            self._handle_ticker_message(message)
        elif channel == 'fills':
            self._handle_fills_message(message)
        elif channel == 'orders':
            self._handle_orders_message(message)

if __name__ == '__main__':
    availables = WrappedFtxClient().get_available_tickers()
    client = FtxWebsocketClient(api_key=Config.get_property('FTX_API_KEY').unwrap(), api_secret=Config.get_property('FTX_API_SECRET').unwrap())
    client.get_orderbook('BTC/USDT')