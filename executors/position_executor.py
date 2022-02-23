import json
from collections import Counter

from pika.exchange_type import ExchangeType

import message_constants as msg
from accessors.wrapped_ftx_client import WrappedFtxClient
from config import Config
from accessors.db_accessor import DbAccessor
from accessors.ftx_order_handler import FtxOrderHandler
from executors.simple_executor import SimpleExecutor
from models.position import Position
import dataclasses

class PositionExecutor(SimpleExecutor):
    def __init__(self, rabbit_mq_host: str = None, api_key: str = None, api_secret: str = None, subaccount: str = None):
        super().__init__(rabbit_mq_host=rabbit_mq_host,
                         queue=msg.POSITION_EXECUTING_QUEUE, exchange=msg.POSITION_EXCHANGE, exchange_type=ExchangeType.fanout)
        self.rest_client = WrappedFtxClient(api_key=api_key, api_secret=api_secret, subaccount_name=subaccount)
        self.db_accessor = DbAccessor()
        self._api_key = api_key
        self._api_secret = api_secret
        self._subacount = subaccount

    def on_error(self, error):
        pass

    def __create_optimal_position_execution_ordering(self, counter: Counter):
        sales = {}
        buys = {}
        for k, v in counter.items():
            if v < 0:
                sales[k] = v
            elif v > 0:
                buys[k] = v
        ordered_sales = sorted([(k, v) for (k, v) in sales.items()], key=lambda x: x[1])
        ordered_buys = sorted([(k, v) for (k, v) in buys.items()], key=lambda x: x[1], reverse=True)
        # For a given list of assets:
        pointer_sales = 0
        pointer_buys = 0
        orders = []
        while len(orders) <= len(ordered_sales) + len(ordered_buys):
            running_sum = sum([o[1] for o in orders])
            if pointer_sales == len(ordered_sales):
                orders += ordered_buys[pointer_buys:]
                break
            if pointer_buys == len(ordered_buys):
                orders += ordered_sales[pointer_sales:]
                break
            if abs(running_sum) >= ordered_buys[pointer_buys][1]:
                orders += [ordered_buys[pointer_buys]]
                pointer_buys += 1
            else:
                orders += [ordered_sales[pointer_sales]]
                pointer_sales += 1
        return orders

    def __get_notional_netted_weightings(self, new_positions):
        def create_ftx_symbol_name(position):
            if position.product_type == 'PERP':
                return f"{position.base}-PERP"
            else:
                return f"{position.base}/{position.quote}"

        existing_notional_exposures = {(f"{k}/USD" if 'PERP' not in k else k): v for (k, v) in
                                       self.rest_client.get_notional_exposures().items()}
        total_account_value = self.rest_client.get_net_account_value()
        # New positions represent relative weightings of the total account value
        # We should calculate the net position changes based on the existing positions vs. our optimal weightings
        notional_weightings = {}
        for position in new_positions:
            symbol = create_ftx_symbol_name(position)
            notional_weightings[symbol] = total_account_value * position.relative_size
        counter = Counter(notional_weightings)
        counter.subtract(Counter(existing_notional_exposures))
        return counter

    def execute_position_changes(self):
        new_positions = self.db_accessor.fetch_unfilled_strategies()
        if not new_positions:
            raise Exception('No positions found')
        # For netting purposes, let's consider everything that isn't a PERP to be denominated in USDT
        # In the future, it may make sense
        counter = self.__get_notional_netted_weightings(new_positions)
        orders = self.__create_optimal_position_execution_ordering(counter)
        for idx, (market, _) in enumerate(orders):
            # Ignore this malformed type
            if market == 'USD/USD':
                continue
            value = counter.get(market)
            side = 'buy' if value >= 0 else 'sell'
            try:
                order_handler = FtxOrderHandler(api_key=self._api_key,
                                                api_secret=self._api_secret,
                                                subaccount=self._subacount)

                order_data = order_handler.fill_limit_order_in_quote_units(
                    market=market, side=side, size_in_quote=abs(value), aggression=0.5
                )
                order_handler.close()
                self.message_helper.db_write_fill_order_data(order_data=dataclasses.asdict(order_data))
            except Exception as e:
                print(f'Exception occurred {e}')
                self.message_helper.log_error_message(exception=str(e), other_data={"market": market, "exchange": "FTX"})
                continue
            self.message_helper.log_info_message(message=f"Successful fill - {market} @ {abs(value)} [{side}]", other_data={"market": market, "exchange": "FTX"})
            counter = self.__get_notional_netted_weightings(new_positions)
        self.message_helper.db_write_strategy_filled(position_ids=[pos.id for pos in new_positions])

    def on_message_consumption(self, ch, method, properties, body):
        b = json.loads(body)
        if b['message_type'] == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
            print('Terminating position executor...')
            self.stop()
        elif b['message_type'] == 'POSITION_RECALCULATE':
            print("Executing position changes...")
            self.execute_position_changes()
        elif b['message_type'] == 'ERROR':
            print("Error occurred in scheduler/executor")
            self.on_error(b)


if __name__ == '__main__':
    pos1 = [Position(
        id=1,
        strategy='alpha1',
        group='',
        quote='USD',
        base='AVAX',
        exchange='FTX',
        product_type='SPOT',
        relative_size=0.81,
    ),
    Position(
        id=1,
        strategy='alpha1',
        group='',
        quote='USD',
        base='AVAX',
        exchange='FTX',
        product_type='PERP',
        relative_size=0.15,
    )
    ]
    DbAccessor().write_new_strategy_positions(pos1)
    p = PositionExecutor(
        rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap(),
        api_key=Config.get_property("FTX_API_KEY").unwrap(),
        api_secret=Config.get_property("FTX_API_SECRET").unwrap(),
        subaccount=Config.get_property("FTX_SUBACCOUNT_NAME").unwrap(),
    )
    p.execute_position_changes()