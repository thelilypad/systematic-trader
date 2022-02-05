import json
import typing
from collections import Counter

import pika
import message_constants as msg
from config import Config
from accessors.db_accessor import DbAccessor
from accessors.ftx_order_handler import FtxOrderHandler

class PositionExecutor:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap())
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=msg.POSITION_EXCHANGE, exchange_type='fanout')
        r = self.channel.queue_declare(queue=msg.POSITION_EXECUTING_QUEUE)
        self.channel.queue_bind(r.method.queue, msg.POSITION_EXCHANGE)
        self.channel.basic_consume(queue=msg.POSITION_EXECUTING_QUEUE,
                              on_message_callback=self.on_message,
                              auto_ack=True)
        self.order_handler = FtxOrderHandler(api_key=Config.get_property('FTX_API_KEY').unwrap(),
                        api_secret=Config.get_property('FTX_API_SECRET').unwrap(),
                        subaccount=Config.get_property('FTX_SUBACCOUNT_NAME').unwrap())

    def on_error(self, error):
        pass

    def net_positions(self, new_positions, old_positions):
        positions = {}
        for pos in old_positions:
            pass
        return []

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

        existing_notional_exposures = {(f"{k}/USD" if 'PERP' not in k else k): v for (k, v) in self.order_handler.rest_client.get_notional_exposures().items()}
        total_account_value = self.order_handler.rest_client.get_net_account_value()
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
        new_positions = DbAccessor().fetch_unfilled_strategies()
        # For netting purposes, let's consider everything that isn't a PERP to be denominated in USDT
        # In the future, it may make sense
        counter = self.__get_notional_netted_weightings(new_positions)
        counter = {'AVAX/USD': -100, 'AVAX-PERP': 50, 'SOL/USD': 50}
        orders = self.__create_optimal_position_execution_ordering(counter)
        for idx, (market, _) in enumerate([('AVAX/USD', -100), ('AVAX-PERP', 50), ('SOL/USD', 50)]):
            value = counter.get(market)
            side = 'buy' if value >= 0 else 'sell'
            self.order_handler.fill_limit_order_in_quote_units(
                market=market, side=side, size_in_quote=abs(value), aggression=min(.99, 0.5 + idx/len(orders))
            )
            counter = self.__get_notional_netted_weightings(new_positions)
            print(counter)
            break

    def on_message(self, ch, method, properties, body):
        b = json.loads(body)
        print(b)

        if b['message_type'] == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
            print('Terminating position executor...')
            self.stop()
        elif b['message_type'] == 'POSITION_RECALCULATE':
            print("Executing position changes...")
            self.execute_position_changes()
        elif b['message_type'] == 'ERROR':
            print("Error occurred in scheduler/executor")
            self.on_error(b)

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.connection.close()

if __name__ == '__main__':
    PositionExecutor().execute_position_changes()
