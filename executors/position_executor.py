import json
import typing
from collections import Counter

import pandas as pd
from datetime import datetime
import pika, sys, os
import message_constants as msg
from config import Config
from accessors.db_accessor import DbAccessor
from models.position import Position
from accessors.ftx_order_handler import FtxOrderHandler
from accessors.wrapped_ftx_client import WrappedFtxClient

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

    def execute_position_changes(self):
        def create_ftx_symbol_name(position):
            if position.product_type == 'PERP':
                return f"{position.base}-PERP"
            else:
                return f"{position.base}/{position.quote}"

        new_positions = DbAccessor().fetch_unfilled_strategies()
        # For netting purposes, let's consider everything that isn't a PERP to be denominated in USDT
        # In the future, it may make sense
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
        sales = {}
        buys = {}
        for k, v in counter.items():
            if v < 0:
                sales[k] = v
            elif v > 0:
                buys[k] = v
        print(sales, buys)

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
    sales = {'AVAX/USD': -90.97665217433, 'USD/USD': 7.68063993265, 'SOL/USD': -31.618116472579505, 'SOL-PERP': -10.53875}
    for (market, notional)