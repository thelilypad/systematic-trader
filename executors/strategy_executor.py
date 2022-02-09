#!/usr/bin/env python
import abc
import typing

import message_constants as msg
import json
from config import Config
from accessors.db_accessor import DbAccessor
from pika.exchange_type import ExchangeType

from executors.simple_executor import SimpleExecutor
from models.position import Position

"""
Handles position calculation.
"""


class StrategyExecutor(SimpleExecutor):
    def __init__(self):
        super().__init__(rabbit_mq_host=Config.get_property("RABBITMQ_SERVER_URI", "localhost").unwrap(),
                         exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.direct,
                         queue=msg.POSITION_SCHEDULING_QUEUE)

    @abc.abstractmethod
    def run_strategy(self) -> typing.List[Position]:
        return []

    def recalculate_positions(self):
        try:
            positions = self.run_strategy()
            insert_ts = DbAccessor().write_new_strategy_positions(positions)
            self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.LOG_QUEUE, body=
            json.dumps(
                {
                    "message_type": "INFO",
                    "sender": "PositionCalculator",
                    "message": f"Successfully calculated positions for strategy {strategy.name} at {insert_ts}",
                    "other_data": json.dumps(positions),
                }
            ))
        except Exception as e:
            self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.CLOCK_QUEUE, body=
            json.dumps(
                {
                    "message_type": "ERROR",
                    "sender": "PositionCalculator",
                    "message": f"Unable to recompute positions for strategy {strategy.name}",
                    "other_data": json.dumps(e),
                }
            ))

    def on_message_consumption(self, ch, method, properties, body):
        b = json.loads(body)
        if b['message_type'] == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
            print('Terminating position scheduling process...')
            self.stop()
        elif b['message_type'] == msg.NORMAL_POSITION_SCHEDULING_MSG:
            print('Recalculating positions...')
            self.recalculate_positions()


if __name__ == '__main__':
    StrategyExecutor().run()
