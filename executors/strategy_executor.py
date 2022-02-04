#!/usr/bin/env python
import pika, sys, os
import message_constants as msg
import json
from config import Config
from utils.db_accessor import DbAccessor

"""
Handles position calculation.
"""
class StrategyExecutor:

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=Config.get_property("RABBITMQ_SERVER_URI", 'localhost').unwrap())
        )
        self.channel = self.connection.channel()
        r = self.channel.queue_declare(queue=msg.POSITION_SCHEDULING_QUEUE)
        self.channel.queue_bind(r.method.queue, exchange=msg.POSITION_EXCHANGE)
        self.channel.basic_consume(queue=msg.POSITION_SCHEDULING_QUEUE,
                              on_message_callback=self.on_message,
                              auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.connection.close()

    def recalculate_positions(self):
        positions = []
        try:
            insert_ts = DbAccessor().write_new_strategy_positions(positions)
            self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.LOG_QUEUE, body=
                                       json.dumps(
                                           {
                                               "message_type": "INFO",
                                               "sender": "PositionCalculator",
                                               "message": f"Successfully calculated positions for strategy {strategy} at {insert_ts}",
                                               "other_data": json.dumps(positions),
                                           }
                                       ))
        except Exception as e:
            self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.CLOCK_QUEUE, body=
                                       json.dumps(
                                           {
                                               "message_type": "ERROR",
                                               "sender": "PositionCalculator",
                                               "message": f"Unable to recompute positions for strategy {strategy}",
                                               "other_data": json.dumps(e),
                                           }
                                       ))

    def on_message(self, ch, method, properties, body):
        b = json.loads(body)
        if b['message_type'] == msg.TERMINATE_ALL_POSITIONS_EXC_MSG:
            print('Terminating position scheduling process...')
            self.stop()
        elif b['message_type'] == msg.NORMAL_POSITION_SCHEDULING_MSG:
            print('Recalculating positions...')
            self.recalculate_positions()

if __name__ == '__main__':
    StrategyExecutor().run()