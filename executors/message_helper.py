import json
import typing

import pika
import message_constants as msg


class MessageHelper:
    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel, sender: str):
        self.channel = channel
        self.sender = sender

    """
    Messages to send to executors/position_executor.py
    """
    def position_executor_change_positions(self):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.POSITION_EXECUTING_QUEUE, body=json.dumps({"message_type": msg.CHANGE_POSITION_MSG}))

    """
    Messages to send to executors/strategy_executor.py
    """
    def strategy_executor_recalculate_positions(self):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.POSITION_SCHEDULING_QUEUE, body=json.dumps({"message_type": msg.POSITION_RECALCULATION_MSG}))

    """
    Messages to send to executors/log_executor.py
    """

    def log_info_message(self, message: str, other_data: dict):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.LOG_QUEUE, body=json.dumps(
            {
                "message_type": "INFO",
                "sender": self.sender,
                "message": message,
                "other_data": other_data,
            }
        ))

    def log_error_message(self, exception: str, other_data: dict):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.LOG_QUEUE, body=json.dumps(
            {
                "message_type": "ERROR",
                "sender": self.sender,
                "message": exception,
                "other_data": other_data,
            }
        ))

    """
    Messages to send to executors/db_writer_executor.py
    """

    def db_write_strategy_filled(self, position_ids: typing.List[float]):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.DB_WRITER_QUEUE, body=json.dumps({
                                       'message_type': 'mark_strategy_filled',
                                       'position_ids': position_ids,
                                   }))

    def db_write_fill_order_data(self, order_data: dict):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.DB_WRITER_QUEUE, body=json.dumps({
                                       'message_type': 'record_fills',
                                       'order_data': order_data,
                                   }))
