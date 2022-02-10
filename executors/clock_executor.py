import schedule
import time
import pika
import message_constants as msg
import json
from config import Config
from executors.simple_executor import SimpleExecutor

"""
Simple clock setup for dictating when the position recalculation job should execute.
"""
class ClockExecutor(SimpleExecutor):
    def __init__(self, rabbit_mq_host: str):
        super().__init__(rabbit_mq_host=rabbit_mq_host, exchange=msg.POSITION_EXCHANGE, exchange_type=ExchangeType.fanout, queue=msg.CLOCK_QUEUE)

    def trigger_position_execution(self):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.POSITION_EXECUTING_QUEUE, body=json.dumps({"message_type": msg.CHANGE_POSITION_MSG}))

    def trigger_position_recalculation(self):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.POSITION_SCHEDULING_QUEUE, body=json.dumps({"message_type": msg.POSITION_RECALCULATION_MSG}))

    def run(self):
        schedule.every(5).seconds.do(self.trigger_position_recalculation)
        schedule.every(10).seconds.do(self.trigger_position_execution)
        self.channel.start_consuming()
        while not self.has_stopped:
            schedule.run_pending()
            time.sleep(1)

    def stop(self):
        print("Stopping strategy clock...")
        self.has_stopped = True
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.POSITION_SCHEDULING_QUEUE, body=json.dumps({"message_type": msg.TERMINATE_ALL_POSITIONS_EXC_MSG}))
        self.connection.close()

    def on_message(self, ch, method, properties, body):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.LOG_QUEUE, body=body)
        self.stop()

if __name__ == '__main__':
    clock = StrategyClock()
    try:
        clock.run()
    except KeyboardInterrupt:
        clock.stop()
