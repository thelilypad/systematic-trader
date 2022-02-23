import schedule
import time
from pika.exchange_type import ExchangeType

import message_constants as msg
import json
from executors.simple_executor import SimpleExecutor


class ClockExecutor(SimpleExecutor):
    """
    Simple clock executor for handling orchestration of other services.
    """

    def __init__(self, rabbit_mq_host: str):
        super().__init__(rabbit_mq_host=rabbit_mq_host, exchange=msg.POSITION_EXCHANGE,
                         exchange_type=ExchangeType.fanout, queue=msg.CLOCK_QUEUE)

    def trigger_position_execution(self):
        self.message_helper.position_executor_change_positions()

    def trigger_position_recalculation(self):
        self.message_helper.strategy_executor_recalculate_positions()

    def run(self):
        schedule.every(5).seconds.do(self.trigger_position_recalculation)
        schedule.every(10).seconds.do(self.trigger_position_execution)
        self.channel.start_consuming()
        while not self.has_stopped:
            schedule.run_pending()
            time.sleep(1)

    def stop(self):
        self.channel.basic_publish(exchange=msg.POSITION_EXCHANGE, routing_key=msg.POSITION_SCHEDULING_QUEUE,
                                   body=json.dumps({"message_type": msg.TERMINATE_ALL_POSITIONS_EXC_MSG}))
        super().stop()

    def on_message_consumption(self, ch, method, properties, body):
        self.stop()


if __name__ == '__main__':
    clock = ClockExecutor()
    try:
        clock.run()
    except KeyboardInterrupt:
        clock.stop()
