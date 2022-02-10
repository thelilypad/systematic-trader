POSITION_EXCHANGE = 'positions'
POSITION_SCHEDULING_QUEUE = 'pos_scheduler'
POSITION_EXECUTING_QUEUE = 'pos_executor'
LOG_QUEUE = 'logs'
CLOCK_QUEUE = 'clock'
WATCHER_QUEUE = 'watcher'
DB_WRITER_QUEUE = 'db_writer'

def create_message_type(message: str, queue: str = '', exchange: str = '') -> str:
    """
    Simple method to create new message types for RabbitMQ
    """
    cos = message
    if not queue and not exchange:
        raise Exception('Either queue or exchange must be provided')
    if queue:
        cos += f'__{queue}'
    if exchange:
        cos += f'__{exchange}'
    return cos


TERMINATE_ALL_POSITIONS_EXC_MSG = create_message_type('terminate', exchange=POSITION_EXCHANGE)
NORMAL_POSITION_SCHEDULING_MSG = create_message_type('cron', queue=POSITION_SCHEDULING_QUEUE,                                                     exchange=POSITION_EXCHANGE)
CHANGE_POSITION_MSG = create_message_type('change_position', queue=POSITION_EXECUTING_QUEUE, exchange=POSITION_EXCHANGE)