import pandas as pd
from dataclasses import dataclass


@dataclass
class Position:
    """
    A simple helper model representing a position as stored in `strategy_queue` DB table.
    This operates like a traditional queue:
    - When a position is "processed" (e.g. processed_timestamp is not null), this means it has already been handled by the position_executor
    - A position belongs to a strategy, which will eventually map to subaccounts
    - A position can also belong to a group, which can be used for performance attribution later on (maybe)
    - A position represents a relative size (e.g. some fraction of -1 to 1 * total exposure allowed)
    - A position represents a {base, quote, exchange, product type}
    """

    def __init__(self, id: int, strategy: str, group: str,
                 quote: str, base: str, exchange: str,
                 product_type: str, relative_size: float,
                 timestamp: pd.Timestamp = None, processed_timestamp: pd.Timestamp = None):
        self.id = id
        self.timestamp = timestamp
        self.processed_timestamp = processed_timestamp
        self.strategy = strategy
        self.group = group
        self.quote = quote
        self.base = base
        self.exchange = exchange
        self.product_type = product_type
        self.relative_size = relative_size
