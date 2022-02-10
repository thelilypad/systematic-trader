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
    id: int
    strategy: str
    group: str
    quote: str
    base: str
    exchange: str
    product_type: str
    relative_size: float
    processed_timestamp: pd.Timestamp
    timestamp: pd.Timestamp = None