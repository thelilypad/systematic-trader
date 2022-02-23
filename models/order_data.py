from dataclasses import dataclass
import typing

@dataclass
class OrderData:
    quote: str
    base: str
    exchange: str
    product_type: str
    best_price: float
    fill_average_price: float
    slippage_ratio: float
    fill_json: str
    order_quantity: float
    order_type: str
    unfilled_quantity: float
    quantity_type: str
    start_timestamp: float
    end_timestamp: float

    @staticmethod
    def to_insert(order: "OrderData", table: str = 'fills') -> str:
        # this takes advantage of the fact that the order is preserved in the internal
        # dict representation
        columns = ", ".join(list(order.__dict__.keys()))
        items = ", ".join([str(i) if not isinstance(i, str) else f"'{i}'" for i in list(order.__dict__.values())])
        return f"INSERT INTO {table} ({columns}) VALUES ({items})"