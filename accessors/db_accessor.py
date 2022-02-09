from datetime import datetime

import sqlalchemy as db
from sqlalchemy.sql import text

from models.position import Position
from config import Config
import pandas as pd
from typing import List, Tuple

class DbWriteException(BaseException):
    pass

"""
Simple wrapper class for supporting DB queries via SQL Alchemy
(this actually just mostly ignores SQL alchemy and runs raw SQL)
"""
class DbAccessor:
    metadata = db.MetaData()
    engine = db.create_engine(Config.get_property("SQL_URI").unwrap())
    connection = engine.connect()

    def get_symbol_last_prices(self) -> List[Tuple]:
        """
        Returns the last timestamp for all symbol-product_type-exchange pairs, largely to determine what the last recorded data was for update jobs for prices.
        :return: returns a list of (base, quote, product_type, exchange, max_timestamp) for all symbols
        """
        query = text(
            """select base, quote, product_type, exchange, max(timestamp) from price_data group by base, quote, product_type, exchange;""")
        rs = self.connection.execute(query).fetchall()
        return rs

    def get_symbol_last_funding(self) -> List[Tuple]:
        """
        Returns the last timestamp for all future-exchange pairs, largely to determine what the last recorded data was for update jobs for funding.
        :return: returns a list of (future, exchange, max(timestamp)) for all symbols
        """
        query = text("""select future, exchange, max(timestamp) from funding_data group by future, exchange;""")
        rs = self.connection.execute(query).fetchall()
        return rs

    def get_full_history_price_data_for_symbol_and_exchange(self, exchange: str, quote: str, base: str, product_type: str = 'SPOT') -> List[Tuple]:
        """
        Retrieves the full available price data for a specific (base, quote, product_type, exchange) - e.g.
        all price data for BTC, USDT, SPOT, FTX
        :param exchange: a crypto exchange
        :param quote: the quote part of a specific symbol (e.g. BTC)
        :param base: the base of a specific symbol (for PERPs this will usually be listed as USDT, but the symbol name for FTX for example is -PERP)
        :param product_type: the type of product this is for (PERP, FUTURE, SPOT, etc.)
        :return: a list of all price data
        """
        query = text(
            """SELECT * from price_data where exchange=:exchange and quote=:quote and base=:base and product_type=:product_type"""
        )
        result = self.connection.execute(query, exchange=exchange, quote=quote, base=base,
                                    product_type=product_type).fetchall()
        return result

    def get_price_data_since(self, date_cutoff: pd.Timestamp) -> List[Tuple]:
        """
        Get all price data of all assets since a specific datetime.
        :param date_cutoff: A datetime to use as a cutoff for fetching
        :return: All price data available since {date_cutoff}
        """
        query = text(
            """SELECT * from price_data where timestamp > :date_cutoff"""
        )
        result = self.connection.execute(query, date_cutoff=str(date_cutoff)).fetchall()
        return result

    def write_new_strategy_positions(self, positions: List[Position]) -> pd.Timestamp:
        """
        Writes a list of positions returned from a strategy run to the strategy_queue table.
        This is read for strategy execution, and essentially takes the output of a strategy and preps it to be executed
        in a given exchange.
        :param positions: a list of Position objects representing our strategy output
        :return: the timestamp for insertion of our strategy Positions
        """
        insert_ts = datetime.now()
        def convert_to_tuples(x: Position) -> str:
            """
            Simple helper method to convert the position object into an SQL string. This is slightly more performant and infinitely
            more buggy than properly using the ORM.
            :param x: the Position object
            :return: an SQL-compliant string for inserts
            """
            to_str = lambda x: f"'{x}'"
            pos = ",".join(list(map(to_str, [str(insert_ts), x.strategy, x.group, x.quote, x.base, x.exchange, x.product_type, str(x.relative_size)])))
            return f'({pos})'

        query = text(
            f'INSERT INTO strategy_queue(timestamp, strategy, "group", quote, base, exchange, product_type, relative_size) VALUES {",".join([convert_to_tuples(position) for position in positions])}'
        )
        rs = self.connection.execute(query)
        return insert_ts

    def mark_strategy_filled(self, positions: List[Position]) -> List[Tuple]:
        """
        Used to mark a strategy as 'executed' - that is, the positions returned by the strategy have been correctly filled live.
        :param positions: Fetched positions we just filled
        :return: likely empty result set
        """
        if not positions:
            raise Exception('Positions must be provided')
        ids = ", ".join([str(pos.id) for pos in positions])
        processed_ts = datetime.now()
        query = text(
            """
                UPDATE strategy_queue
                SET processed_timestamp=:processed_ts
                where id in ({ids})
            """
        )
        rs = self.connection.execute(query, processed_ts=processed_ts, ids=[pos.id for pos in positions])
        return rs

    def fetch_unfilled_strategies(self) -> List[Position]:
        """
        Returns all strategy positions in queue that have not been processed (waiting to execute).
        :return: A list of positions to execute
        """
        to_pos = lambda x: Position(id=x[0], timestamp=pd.to_datetime(x[1]), strategy=x[2], base=x[3], quote=x[4], exchange=x[5], product_type=x[6], group=x[7], relative_size=x[8])
        query = text(
            """
                SELECT * from strategy_queue
                    where processed_timestamp is NULL
            """
        )
        rs = self.connection.execute(query).fetchall()
        return list(map(to_pos, rs))

if __name__ == '__main__':
    position = Position(
        strategy='alpha1',
        group='',
        base='BTC',
        quote='USDT',
        exchange='FTX',
        product_type='PERP',
        relative_size=0.5
    )
    print(DbAccessor().fetch_unfilled_strategies()[0].__dict__)