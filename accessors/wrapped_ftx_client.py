from ftx import api
import typing
import pandas as pd
from collections import defaultdict
from datetime import timedelta
from accessors.db_accessor import DbAccessor
import multiprocess
import time
import sys
from utils.utils import pluck

sys.path.append('..')
from config import Config
from constants import *


class WrappedFtxClient:
    def __init__(self, api_key: str = None, api_secret: str = None, subaccount_name: str = None):
        if api_key and not api_secret:
            raise Exception('Provided api_key but not api_secret')
        if not api_key and api_secret:
            raise Exception('Provided api_secret but not api_key')
        self.mode = 'private_enabled' if bool(api_key) and bool(api_secret) else 'public_only'
        self.client = api.FtxClient(api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name)

    def __gate_private_method(self):
        if self.mode == 'private_enabled':
            pass
        else:
            raise Exception('Unable to access this method without api_key & api_secret')

    def get_open_orders(self, symbol: str):
        """
        A simple wrapper to fetch all available open orders for a symbol (if multiple)
        :param symbol: a market (e.g. BTC/USD)
        :return:
        """
        return self.client.get_open_orders(market=symbol)

    def cancel_orders(self, symbol: str):
        """
        Cancels all available orders for a given symbol
        :param symbol: a market (e.g. BTC/USD)
        :return:
        """
        return self.client.cancel_orders(market_name=symbol)

    def get_orderbook(self, symbol: str) -> typing.Dict[str, typing.List[typing.Tuple]]:
        """
        Gets the maximum available levels of the orderbook available for a given market
        :param symbol: a market on FTX
        :return: a list containing the price, volume for the top levels available in an orderbook
        """
        self.__gate_private_method()
        return self.client.get_orderbook(symbol, 500)

    def get_markets_info(self):
        futures = self.client.get_futures()
        return [pluck(f, 'name', 'expiryDescription', 'last', 'bid', 'ask') for f in futures if
                f['expiryDescription'] == 'Perpetual']

    def get_current_positions(self) -> typing.Tuple[typing.List, typing.List]:
        """
        A simple helper method for returning all positions (PERPS) and balances (e.g. owned coins that are not PERPS/spot
        :return: a tuple of lists of perps and coins
        """
        self.__gate_private_method()
        return self.client.get_positions(show_avg_price=True), self.client.get_balances()

    def __is_blacklisted(self, sym: str):
        for blacklist in BLACKLISTED:
            if blacklist in sym:
                return True
        if not '-' in sym and not '/' in sym:
            return True
        return False

    def project_slippage(self, symbol: str, quantity_in_units: float) -> typing.Tuple[float, float, float]:
        """
        Simple helper method to project slippage based on the available state of an FTX's market orderbook.
        The REST API will only return the first 100 levels for a given market, so any impact below that will trigger an
        error from this method.
        :param symbol: An FTX-ready market symbol (e.g. ETH-PERP, ETH/USDT)
        :param quantity_in_units: the quantity in base units (e.g. 50 ETH)
        :return: A tuple containing the following:
        - The average price of a complete fill using the state of an orderbook (a market order)
        - The total slippage incurred in filling this order (best 100 levels only)
        - The best price (best bid or ask depending on side)
        """
        orderbook = self.get_orderbook(symbol)
        # Get the right side of the book based on the quantity
        depth = orderbook['asks'] if quantity_in_units > 0 else orderbook['bids']
        # To simplify future math, use absolute values and preserve the sign as a separate var
        designed_quantity = abs(quantity_in_units)
        sign = -1 if quantity_in_units < 0 else 1

        # Grab the best price (top-of-book)
        price, quantity = depth[0]
        best_price = price

        # Find the absolute quantity available if we clean the book (100 rows)
        total_quantity_avail = sum([abs(d[1]) for d in depth])
        # If the quantity exceeds the liquidity available, warn but still try to fill
        if designed_quantity > total_quantity_avail:
            raise Exception(f'Unable to fully fill order, {abs(designed_quantity - total_quantity_avail)} missing')

        # We can compute the slippage by figuring out the averaged price if we filled
        total_fill_cost = 0
        while designed_quantity > self.EPS and len(depth):
            price_level, quantity_level = depth[0]
            quantity_level = abs(quantity_level)
            fill_level = min(quantity_level, designed_quantity)
            fill_price = fill_level * price_level
            total_fill_cost += fill_price
            designed_quantity -= fill_level
            depth = depth[1:]

        total_fill_avg_price = total_fill_cost / abs(quantity_in_units)
        slippage_ratio = total_fill_avg_price / best_price

        # If order is a sell
        if sign == -1:
            slippage_ratio = 1 - slippage_ratio
        else:
            slippage_ratio = slippage_ratio - 1
        return total_fill_avg_price, slippage_ratio, best_price

    def get_available_tickers(self) -> typing.Dict[str, typing.List]:
        """
        Gets a modified taxonomy of all available markets for FTX, separating into futures, perps, and spot.
        :return: A dict with keys {blacklisted, perps, spot, futures} - blacklisted represents markets we aren't usually
        interested in (e.g. ETHBULL, trading leveraged tokens), perps are -PERP, futures usually end with a date, and spot is the rest.
        """
        tickers = defaultdict(list)
        for item in self.client.get_markets():
            name = item['name']
            is_blacklisted = False
            for blacklist in BLACKLISTED:
                quote = name.split('/')[0].split('-')[0]
                if blacklist in quote:
                    tickers['blacklisted'].append(name)
                    is_blacklisted = True
            if is_blacklisted:
                continue
            if item.get('tokenizedEquity', False):
                tickers['tokenized_equity'].append(name)
            elif item['type'] == 'future':
                if 'PERP' in item['name']:
                    tickers['perps'].append(name)
                else:
                    tickers['futures'].append(name)
            elif item['type'] == 'spot':
                tickers['spot'].append(name)
            else:
                raise Exception("Unknown type")
        return tickers

    def parse_symbol(self, sym: str) -> typing.Tuple[str, str, str, typing.Optional[str]]:
        """
        Parses an FTX market symbol name into {base, quote, product_type, expiry_date} (expiry_date is only present
        in true futures). Product type can be {PERP, MOVE, FUTURE, or SPOT}.
        :param sym: a market name (e.g. ETH-PERP)
        :return: a tuple representing {base, quote, product_type, expiry_date}
        """
        base = ''
        quote = ''
        product_type = ''
        expiry_date = ''
        if '/' in sym:
            product_type = 'SPOT'
            base, quote = sym.split('/')
        elif '-' in sym:
            splits = sym.split('-')
            base = splits[0]
            if len(splits) > 2:
                product_type = 'MOVE'
                expiry_date = '-'.join(splits[-2:])
            else:
                if splits[-1] == 'PERP':
                    product_type = 'PERP'
                    quote = 'USDT'
                else:
                    product_type = 'FUTURE'
                    expiry_date = splits[-1]
                    quote = 'USDT'
        else:
            raise Exception(f'Unknown product type for {sym}')
        return base, quote, product_type, expiry_date

    def fetch_funding_data_since_beginning(self, client, symbol: str, until_ts: pd.Timestamp = None, ) -> pd.DataFrame:
        """
        Simple job to fetch funding rates for FTX and chunk it into a dataframe up to a certain point.
        :param client: the FTX client (passed from the thread)
        :param symbol: a market name
        :param until_ts: the timestamp to fetch until
        :return: a dataframe representing the new data fetched for funding
        """
        start = time.time()
        results = []
        until = None
        if until_ts:
            until = until_ts.value / 1e9
        else:
            until = 1
        while True:
            data = self.client.get_funding_rates(symbol, end_time=start)
            if len(data) <= 1:
                break
            new_start = pd.to_datetime(pd.DataFrame(data)['time'].min())
            if start < until:
                break
            results += data
            start = new_start.value / 1e9
        df = pd.DataFrame(results).sort_values(by='time').set_index('time')
        df['future'] = symbol
        df['symbol'] = symbol.split('-')[0]
        df['exchange'] = 'FTX'
        df['fetch_time'] = pd.to_datetime(time.time(), unit='s')
        if until_ts:
            return df.loc[str(until_ts + timedelta(seconds=3600)):]
        else:
            return df

    def fetch_historical_data_since_beginning(self, client,
                                              symbol: str,
                                              resolution: str = '1h',
                                              limit: int = 10000,
                                              until_ts: pd.Timestamp = None,
                                              ) -> pd.DataFrame:


        if not resolution in self.RESOLUTIONS_MAP:
            raise Exception(
                f"{resolution} not found in RESOLUTIONS_MAP. Available resolutions are: {list(self.RESOLUTIONS_MAP.keys())}")
        res = self.RESOLUTIONS_MAP[resolution]
        start = time.time()
        results = []
        until = None
        if until_ts:
            until = until_ts.value / 1e9
        else:
            until = 1
        while True:
            data = client.get_historical_data(symbol, resolution=res, limit=10000, end_time=start)
            if len(data) == 1:
                break
            new_start = pd.to_datetime(data[0]['startTime'])
            if start < until:
                break
            results += data
            start = new_start.value / 1e9
        df = pd.DataFrame(results).sort_values(by='startTime').set_index('startTime')
        df['exchange'] = 'FTX'
        df['resolution'] = res
        df['fetch_time'] = pd.to_datetime(time.time())
        base, quote, product_type, expiry_date = self.parse_symbol(symbol)
        df['base'] = base
        df['quote'] = quote
        df['product_type'] = product_type
        df['expiry_date'] = expiry_date
        del df['time']
        if until_ts:
            return df.loc[str(until_ts + timedelta(seconds=res)):]
        else:
            return df

    def __fetch_1h_price_data(self, client, symbol: str, until_ts: pd.Timestamp = None, ):
        df = self.fetch_historical_data_since_beginning(self.client, symbol, '1h', until_ts=until_ts)
        df = df.reset_index()
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'quote_volume', 'exchange', 'resolution',
                      'fetch_time', 'base', 'quote', 'product_type', 'expiry_date']
        return df

    def __read_worker(self, argz):
        read_func, queue, db_write_queue, last_ts = argz
        while True:
            symbol = queue.get(block=True)  # block=True means make a blocking call to wait for items in queue
            if symbol is None:
                print("Shutting down fetch worker...")
                db_write_queue.put(None)
                break
            base, quote, product_type, expiry_date = self.parse_symbol(symbol)
            reconstructed_sym = f"{base}-{quote}-{product_type}"
            print(f'Fetching historical data for {symbol} since {last_ts.get(reconstructed_sym, None)}')
            df = read_func(self.client, symbol, until_ts=last_ts.get(reconstructed_sym, None))
            db_write_queue.put((symbol, df))

    def __write_worker(self, argz):
        write_table, db_write_queue, last_ts = argz
        engine = db.create_engine(Config.get_property("SQL_URI").unwrap())
        while True:
            (symbol, df) = db_write_queue.get(block=True)
            if df is None:
                print('Shutting down write worker...')
                break
            print(f'Writing {symbol} to {write_table}....')
            df.to_sql(write_table, con=engine, if_exists='append', index=False, method='multi')

    def run_update_job(self, tickers_to_fetch: list, read_func, write_table: str, until_ts: dict):
        """
        Simple utility method for wrapping both funding and price update jobs.
        This additionally spins up multiple processes (as many as CPUs available) in order to more rapidly
        execute the update job.
        :param tickers_to_fetch: A list of the tickers to fetch
        :param read_func: A function to execute on read success
        :param write_table: The output table to write data to
        :param until_ts: A dict of timestamps representing the last recorded data for a given price or funding pair
        :return: {None}
        """
        read_queue = multiprocess.Queue()
        db_write_queue = multiprocess.Queue()
        for item in tickers_to_fetch:
            read_queue.put(item)
        read_queue.put(None)

        read_process = multiprocess.Process(target=self.__read_worker,
                                            args=((read_func, read_queue, db_write_queue, until_ts),))
        write_process = multiprocess.Process(target=self.__write_worker,
                                             args=((write_table, db_write_queue, until_ts),))
        read_process.start()
        write_process.start()
        read_process.join()
        write_process.join()

    def get_net_account_value(self) -> float:
        """
        Gets the current account value, based on the USD values of each of the balances.
        This should factor in the PERP payouts, which reflect properly in USD balances.
        """
        return sum([b['usdValue'] for b in self.client.get_balances()])

    def get_notional_exposures(self) -> typing.Dict[str, float]:
        """
        This provides a dict of the balances (coins) and positions (perps)
        present in the account, along with their notional exposure (not net collateral).
        """
        notionals = {}
        for coin in self.client.get_balances():
            notionals[coin['coin']] = coin['usdValue']
        for future in self.client.get_positions():
            notionals[future['future']] = future['cost']
        return notionals

    def run_update_all_funding(self):
        """
        Update job for all funding data in the DB.
        :return: {None}
        """
        until_ts = defaultdict(pd.Timestamp)
        for listing in DbAccessor().get_symbol_last_funding():
            (base, quote, product_type, exchange, last_fetch) = listing
            until_ts[f"{base}-{quote}-{product_type}"] = pd.to_datetime(last_fetch)
        tickers = self.get_available_tickers()
        tickers_to_fetch = tickers['perps']
        self.run_update_job(tickers_to_fetch, self.fetch_funding_data_since_beginning, 'funding_data', until_ts)

    def run_update_all_prices(self):
        """
        Update job for all price data in the DB.
        :return: {None}
        """
        until_ts = defaultdict(pd.Timestamp)
        for listing in DbAccessor().get_symbol_last_prices():
            (base, quote, product_type, exchange, last_fetch) = listing
            until_ts[f"{base}-{quote}-{product_type}"] = pd.to_datetime(last_fetch)

        tickers = self.get_available_tickers()
        tickers_to_fetch = tickers['spot'] + tickers['perps']
        self.run_update_job(tickers_to_fetch, self.__fetch_1h_price_data, 'price_data', until_ts)

if __name__ == '__main__':
    p = WrappedFtxClient(
        api_key=Config.get_property('FTX_API_KEY').unwrap(),
        api_secret=Config.get_property('FTX_API_SECRET').unwrap(),
        subaccount_name=Config.get_property('FTX_SUBACCOUNT_NAME').unwrap()
    )
    print(p.get_orderbook('BTC-PERP'))
