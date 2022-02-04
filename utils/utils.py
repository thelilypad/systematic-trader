import typing

import pandas as pd
import numpy as np
from pathlib import Path


def prep_backtest_ohlcv_data(altcoin_ohlcv: pd.DataFrame, start_time: str,
                             end_time: str) -> pd.DataFrame:
    """
    :param altcoin_ohlcv: A dataframe indexed by time (hourly default, timezone UTC)
    :param start_time: a string representing UTC tz-aware time when to begin the dataframe
    :param end_time: a string representing UTC tz-aware time when to end the dataframe
    :return: the time-chopped pandas dataframe
    """
    df = altcoin_ohlcv.set_index(pd.to_datetime(altcoin_ohlcv.index))
    df = df.loc[start_time:end_time]
    return df


def get_project_root() -> Path:
    """
    Returns the project root file directory.
    """
    return Path(__file__).parent.parent


def convert_to_log_returns(altcoin_ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the returns data, aggregating USDT/BUSD/USDC returns (mean)
    :param altcoin_ohlcv: A dataframe with the following format:
    Column:
    - quote: str = a crypto symbol (all are assumed to be based on USD(T/C) prices
    - time_period_start: (pd.Timestamp/str) = the timestamp for each opening quote
    - px_open: float = the spot price at time_period_start
    :return: pd.DataFrame (pivot table)
    """
    pivot = altcoin_ohlcv.pivot_table(columns='quote', index='time_period_start', values=['px_open'], aggfunc='mean')
    pivot = pivot.fillna(method='ffill')
    pivot = np.log(pivot) - np.log(pivot.shift(1))
    pivot.columns = pivot.columns.get_level_values(1)
    pivot = pivot.iloc[1:]
    return pivot


def reorg_to_market_returns(pivot: pd.DataFrame, col_market: str) -> pd.DataFrame:
    """
    Reorganize the columns of a returns pivot table dataframe to put market returns (beta) as first column.
    :param pivot: pd.DataFrame = a pivot table of returns (see #convert_to_log_returns)
    :param col_market: str = the name of the column to use as "market returns" (beta)
    :return: pd.DataFrame
    """
    return pivot[[col_market] + [col for col in pivot.columns if col != col_market]]


DEFAULT_PRICE_DATA_COLUMNS: typing.Final = {
    'DOLLAR_VOLUME': 'dollar_volume',
    'PRICE_HIGH': 'px_high',
    'PRICE_LOW': 'px_low',
    'VOLUME_IN_SYMBOL_UNITS': 'sx_sum',
    'PRICE_OPEN': 'px_open',
    'PRICE_CLOSE': 'px_close',
    'TIME': 'time_period_start',
    'PRODUCT': 'product',
    'SYMBOL': 'quote',
    'BASE_UNIT': 'base',
}


def create_dollar_volume_if_needed(df: pd.DataFrame, price_columns: dict = DEFAULT_PRICE_DATA_COLUMNS) -> pd.DataFrame:
    """ Creates dollar volume by (high - low)/2 pricing for each time period.
    :param df: pd.DataFrame = a dataframe of returns, columns implied by price_columns
    :param price_columns: dict = a dict for mapping the df's columns to the function required columns
    :return: a dataframe with a dollar_volume column
    """
    if not all(
            k in price_columns.keys() for k in ('DOLLAR_VOLUME', 'PRICE_HIGH', 'PRICE_LOW', 'VOLUME_IN_SYMBOL_UNITS')):
        raise Exception("Missing required columns from price columns mapper")
    dp = price_columns
    if dp['DOLLAR_VOLUME'] in df.columns:
        return df
    df[dp['DOLLAR_VOLUME']] = ((df[dp['PRICE_HIGH']] + df[dp['PRICE_LOW']]) / 2) * df[dp['VOLUME_IN_SYMBOL_UNITS']]
    return df


def resample_timeframe(df: pd.DataFrame, timeframe: str = '1D', price_columns: dict = DEFAULT_PRICE_DATA_COLUMNS,
                       merge_stables: bool = True) -> pd.DataFrame:
    """
    Resample hourly data to daily data.
    :param df: pd.DataFrame = a dataframe of returns data with the price_columns implied columns
    :param timeframe: str = a timeframe allowed by pandas.DataFrame#resample
    :param price_columns: dict = a dict for mapping the df's columns to the function required columns
    :param merge_stables: bool = merges stablecoin price data per timestamp if true, otherwise doesn't
    :return: the resampled dataframe output
    """
    if not all(k in price_columns.keys() for k in (
            'SYMBOL', 'BASE', 'PRODUCT', 'PRICE_OPEN', 'PRICE_CLOSE', 'PRICE_HIGH', "PRICE_LOW", 'DOLLAR_VOLUME',
            'TIME')):
        raise Exception("Missing required columns from price columns mapper")
    dp = price_columns
    df = create_dollar_volume_if_needed(df, price_columns)
    df = df.set_index(pd.to_datetime(df[dp['TIME']])).groupby(
        [dp['SYMBOL'], dp['BASE'], dp['PRODUCT']]).resample(timeframe).agg(
        {dp['PRICE_OPEN']: 'first', dp['PRICE_CLOSE']: 'last', dp['PRICE_HIGH']: max, dp['PRICE_LOW']: min,
         dp['DOLLAR_VOLUME']: sum})
    if merge_stables:
        # If we have multiple stablecoins as bases (e.g. USDC, USDT, BUSD) treat all of them as equivalent
        # and take the mean prices between them for each time
        return df.groupby([dp['PRODUCT'], dp['SYMBOL'], dp['TIME']]).agg(
            {dp['PRICE_OPEN']: 'mean', dp['PRICE_CLOSE']: 'mean', dp['PRICE_HIGH']: 'mean', dp['PRICE_LOW']: 'mean',
             dp['DOLLAR_VOLUME']: sum}).reset_index()
    return df.reset_index()

def pluck(d: dict, *keys):
    return {k: v for k, v in d.items() if k in keys}


def retry(func, retries: int = 3, **args):
    num_retries = 0
    try:
        num_retries += 1
        output = func(**args)
        return output
    except Exception:
        if num_retries > self.max_retries:
            raise Exception(f'Achieved max_retries = {retries}')

def simple_pluck_dict(d: dict, keys: typing.List[str], replace_with_none: bool = True) -> typing.List:
    return [d.get(key, None) if replace_with_none else d[key] for key in keys]
