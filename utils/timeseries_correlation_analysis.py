import typing

import pandas
import pandas as pd
import numpy as np
from numpy.lib.stride_tricks import as_strided as stride
from tqdm import tqdm
import sys, os
sys.path.insert(0, os.path.abspath('..'))

import data.accessor.big_query as bq
tqdm.pandas()


def compute_beta(df: pd.DataFrame, name: str = 'Beta', market_idx: int = 0) -> pd.Series:
    X = df.values[:, [market_idx]]
    # prepend a column of ones for the intercept
    X = np.concatenate([np.ones_like(X), X], axis=1)
    # matrix algebra
    b = np.linalg.pinv(X.T.dot(X)).dot(X.T).dot(df.values[:, 1:])
    return pd.Series(b[1], df.columns[1:], name=name)


class TimeseriesCorrelationAnalysis:
    def __init__(self, df: pd.DataFrame, null_fill_method: str = 'ffill'):
        self.df = df
        self.df = self.df.fillna(method=null_fill_method)

    def __roll(self, window, **kwargs):
        v = self.df.values
        d0, d1 = v.shape
        s0, s1 = v.strides

        a = stride(v, (d0 - (window - 1), window, d1), (s0, s0, s1))

        rolled_df = pd.concat({
            row: pd.DataFrame(values, columns=self.df.columns)
            for row, values in zip(self.df.index, a)
        })

        return rolled_df.groupby(level=0, **kwargs)

    def beta(self, window: int = 120):
        dfs = self.__roll(window=window)
        up_dfs = []
        down_dfs = []
        for name, df in dfs:
            mkt_returns = df[df.columns[0]]
            up_dfs.append(df[mkt_returns >= 0])
            down_dfs.append(df[mkt_returns < 0])
        print('computing betas')
        all_beta = dfs.progress_apply(compute_beta)
        down_beta = pd.Series(down_dfs).apply(lambda x: compute_beta(x, name='Down Beta'))
        up_beta = pd.Series(up_dfs).apply(lambda x: compute_beta(x, name='Up Beta'))
        return all_beta, down_beta, up_beta

    def rolling_corr(self, window: int = 120):
        return self.df.rolling(window=window).corr(pairwise=True)[(len(self.df.columns) * (window - 1)):]

    def pairwise_median_corr_by_window(self, period_start: int, rolling_window: int = 120):
        multindex = self.rolling_corr(window=rolling_window)
        datetimes = multindex.index.get_level_values(0).unique()
        medianed_values = multindex.iloc[multindex.index.get_level_values(0).isin(datetimes[period_start:])]
        listings = []
        for column in tqdm(medianed_values.columns):
            zf = medianed_values[column].unstack().reset_index()
            zf = zf.set_index('datetime')
            for col in zf.columns:
                if col == column:
                    continue
                listings.append({"col1": column, "col2": col, "median_corr": zf[col].median()})
        return pd.DataFrame(listings)

    def compute_beta_for_indices_on_coins(self, coin_log_returns: pd.DataFrame):
        pass

if __name__ == '__main__':
    dates = pd.date_range('1995-12-31', periods=480, freq='M', name='Date')
    stoks = pd.Index(['s{:04d}'.format(i) for i in range(4000)])
    df = pd.read_csv('combined_index_returns.csv')
    df = df.set_index('Unnamed: 0')
    df = df.fillna(method='ffill')
    TimeseriesCorrelationAnalysis(df).beta().to_csv('beta.csv')