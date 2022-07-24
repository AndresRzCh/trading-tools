"""Kucoin Tools"""

import logging
import dateparser
import pandas as pd
from tradingtools import commontools

logger = logging.getLogger('kucointools')


def get_klines(client, market, symbol, timeframe, start, end):
    """
    Get the candles of a symbol in a specified time-frame between two timestamps
    :param client: client class
    :param market: 'SPOT' or 'FUTURES'
    :param symbol: symbol string ('KCS-XBT')
    :param timeframe: klines timeframe string ('5min')
    :param start: human-readable datetime for data start ('5 days ago')
    :param end: human-readable datetime for data end ('now')
    :return: pandas.DataFrame with the data
    """
    start = int(dateparser.parse(start + ' UTC').timestamp()) if start is not None else None
    end = int(dateparser.parse(end + ' UTC').timestamp()) if end is not None else None

    if market == 'SPOT':
        klines = client.get_kline_data(symbol, kline_type=timeframe, start=start, end=end)
        df = pd.DataFrame(klines, columns=['open_time', 'open', 'close', 'high', 'low', 'trades', 'vol']).astype(float)
        df['open_time'] = pd.to_datetime(df['open_time'] * 1e9).astype("datetime64[us]")
    else:
        klines = market.get_kline_data(symbol, granularity=timeframe, begin_t=start * 1000, end_t=end * 1000)
        df = pd.DataFrame(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'vol']).astype(float)
        df['open_time'] = pd.to_datetime(df['open_time'] * 1e6).astype("datetime64[us]")
    return df


def download_data(client, market, symbol, timeframe, start, end, path):
    """
    Download the desired klines to a parquet file
    :param client: client class
    :param market: 'SPOT' or 'FUTURES'
    :param symbol: symbol string ('KCS-XBT')
    :param timeframe: klines timeframe string ('5min')
    :param start: human-readable datetime for data start ('5 days ago')
    :param end: human-readable datetime for data end ('now')
    :param path: save path string
    :return: None
    """
    return commontools.download_data(client, market, symbol, timeframe, start, end, path, logger, get_klines)


def get_data(symbol, timeframe, path):
    """
    Read a parquet file
    :param symbol: symbol string ('KCS-XBT')
    :param timeframe: klines timeframe string ('5min')
    :param path: save path string
    :return: pandas.DataFrame with the data
    :return: pandas.DataFrame with the data
    """
    return pd.read_parquet(f'{path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip')
