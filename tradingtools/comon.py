import os
import dateparser
import pandas as pd
import numpy as np


def download_data(client, market, symbol, timeframe, start, end, path, logger, get_klines):
    """

    :param client:
    :param market:
    :param symbol:
    :param timeframe:
    :param start:
    :param end:
    :param path:
    :param logger:
    :param get_klines:
    :return:
    """
    if not os.path.isfile(f'{path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip'):
        logger.info(f'Creating File: {path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip')
        df = get_klines(client, market, symbol.upper(), timeframe, start, end)
        df.to_parquet(f'{path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip', compression='gzip', allow_truncated_timestamps=True)
    else:
        df = pd.read_parquet(f'{path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip')
        dt_range = (dateparser.parse(start, settings={'TIMEZONE': 'UTC'}), dateparser.parse(end, settings={'TIMEZONE': 'UTC'}))
        dt_data = (df['open_time'].iloc[0], df['open_time'].iloc[-1])
        logger.debug(f'Requested date range: {dt_range[0].strftime("%Y/%m/%d %H:%M:%S")} to {dt_range[1].strftime("%Y/%m/%d %H:%M:%S")}')
        logger.debug(f'Database date range: {dt_data[0].strftime("%Y/%m/%d %H:%M:%S")} to {dt_data[1].strftime("%Y/%m/%d %H:%M:%S")}')

        if (dt_range[1] < dt_data[0]) or (dt_range[0] < dt_data[0] <= dt_range[1] <= dt_data[1]):
            df = pd.concat([get_klines(client, market, symbol, timeframe, str(dt_range[0]), str(dt_data[0])).iloc[:-1], df])
            logger.debug('Range lower than database')

        elif dt_range[0] < dt_data[0] and dt_range[1] > dt_data[1]:
            df = pd.concat([get_klines(client, market, symbol, timeframe, str(dt_range[0]), str(dt_data[0])).iloc[:-1], df])
            df = pd.concat([df, get_klines(client, market, symbol, timeframe, str(dt_data[1]), str(dt_range[1])).iloc[1:]])
            logger.debug('Range lower and greater than database')

        elif (dt_data[0] <= dt_range[0] <= dt_data[1] < dt_range[1]) or (dt_range[0] > dt_data[1]):
            df = pd.concat([df, get_klines(client, market, symbol, timeframe, str(dt_data[1]), str(dt_range[1])).iloc[1:]])
            logger.debug('Range greater than database')

        else:
            logger.debug('Nothing to update')

    df.to_parquet(f'{path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip', compression='gzip', allow_truncated_timestamps=True)
    logger.info(f'Database updated and saved into {path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip')
    return None


def check_prices(price, limit, stop, side, eps, logger):
    """

    :param price:
    :param limit:
    :param stop:
    :param side:
    :param eps:
    :param logger:
    :return:
    """
    if stop is None and limit is not None:
        if (1 - eps) * limit <= price <= (1 + eps) * limit:
            logger.warning(f'Too close prices. Price {price}. Limit {limit}. Converting LIMIT to MARKET')
            stop = None
            limit = None
        elif (side == 'BUY' and price < limit) or (side == 'SELL' and price > limit):
            logger.warning(f'Inconsistent {side} prices. Limit {limit}. Price {price}. Converting LIMIT to STOP MARKET.')
            stop = limit
            limit = None

    elif stop is not None and limit is None:
        if (1 - eps) * stop <= price <= (1 + eps) * stop:
            logger.warning(f'Too close prices. Price {price}. Stop {stop}. Converting STOP MARKET to MARKET')
            stop = None
            limit = None

    elif stop is not None and limit is not None:
        if (1 - eps) * limit <= stop <= (1 + eps) * limit:
            logger.warning(f'Too close prices. Limit {limit}. Stop {stop}. Converting STOP LIMIT to STOP MARKET')
            limit = None

        elif (side == 'BUY' and stop < limit) or (side == 'SELL' and stop > limit):
            logger.warning(f'Inconsistent {side} prices. Limit {limit}. Stop {stop}. Converting STOP LIMIT to STOP MARKET.')
            stop = limit
            limit = None

    return limit, stop


def check_quantities(quantity, price, min_qty, min_notional, logger):
    """

    :param quantity:
    :param price:
    :param min_qty:
    :param min_notional:
    :param logger:
    :return:
    """
    quantity = np.floor(quantity * round(1 / min_qty)) / round(1 / min_qty)
    notional = quantity * price
    if notional < min_notional or quantity < min_qty:
        logger.error('Insufficent quantity or notional')
        return None
    return str(quantity)