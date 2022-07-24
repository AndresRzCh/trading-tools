"""Binance Tools"""
import logging
import time
import numpy as np
import pandas as pd
from tradingtools import commontools

logger = logging.getLogger('binancetools')


def fix_time(client):
    """
    Fix the client time if it drifts over some hours working
    :param client: broken client
    :return: fixed client
    """
    client.timestamp_offset = client.get_server_time()['serverTime'] - time.time() * 1000
    return client


def get_klines(client, market, symbol, timeframe, start, end, reduce=None):
    """
    Get the candles of a symbol in a specified time-frame between two timestamps
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol string ('BTCUSDT')
    :param timeframe: klines timeframe string ('5m')
    :param start: human-readable datetime for data start ('5 days ago')
    :param end: human-readable datetime for data end ('now')
    :param reduce: reduce the indices in the pandas.DataFrame to some list ([1, 150])
    :return: pandas.DataFrame with the data
    """
    cols = ['open_time', 'open', 'high', 'low', 'close', 'vol', 'close_time', 'quote_vol', 'trades', 'taker_base_vol', 'taker_quote_vol', 'ignore']
    market = market.upper()
    symbol = symbol.upper()
    fun = client.futures_historical_klines if market == 'USDM' else (client.futures_coin_klines if market == 'COINM' else client.get_historical_klines)
    df = pd.DataFrame(fun(symbol, timeframe, start + ' UTC', end + ' UTC'), columns=cols).astype(float)
    if reduce is not None:
        df = df[reduce]
    else:
        df['open_time'] = pd.to_datetime(df['open_time'] * 1e6).astype("datetime64[us]")
        df['close_time'] = pd.to_datetime(df['close_time'] * 1e6).astype("datetime64[us]")
    return df


def download_data(client, market, symbol, timeframe, start, end, path):
    """
    Download the desired klines to a parquet file
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol string ('BTCUSDT')
    :param timeframe: klines timeframe string ('5m')
    :param start: human-readable datetime for data start ('5 days ago')
    :param end: human-readable datetime for data end ('now')
    :param path: save path string
    :return: None
    """
    return commontools.download_data(client, market, symbol, timeframe, start, end, path, logger, get_klines)


def get_data(symbol, timeframe, path):
    """
    Read a parquet file
    :param symbol: symbol string ('BTCUSDT')
    :param timeframe: klines timeframe string ('5m')
    :param path: save path string
    :return: pandas.DataFrame with the data
    :return: pandas.DataFrame with the data
    """
    return pd.read_parquet(f'{path}\\{symbol.lower()}_{timeframe.lower()}.parquet.gzip')


def send_order(f, market, quantity, price, limitprice, stopprice, symbol, side, positionside, timeinforce, tick_size, eps):
    """
    Place an order into the exchange
    :param f: function to create the order
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param quantity: quantity to buy/sell in coin (0.01)
    :param price: actual price
    :param limitprice: limit price
    :param stopprice: stop price
    :param symbol: symbol ('BTCUSDT')
    :param side: 'BUY' or 'SELL'
    :param positionside: 'LONG' or 'SHORT'
    :param timeinforce: 'GTC', 'IOC' or 'FOK'
    :param tick_size: symbol resolution
    :param eps: tolerance
    :return: placed order
    """
    limitprice, stopprice = commontools.check_prices(price, limitprice, stopprice, side, eps, logger)

    if stopprice is not None:
        if (side == 'BUY' and price <= stopprice) or (side == 'SELL' and price >= stopprice):
            stopMarket = {'SPOT': 'STOP_LOSS', 'COINM': 'STOP_MARKET', 'USDM': 'STOP_MARKET'}
            stopLimit = {'SPOT': 'STOP_LOSS_LIMIT', 'COINM': 'STOP', 'USDM': 'STOP'}
        else:
            stopMarket = {'SPOT': 'TAKE_PROFIT', 'COINM': 'TAKE_PROFIT_MARKET', 'USDM': 'TAKE_PROFIT_MARKET'}
            stopLimit = {'SPOT': 'TAKE_PROFIT_LIMIT', 'COINM': 'TAKE_PROFIT', 'USDM': 'TAKE_PROFIT'}
    else:
        stopMarket = None
        stopLimit = None

    if quantity is not None:
        if limitprice is not None and stopprice is None:
            limitprice = np.floor(limitprice * round(1 / tick_size)) / round(1 / tick_size)
            logger.debug(f'Creating LIMIT {side} ({positionside}) order of {quantity} {symbol} at {limitprice}')
            return f(symbol=symbol, side=side, positionSide=positionside, type='LIMIT', quantity=quantity, price=limitprice, timeInForce=timeinforce)
        elif stopprice is not None and limitprice is None:
            stopprice = np.floor(stopprice * round(1 / tick_size)) / round(1 / tick_size)
            logger.debug(f'Creating STOP MARKET {side} ({positionside}) order of {quantity} {symbol} triggered at {stopprice}')
            return f(symbol=symbol, side=side, positionSide=positionside, type=stopMarket[market], quantity=quantity, stopPrice=stopprice)
        elif stopprice is not None and limitprice is not None:
            limitprice = np.floor(limitprice * round(1 / tick_size)) / round(1 / tick_size)
            stopprice = np.floor(stopprice * round(1 / tick_size)) / round(1 / tick_size)
            logger.debug(f'Creating STOP LIMIT {side} ({positionside}) order of {quantity} {symbol} at {limitprice} triggered at {stopprice}')
            return f(symbol=symbol, side=side, positionSide=positionside, type=stopLimit[market], quantity=quantity, price=limitprice, timeInForce=timeinforce,
                     stopPrice=stopprice)
        else:
            logger.debug(f'Creating MARKET {side} ({positionside}) order of {quantity} {symbol}')
            return f(symbol=symbol, side=side, positionSide=positionside, type='MARKET', quantity=quantity)
    else:
        logger.error('Please insert a valid notional or quantity parameter')
        return None


def create_order(client, market, symbol, side, quantity=None, notional=None, limitprice=None, stopprice=None,
                 positionside=None, timeinforce='GTC', custom=None, eps=0.001):
    """
    Place an order into the exchange (with more options that send_order)
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol ('BTCUSDT')
    :param side: 'BUY' or 'SELL'
    :param quantity: quantity to buy/sell in coin (0.01)
    :param notional: quantity to buy/sell in notional (100)
    :param limitprice: limit price
    :param stopprice: stop price
    :param positionside: 'LONG' or 'SHORT'
    :param timeinforce: 'GTC', 'IOC' or 'FOK'
    :param custom: custom order functions
    :param eps: tolerance
    :return:
    """
    function_spot = custom[0] if custom is not None else client.create_order
    function_usdm = custom[1] if custom is not None else client.futures_create_order
    function_coin = custom[2] if custom is not None else client.futures_coin_create_order
    symbol = symbol.upper()

    if market.upper() == 'USDM':
        info = next(filter(lambda x: x['symbol'] == symbol, client.futures_exchange_info()['symbols']))
        price = float(client.futures_symbol_ticker(symbol=symbol)['price'])
        order_price = float(limitprice) if limitprice else (float(stopprice) if (stopprice and not limitprice) else price)
        min_qty = float(info['filters'][1]['minQty'])
        min_notional = float(info['filters'][5]['notional'])
        tick_size = float(info['filters'][0]['tickSize'])

        if notional is None and quantity is not None:
            quantity = commontools.check_quantities(quantity, order_price, min_qty, min_notional, logger)

        elif quantity is None and notional is not None:
            quantity = notional / order_price
            quantity = commontools.check_quantities(quantity, order_price, min_qty, min_notional, logger)

        else:
            quantity = None

        return send_order(function_usdm, 'USDM', quantity, price, limitprice, stopprice, symbol, side, positionside, timeinforce, tick_size, eps)

    elif market.upper() == 'COINM':
        info = next(filter(lambda x: x['symbol'] == symbol, client.futures_coin_exchange_info()['symbols']))
        price = float(client.futures_coin_symbol_ticker(symbol=symbol)[0]['price'])
        tick_size = float(info['filters'][0]['tickSize'])

        if not (quantity % 1 == 0 and quantity > 0):
            quantity = None

        return send_order(function_coin, 'COINM', quantity, price, limitprice, stopprice, symbol, side, positionside, timeinforce, tick_size, eps)

    else:
        info = client.get_symbol_info(symbol)
        price = float(client.get_symbol_ticker(symbol=symbol)['price'])
        order_price = float(limitprice) if limitprice else (float(stopprice) if (stopprice and not limitprice) else price)
        min_qty = float(info['filters'][2]['minQty'])
        min_notional = float(info['filters'][3]['minNotional'])
        tick_size = float(info['filters'][0]['tickSize'])

        if notional is None and quantity is not None:
            quantity = commontools.check_quantities(quantity, order_price, min_qty, min_notional, logger)

        elif quantity is None and notional is not None:
            quantity = notional / order_price
            quantity = commontools.check_quantities(quantity, order_price, min_qty, min_notional, logger)

        else:
            quantity = None

        return send_order(function_spot, 'SPOT', quantity, price, limitprice, stopprice, symbol, side, positionside, timeinforce, tick_size, eps)


def get_orders(client, market, symbol=None):
    """
    Get placed orders
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol string ('BTCUSDT')
    :return: list of created orders
    """
    logger.debug(f'Getting all open orders of {symbol} at {market}.')
    if market.upper() == 'COINM':
        return client.futures_coin_get_open_orders(symbol=symbol)
    elif market.upper() == 'USDM':
        return client.futures_get_open_orders(symbol=symbol)
    else:
        return client.get_open_orders(symbol=symbol)


def cancel_orders(client, market, symbol):
    """
    Cancel all placed orders
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol string ('BTCUSDT')
    :return: number of orders cancelled
    """
    executed = 0
    if market.upper() == 'COINM':
        orders = client.futures_coin_get_open_orders(symbol=symbol)
        for order in orders:
            client.futures_coin_cancel_order(symbol=symbol, orderId=order['orderId'])
            logger.debug(f'Order {order["orderId"]} cancelled for {symbol} at COINM.')
            executed += 1
    elif market.upper() == 'USDM':
        orders = client.futures_get_open_orders(symbol=symbol)
        for order in orders:
            client.futures_cancel_order(symbol=symbol, orderId=order['orderId'])
            logger.debug(f'Order {order["orderId"]} cancelled for {symbol} at USDM.')
            executed += 1
    else:
        orders = client.get_open_orders(symbol=symbol)
        for order in orders:
            client.cancel_order(symbol=symbol, orderId=order['orderId'])
            logger.debug(f'Order {order["orderId"]} cancelled for {symbol} at SPOT.')
            executed += 1
    return executed


def get_positions(client, market, symbol=None, side=None):
    """
    Get the actual open positions
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol string ('BTCUSDT')
    :param side: 'BUY' or 'SELL'
    :return: list of open positions
    """
    logger.debug(f'Getting all active positions of {symbol} at {market}.')
    if market.upper() == 'COINM':
        df = pd.DataFrame(client.futures_coin_position_information())
        if symbol:
            if side:
                if side == 'LONG':
                    return df[(df['positionAmt'].astype(int) > 0) & (df['symbol'] == symbol)]
                return df[(df['positionAmt'].astype(int) < 0) & (df['symbol'] == symbol)]
            return df[(df['positionAmt'].astype(int) != 0) & (df['symbol'] == symbol)]
        return df[df['positionAmt'].astype(int) != 0]
    elif market.upper() == 'USDM':
        df = pd.DataFrame(client.futures_position_information())
        if symbol:
            if side:
                if side == 'LONG':
                    return df[(df['positionAmt'].astype(float) > 0) & (df['symbol'] == symbol)]
                return df[(df['positionAmt'].astype(float) < 0) & (df['symbol'] == symbol)]
            return df[(df['positionAmt'].astype(float) != 0) & (df['symbol'] == symbol)]
        return df[df['positionAmt'].astype(float) != 0]
    else:
        df = pd.DataFrame(client.get_account()['balances'])
        df['balance'] = df['free'].astype(float) + df['locked'].astype(float)
        if symbol:
            return df[(df['balance'].astype(float) != 0) & (df['asset'] == symbol)]
        return df[df['balance'].astype(float) != 0]


def close_positions(client, market, symbol=None):
    """
    Close the actual open positions
    :param client: client class
    :param market: 'SPOT', 'USDM' or 'COINM'
    :param symbol: symbol string ('BTCUSDT')
    :return: number of positions closed
    """
    executed = 0

    if market.upper() == 'COINM':
        df = pd.DataFrame(client.futures_coin_position_information())
        if symbol:
            positions = df[(df['positionAmt'].astype(int) != 0) & (df['symbol'] == symbol)]
        else:
            positions = df[df['positionAmt'].astype(int) != 0]

    elif market.upper() == 'USDM':
        df = pd.DataFrame(client.futures_position_information())
        if symbol:
            positions = df[(df['positionAmt'].astype(float) != 0) & (df['symbol'] == symbol)]
        else:
            positions = df[df['positionAmt'].astype(float) != 0]

    else:
        return

    for i in positions.index:
        symbol = positions.loc[i, 'symbol']
        quantity = abs(float(positions.loc[i, 'positionAmt']))
        if float(positions.loc[i, 'positionAmt']) > 0:
            positionSide = 'LONG'
            side = 'SELL'
        else:
            positionSide = 'SHORT'
            side = 'BUY'
        create_order(client, market, symbol, side, quantity=quantity, positionside=positionSide)
        executed += 1

    return executed
