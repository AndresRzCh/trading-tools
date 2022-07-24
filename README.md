# Trading API Tools
Helper functions for using crypto exchanges API in a simpler way. Currently only Binance and Kucoin are implemented.

## Features
* ``fix_time``: Sync the timestamp from the Binance API with the local timestamp to prevent errors
* ``get_klines``: Get the candlesticks for a symbol in a timeframe during a time period
* ``download_data``: Save the previous candlesticks into a parquet file
* ``get_data``: Open a given parquet file
* ``create_order``: Places an smart order. Both quantity or notional can be specified and limit / market / stop
orders are automatically distinguished 
* ``get_orders``: Get a list of all open orders in a given market (Spot / USDM / COINM)
* ``cancel_orders``: Cancel all open orders in a given market 
* ``get_positions``: Get a list of all open positions in a given market
* ``close_positions``: Automatically closes all open positions in a given market

## Disclaimer
There are no warranties expressed or implied in this repository.  
I am not responsible for anything done with this program.  
You assume all responsibility and liability.  
Use it at your own risk.  