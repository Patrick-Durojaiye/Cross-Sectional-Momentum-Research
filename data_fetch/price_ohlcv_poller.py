import ccxt
import pandas as pd
import logging
from multiprocessing import Pool, cpu_count
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename='ohlcv_data_fetch.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class FuturesDataPoller:
    def __init__(self, exchange_name, market_type='future'):
        """
        Initialize the Binance Futures client.
        :param market_type: 'future' for USDⓈ-M Futures, 'delivery' for COIN-M Futures
        """

        if exchange_name.lower() == 'binance-futures':
            self.exchange = ccxt.binanceusdm({
                'enableRateLimit': True,
                'options': {'defaultType': market_type},
            })
        elif exchange_name.lower() == 'bybit':
            self.exchange = ccxt.bybit({
                'enableRateLimit': True,
            })
        else:
            raise ValueError('Exchange must be either "binance-futures" or "bybit"')
        self.exchange_name = exchange_name.lower()
        self.market_type = market_type
        self.exchange.load_markets()

    @staticmethod
    def _format_symbol(symbol):
        """

        :param symbol:
        :return:
        """
        return symbol+'/USDT:USDT'

    def fetch_historical_ohlcv(self, symbol, timeframe='1m', start_date=None):
        """
        Fetch historical OHLCV data beyond the API limit by iterating.
        :param symbol: The trading pair symbol (e.g., 'BTC/USDT')
        :param timeframe: The timeframe for the candlesticks (e.g., '1m', '1h', '1d')
        :param start_date: Starting date as a string in 'YYYY-MM-DD' format
        :return: A pandas DataFrame with the complete OHLCV data
        """

        formatted_symbol = self._format_symbol(symbol)

        since = self.exchange.parse8601(start_date) if start_date else None

        all_ohlcv = []

        while since is None or since < self.exchange.milliseconds():
            try:
                ohlcv = self.exchange.fetch_ohlcv(formatted_symbol, timeframe, since=since, limit=1500)
                print("Done one")
                if not ohlcv:
                    break
                all_ohlcv.extend(ohlcv)
                since = ohlcv[-1][0] + 1  # Move to the next timestamp
                time.sleep(1)

            except ccxt.BaseError as e:
                logger.error(
                    f'Error fetching OHLCV data for {formatted_symbol}: {str(e)}'
                )

                print(f'Error fetching OHLCV data for {formatted_symbol}: {str(e)}')
                break

        try:
            if all_ohlcv:
                df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

                file_name = f"{symbol}_ohlcv.csv"
                file_path = f"../data/uncleaned/ohlcv/{file_name}"

                df.to_csv(file_path, index=False)
                logging.info(f"OHLCV for {formatted_symbol} has been saved to {file_path}")
                return df

            else:
                logger.info(f"No data found for {formatted_symbol} with the given parameters.")
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        except Exception as e:
            logger.error(f"Error processing OHLCV data for {formatted_symbol}: {str(e)}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])


def process_row(row):

    symbol = row['Symbol']
    start_date = row['First_Sighted_Date']
    exchange_name = row['Exchange']

    start_date_iso = f"{start_date}T00:00:00Z"

    try:
        logger.info(f"Processing {symbol} on {exchange_name} starting from {start_date_iso}")
        if exchange_name == 'binance-futures':
            binance_poller.fetch_historical_ohlcv(symbol=symbol, timeframe='1m', start_date=start_date_iso)
        elif exchange_name == 'bybit':
            bybit_poller.fetch_historical_ohlcv(symbol=symbol, timeframe='1m', start_date=start_date_iso)
        else:
            logger.error(f"Exchange {exchange_name} not recognized for {symbol}")
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
        print(f"Error processing {symbol}: {str(e)}")


binance_poller = FuturesDataPoller(exchange_name='binance-futures', market_type='future')
bybit_poller = FuturesDataPoller(exchange_name='bybit', market_type='future')

if __name__ == '__main__':

    # universe_data = pd.read_csv("../data/cleaned/universe_exchange_data/universe_exchange_data.csv")

    retry_rows = [

        {"Symbol": "JST", "First_Sighted_Date": "2023-01-01", "Exchange": "bybit"},
        {"Symbol": "FLR", "First_Sighted_Date": "2023-03-17", "Exchange": "bybit"}
    ]

    num_processes = 10

    with Pool(processes=num_processes) as pool:

        # pool.map(process_row, [row for _, row in universe_data.iterrows()])
        pool.map(process_row, retry_rows)
