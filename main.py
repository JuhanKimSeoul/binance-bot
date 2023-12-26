import asyncio
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
from datetime import datetime as dt, timedelta as td
import pytz
import pandas as pd
from os import path, getenv
from dotenv import load_dotenv
from aiogram import Bot, types
from aiogram import Dispatcher
import io
import logging
import time
import functools
import argparse

'''[From Binance API]
WebSocket connections have a limit of 10 incoming messages per second.
A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.
A single connection can listen to a maximum of 200 streams.
A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark
'''

'''[Server Time Setting Tips]
if linux server time is not synced with the real time like in wsl environment, then executes this
    sudo hwclock -s

if you are running this program on remote server(linux), then you should set the timezone of the server yourself 
for checking log

'''
#########################################################################################
#########################################################################################
#########################################################################################
'''Create an Argument Parser'''
parser = argparse.ArgumentParser(description='Script to perform some operation on a file.')

# Add a positional argument for the input file
parser.add_argument('function', type=str, help='function')

# Add an optional argument for the output file
parser.add_argument('--output', type=str, help='file name of output', default='taker_buy_volume.csv')
parser.add_argument('--timezone', type=str, help='timezone which you are going to convert into', default='Asia/Seoul')
parser.add_argument('--interval', type=str, help='kline interval', default='5m')
parser.add_argument('--timedelta', type=int, help='timedelta(minutes) for getting mean value', default='60')
parser.add_argument('--multiplier', type=int, help='multiplier for threshold of alarming', default='5')
parser.add_argument('--timeout', type=int, help='timeout(seconds) for getting data from server', default='300')

# Add a flag for verbose mode (default is False)
parser.add_argument('--verbose', action='store_true', help='Enable verbose mode')

# Parse the command-line arguments
args = parser.parse_args()
#########################################################################################
#########################################################################################
#########################################################################################
'''Global Variables'''
# Queue to communicate between producer and consumer
# the reason why this getting queue code should not be here is : initiating Queue makes an event loop if it is not executed on coroutine or a callback
# plotting_queue = asyncio.Queue()

# for time frame replacing to seoul standard
# seoul : GMT + 9
timezone = pytz.timezone(args.timezone)

# saved file path
file_path = args.output

# kline interval variable 
# default 5m
interval = args.interval

# timedelta, multiplier for measuring the mean taker buy volume
timedelta = args.timedelta
multiplier = args.multiplier

# socket timeout
timeout = args.timeout
#########################################################################################
#########################################################################################
#########################################################################################
'''env Variables'''
load_dotenv()
BOT_ID = getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = getenv('TELEGRAM_CHAT_ID')
assert BOT_ID is not None
assert CHAT_ID is not None
#########################################################################################
#########################################################################################
#########################################################################################
'''Logger set'''

class Formatter(logging.Formatter):
    """override logging.Formatter to use an aware datetime object"""
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        tzinfo = timezone
        # change utc time into korean utc time
        return tzinfo.localize(dt)
         
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec='milliseconds')
            except TypeError:
                s = dt.isoformat()
        return s

# create logger(root logger)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)

# initialize the log file when running again
f = logging.FileHandler('log.txt', 'w')
f.setLevel(logging.INFO)

# create formatter
formatter = Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')

# add formatter to ch
ch.setFormatter(formatter)
f.setFormatter(formatter)

# add handlers to logger
logger.addHandler(ch)
logger.addHandler(f)

def measure_execution_time(log_prefix=""):
    """decorators for debugging executing time"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                await func(*args, **kwargs)
            except Exception as e:
                logging.error(f'{log_prefix} An error occurred: {e}')
            finally:
                elapsed_time = time.time() - start_time
                logging.info(f'{log_prefix} Execution Time : {elapsed_time}')
        return wrapper
    return decorator
#########################################################################################
#########################################################################################
#########################################################################################
'''Functions'''

async def binance_kline_tracker_by_socket(symbol, interval, q):
    '''binance real-time async socket handler for tracking the k-line
       trying to capture the moment where the k-line closed especially 
       looking for taker buy volume and number of trades'''

    client = AsyncClient()
    task_done = False

    @measure_execution_time("insert_queue_if_kline_is_closed")
    async def insert_queue_if_kline_is_closed(kline_data):
        nonlocal task_done
        # Check if the kline is closed
        if kline_data['x']:
            timestamp = float(kline_data['T'])
            # Convert UTC time to datetime Object and then adapt user-defined timezone
            time_utc = datetime.datetime.fromtimestamp(timestamp / 1000.0).astimezone(timezone)
            # columns will be ('symbol', 'timestamp', 'value')
            payload = (symbol, time_utc, float(kline_data['Q']))
            key_set = ('symbol', 'timestamp', 'value')
            payload_transformed = {key: [value] for key, value in zip(key_set, payload)}
            # Put the data in the queue
            await q.put(payload_transformed)
            task_done = True
    
    async with BinanceSocketManager(client).kline_futures_socket(symbol, interval) as ts:
        abandon_symbol_yn = False
        while True:
            try:
                # ts.recv() function internally acts as while loop and has timeout exception.(original timeout : 10seconds)
                # if the ticker is not on the future market, it will indefinitely loops and exhaust resources 
                # so, if we are waiting more than 100 seconds for payload, then regard it as unsigned ticker -> abandon
                res = await asyncio.wait_for(ts.recv(), 100)
                kline_data = res['k']
                await insert_queue_if_kline_is_closed(kline_data)

            except BinanceAPIException as e:
                logging.error(f'Binance API exception: {e}', exc_info=True)
            
            # error that wait for more than 100 seconds
            except asyncio.TimeoutError as e:
                abandon_symbol_yn = True
                logging.error(f'TimeoutError : {symbol} - {e}', exc_info=True)

            except Exception as e:
                logging.error(f'General exception: {e}', exc_info=True)

            finally:
                if abandon_symbol_yn:
                    logging.info(f'{symbol} abandoned')
                    try:
                        await client.close_connection()
                    except Exception as e:
                        logging.error(f'error info: {e}')
                        return
                    break

                if task_done:
                    logging.info(f'{symbol} kline closed and binance_kline_tracker_by_socket finish task')
                    try:
                        await client.close_connection()
                    except Exception as e:
                        logging.error(f'error info: {e}')
                        return
                    break

@measure_execution_time("async_write_csv")
async def async_write_csv(symbol, file_path, file : pd.DataFrame):
    loop = asyncio.get_event_loop()
    # for appending without column indices
    try:
        if path.exists(file_path):
            await loop.run_in_executor(None, file.to_csv, file_path, ',', '', None, None, False, None, None, 'a')
        else:
            await loop.run_in_executor(None, file.to_csv, file_path, ',', '', None, None, True, None, None, 'a')
        logging.info(f'{symbol} data appended to taker_volume_dataset file')
    except Exception as e:
        logging.error(f'error info: {e}')

@measure_execution_time("async_read_csv")
async def async_read_csv(file_path):
    loop = asyncio.get_event_loop()
    df = None
    try:
        df = await loop.run_in_executor(None, pd.read_csv, file_path)
    except Exception as e:
        logging.error(f'error info: {e}') 
    df = pd.DataFrame(df)

    assert df is not None
    return df

@measure_execution_time("save_data")
async def save_data(q : asyncio.Queue, timeout):
    for _ in range(q.qsize()):
        try:
            # Get the latest data from the queue with a timeout
            payload = await asyncio.wait_for(q.get(), timeout=timeout)
            symbol = payload['symbol']
            data = pd.DataFrame(payload)
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            await async_write_csv(symbol, file_path, data)
        except Exception as e:
            logging.error(f'error info: {e}')

@measure_execution_time("plot_taker_buy_volume_graph")
async def plot_taker_buy_volume_graph(symbols):
    # Create a Telegram bot
    bot = Bot(token=BOT_ID)
    try:
        df = await async_read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # autodatelocator will interpret the timezone unless setting localization.
        df['timestamp'] = df['timestamp'].dt.tz_localize(None) 

        for symbol, group in df.groupby('symbol'):
            if symbol in symbols:
                fig, ax = plt.subplots()

                # Plot the data
                ax.plot(group['timestamp'], group['value'], label=symbol)

                # format date 
                locator = mdates.AutoDateLocator()
                ax.xaxis.set_major_locator(locator)
                ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(locator))

                # Rotate x-axis labels for better readability
                plt.xticks(rotation=45)
                plt.title('Taker Buy Quote Asset Volume')
                plt.xlabel('Timestamp')
                plt.ylabel('Volume')

                # Display legend
                plt.legend()
                
                # Create a BytesIO buffer to save the Matplotlib plot image in memory
                buffer = io.BytesIO()
                plt.savefig(buffer)

                # moves the file pointer to the beginning of the buffer
                buffer.seek(0)

                # upload from buffer
                buf_trs = types.BufferedInputFile(buffer.getvalue(), filename=f'{symbol}.png')
                ret = await send_telegram(bot, buf_trs)

                plt.close()

    except Exception as e:
        logging.error(f'error info: {e}')
    # finally:
        # Close the bot session
        # await bot.close()

@measure_execution_time("send_telegram")
async def send_telegram(bot : Bot, buffer):
    result = None
    # Send the image to the Telegram chat
    try:
        result = await bot.send_photo(chat_id=CHAT_ID, photo=buffer)
    except Exception as e:
        logging.error(f'error info: {e}')
    return result

@measure_execution_time("measure_real_time_hyped_taker_volume_task")
async def measure_real_time_hyped_taker_volume_task(timedelta, multiplier):
    '''measure if there is a real-time hyped taker volume symbol
       calculated by setting the time begin and multiplier to see if 
       there is a symbol over the standard set'''
    try:
        df = await async_read_csv(file_path) 

        # 1st transform process : apply datetime object to a 'timestamp' column
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Input datetime for reference
        time_now = dt.now(timezone)
        # Calculate the datetime for 'timedelta' minutes ago
        time_begin = pd.to_datetime(time_now - td(minutes=timedelta))
        # For Solving Issue : "Invalid comparison between dtype=datetime64[ns] and datetime"
        filtered_df = df[df['timestamp'].dt.to_pydatetime() >= time_begin]
        # Group by 'symbol' and calculate the mean of the 'value' column multiplied by multiplier
        mean_values = filtered_df.groupby('symbol')['value'].mean() * multiplier
        # Get the most recent datetime data for each symbol
        recent_data = df.groupby('symbol').apply(lambda x: x.iloc[-1])['value']
        # Filtered Series Object
        s = recent_data > mean_values
        logging.debug(f'Series object parmater : {s}')
        await plot_taker_buy_volume_graph(s[s].index.to_list())

    except Exception as e:
        logging.error(f'error info: {e}')

# wrapper function ruins the return coroutine
# @measure_execution_time("async_set_future_symbols")
async def async_set_future_symbols():
    '''get binance future tickers'''
    start_time = time.time()
    client = AsyncClient()
    res = await client.futures_exchange_info()
    symbols = list(map(lambda x : x['symbol'], res['symbols']))
    try:
        await client.close_connection()
    except Exception as e:
        logging.error(f'error info: {e}')
        return
    finally:
        elapsed_time = start_time - time.time()
        logging.info(f'elapsed_time : {elapsed_time}')
    return symbols

async def main():

    if args.function == "binance_future_taker":
        while True:
            symbols = await async_set_future_symbols()
            assert symbols is not None
            # async queue for structure : producer(binance_kline_tracker_by_socket) -> consumer(save_data)
            q = asyncio.Queue()

            # Create tasks for producers and consumers
            producers_tasks = [asyncio.create_task(binance_kline_tracker_by_socket(symbol, interval, q)) for symbol in symbols]

            # Run tasks concurrently
            await asyncio.gather(*producers_tasks)
            # seperate producer and consumer, so no concurrently act -> no need
            # await q.join()

            # asyncio chaining
            await save_data(q, timeout)
            await measure_real_time_hyped_taker_volume_task(timedelta, multiplier)

    else:
        return

if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
