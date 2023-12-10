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
import seaborn as sns
import io
import logging
import time
import functools

# From Binance API

# WebSocket connections have a limit of 10 incoming messages per second.
# A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.
# A single connection can listen to a maximum of 200 streams.
# A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark

# if linux server time is not synced with the real time, then executes this
# sudo hwclock -s

# Turn off interactive mode -> do not display automatically
plt.ioff()

# get data from server and sync continously for each 1 hour.
symbols = []

# mean of the taker buy volume value by symbols
mean = {}

# for time frame replacing to seoul standard
# seoul : GMT + 9
utc_timezone = pytz.timezone('UTC')
korean_timezone = pytz.timezone('Asia/Seoul')

# saved file path
file_path = './taker_volume_dataset.csv'

# kline interval variable 
# default 5m
interval = '5m'

# timedelta, multiplier for measuring the mean taker buy volume
timedelta = 60
multiplier = 5

# timePeriod and timeout for syncing ticker, and saving file
timePeriod = 300
timeout = 300

# measuring when producer counts(socket processing task) == consumer counts(writing task)
producer_cnt = 0
consumer_cnt = 0

load_dotenv()
BOT_ID = getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = getenv('TELEGRAM_CHAT_ID')
assert BOT_ID is not None
assert CHAT_ID is not None

# Queue to communicate between data processing and plotting tasks
# the reason why this getting queue code should not be here is : initiating Queue makes an event loop if it is not executed on coroutine or a callback
# plotting_queue = asyncio.Queue()

class Formatter(logging.Formatter):
    """override logging.Formatter to use an aware datetime object"""
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        tzinfo = pytz.timezone('Asia/Seoul')
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

# create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formattera
formatter = Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')
# add formatter to ch
ch.setFormatter(formatter)

# initialize the log file when running again
f = logging.FileHandler('log.txt', 'w')
f.setLevel(logging.INFO)

f.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(f)


def measure_execution_time(log_prefix=""):
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

async def process_socket(symbol, interval, q):
        global producer_cnt
        client = AsyncClient()
        async with BinanceSocketManager(client).kline_futures_socket(symbol, interval) as ts:
            while True:
                try:
                    res = await ts.recv()
                    kline_data = res['k']

                    # Check if the kline is closed
                    if kline_data['x']:
                        start_time = time.time()    
                        timestamp = float(kline_data['T'])
                        time_utc = datetime.datetime.utcfromtimestamp(timestamp / 1000.0)
                        # Convert UTC time to dateTime Object -> Convert to Korean UTC time
                        time_korean_utc = utc_timezone.localize(time_utc).astimezone(korean_timezone)
                        # columns will be ('symbol', 'timestamp', 'value')
                        payload = (symbol, time_korean_utc, float(kline_data['Q']))
                        key_set = ('symbol', 'timestamp', 'value')
                        payload_transformed = {key: [value] for key, value in zip(key_set, payload)}
                        # Put the data in the queue
                        await q.put(payload_transformed)
                        producer_cnt += 1

                        # depends on the kline type. default is 1m.
                        # don't know why the error throws when insert this code.
                        # the error info : "general exception : 'k'"
                        # await asyncio.sleep(58)

                        elapsed_time = time.time() - start_time
                        logging.info(f"Socket processing for {symbol} took {elapsed_time} seconds")

                except BinanceAPIException as e:
                    logging.error(f'Binance API exception: {e}', exc_info=True)

                except Exception as e:
                    logging.error(f'General exception: {e}', exc_info=True)

                finally:
                    global symbols
                    # binance can discard the symbol -> abandon the discarded symbol socket
                    if symbol not in symbols:
                        logging.info(f'{symbol} abandoned')
                        break

async def append_data_to_file(symbol, file_path, file : pd.DataFrame):
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

async def async_read_csv(file_path):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    df = None
    try:
        df = await loop.run_in_executor(None, pd.read_csv, file_path)
        elapsed_time = time.time() - start_time
        logging.info(f'read csv file took for {elapsed_time}')
    except Exception as e:
        logging.error(f'error info: {e}') 
    df = pd.DataFrame(df)

    assert df is not None
    return df

async def save_data(q, timeout):
    global consumer_cnt
    while True: 
        try:
            # Get the latest data from the queue with a timeout
            payload = await asyncio.wait_for(q.get(), timeout=timeout)
            symbol = payload['symbol']
            data = pd.DataFrame(payload)
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            await append_data_to_file(symbol, file_path, data)
            consumer_cnt += 1
        except Exception as e:
            logging.error(f'error info: {e}')

async def sync_ticker_data_from_server(client : AsyncClient, timePeriod):
    global symbols

    while True:
        await asyncio.sleep(timePeriod)
        res = await client.get_exchange_info()
        y = lambda symbol : 'USDT' in symbol
        symbols = list(filter(y, [data['symbol'] for data in res['symbols']]))

async def plot_taker_buy_volume_graph(symbols):
    start_time = time.time()
    # Create a Telegram bot and dispatcher
    bot = Bot(token=BOT_ID)
    dp = Dispatcher()
    try:
        df = await async_read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')

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

                elapsed_time = time.time() - start_time
                if ret is not None:
                    logging.info(f'plotting and sending graph to telegram took for {elapsed_time}')

    except Exception as e:
        logging.error(f'error info: {e}')
    # finally:
        # Close the bot session
        # await bot.close()

async def send_telegram(bot : Bot, buffer):
    result = None
    # Send the image to the Telegram chat
    try:
        result = await bot.send_photo(chat_id=CHAT_ID, photo=buffer)
    except Exception as e:
        logging.error(f'error info: {e}')
    return result

async def measure_real_time_hyped_taker_volume_task(timedelta, multiplier):
    '''measure if there is a real-time hyped taker volume symbol as soon as 
       one cycle of producer and consumer tasks pipeline done '''
    global producer_cnt, consumer_cnt
    while True:
        try:
            if producer_cnt != 0 and consumer_cnt != 0 and producer_cnt == consumer_cnt:
                producer_cnt = 0
                consumer_cnt = 0

                start_time = time.time() 

                df = await async_read_csv(file_path) 
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                # Input datetime for reference
                time_now = dt.now(korean_timezone)
                # Calculate the datetime for 'timedelta' minutes ago
                time_begin = time_now - td(minutes=timedelta)
                filtered_df = df[df['timestamp'] >= time_begin]
                # Group by 'symbol' and calculate the mean of the 'value' column
                # Series Object
                mean_values = filtered_df.groupby('symbol')['value'].mean() * multiplier
                # Get the most recent datetime data for each symbol
                # Series Object
                recent_data = df.groupby('symbol').apply(lambda x: x.iloc[-1])['value']
                # Filtered Series Object
                s = recent_data > mean_values
                await plot_taker_buy_volume_graph(s[s].index.to_list())

                elapsed_time = time.time() - start_time
                logging.info(f'measure_real_time_hyped_taker_volume_task took for {elapsed_time}')
            else:
                await asyncio.sleep(1)            

        except Exception as e:
            logging.error(f'error info: {e}')

async def main():
    global symbols
    client = AsyncClient()

    # sync the data with the server when running first time
    res = await client.get_exchange_info()
    y = lambda symbol : 'USDT' in symbol
    symbols = list(filter(y, [data['symbol'] for data in res['symbols']]))
    symbols = symbols[:]

    try:
        await client.close_connection()
    except Exception as e:
        logging.error(f'error info: {e}')
        return

    # producer - consumer structure for asynchronous program
    # process socket -> queue for writing to a file 
    q = asyncio.Queue()

    # Create tasks for producers and consumers
    # ticker_sync_task = asyncio.create_task(sync_ticker_data_from_server(client, timePeriod))
    producers_tasks = [asyncio.create_task(process_socket(symbol, interval, q)) for symbol in symbols]
    # if there is more than 1 writer, duplication problem appears. How can I fix it? and if it is possible, then is it more efficient to add more writer tasks?
    writer_task = asyncio.create_task(save_data(q, timeout))
    calculate_task = asyncio.create_task(measure_real_time_hyped_taker_volume_task(timedelta, multiplier))
 
    # Run tasks concurrently
    await asyncio.gather(*producers_tasks, writer_task, calculate_task)
    await q.join()

    # await client.close_connection()

if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
