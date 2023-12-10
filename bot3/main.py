import asyncio
import pickle
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
import matplotlib.pyplot as plt
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

load_dotenv()
BOT_ID = getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = getenv('TELEGRAM_CHAT_ID')
assert BOT_ID is not None
assert CHAT_ID is not None

# Queue to communicate between data processing and plotting tasks
# the reason why this getting queue code should not be here is : initiating Queue makes an event loop if it is not executed on coroutine or a callback
# plotting_queue = asyncio.Queue()

# Configure the logging system
logging.basicConfig(level=logging.INFO)

# Create a file handler and set the level to DEBUG
file_handler = logging.FileHandler('logfile.txt', mode='w')

# # Create a console handler and set the level to INFO
console_handler = logging.StreamHandler()

# Create a formatter and attach it to the console handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Get the root logger and add the handlers
logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# console_logger = logging.getLogger('console_logger')
# console_logger.setLevel(logging.INFO)

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

async def process_socket(client, symbol, interval, q):
        client = AsyncClient()
        async with BinanceSocketManager(client).kline_futures_socket(symbol, interval) as ts:
            while True:
                try:
                    res = await ts.recv()
                    kline_data = res['k']

                    # Check if the kline is closed
                    if kline_data['x']:
                        start_time = time.time()    
                        # print(f'{symbol} 1m kline is closed - [start] putting data into the queue')
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

async def append_data_to_file(file_path, file : pd.DataFrame):
    loop = asyncio.get_event_loop()
    # for appending without column indices
    if path.exists(file_path):
        await loop.run_in_executor(None, file.to_csv, file_path, ',', '', None, None, False, None, None, 'a')
    else:
        await loop.run_in_executor(None, file.to_csv, file_path, ',', '', None, None, True, None, None, 'a')

async def async_read_csv(file_path):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, pd.read_csv, file_path)
    df = pd.DataFrame(df)
    elapsed_time = time.time() - start_time
    logging.info(f'read csv file took for {elapsed_time}')
    return df

async def save_data(q, timeout):
    while True: 
        try:
            # Get the latest data from the queue with a timeout
            payload = await asyncio.wait_for(q.get(), timeout=timeout)
            data = pd.DataFrame(payload)
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            await append_data_to_file(file_path, data)
        except Exception as e:
            print(f'error info: {e}')

async def sync_ticker_data_from_server(client : AsyncClient, timePeriod):
    global symbols
    loop = asyncio.get_event_loop()

    while True:
        await asyncio.sleep(timePeriod)
        res = await client.get_exchange_info()
        y = lambda symbol : 'USDT' in symbol
        symbols = list(filter(y, [data['symbol'] for data in res['symbols']]))
        print('[Task Complete] ticker sync')

async def plot_taker_buy_volume_graph(symbols):
    start_time = time.time()
    # Create a Telegram bot and dispatcher
    bot = Bot(token=BOT_ID)
    dp = Dispatcher()
    try:
        df = pd.read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')

        for symbol, group in df.groupby('symbol'):
            if symbol in symbols:
                plt.figure()
                graph = sns.lineplot(data=group, x='timestamp', y='value', label=symbol)

                for ind, label in enumerate(graph.get_xticklabels()):
                    if ind % 3 == 0:  
                        label.set_visible(True)
                        label.set_rotation(45)
                    else:
                        label.set_visible(False)
                
                plt.title('Taker Buy Quote Asset Volume')
                plt.xlabel('Time')
                plt.ylabel('Volume')
                
                # Create a BytesIO buffer to save the Matplotlib plot image in memory
                buffer = io.BytesIO()
                plt.savefig(buffer)
                # moves the file pointer to the beginning of the buffer
                buffer.seek(0)
                # upload from buffer
                buf_trs = types.BufferedInputFile(buffer.getvalue(), filename=f'{symbol}.png')
                await send_telegram(bot, buf_trs)
                plt.close()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        elapsed_time = time.time() - start_time
        logging.info(f'plotting and sending graph to telegram took for {elapsed_time}')
    #     # Close the bot session
    #     await bot.close()

async def send_telegram(bot, buffer):
    # Send the image to the Telegram chat
    await bot.send_photo(chat_id=CHAT_ID, photo=buffer)

async def measure_real_time_hyped_taker_volume_task(timedelta, multiplier, timePeriod):
    while True:
        start_time = time.time()
        try:
            await asyncio.sleep(timePeriod)
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
            
        except Exception as e:
            print(f'error info: {e}')
        finally:
            elapsed_time = time.time() - start_time
            logging.info(f'analyzing and sending telegram hyped coin took for {elapsed_time}')

async def main():
    global symbols
    client = AsyncClient()

    # sync the data with the server when running first time
    res = await client.get_exchange_info()
    y = lambda symbol : 'USDT' in symbol
    symbols = list(filter(y, [data['symbol'] for data in res['symbols']]))
    symbols = symbols[200:300]

    # producer - consumer structure for asynchronous program
    q = asyncio.Queue()

    # Create tasks for producers and consumers
    # ticker_sync_task = asyncio.create_task(sync_ticker_data_from_server(client, timePeriod))
    producers_tasks = [asyncio.create_task(process_socket(client, symbol, interval, q)) for symbol in symbols]
    # if there are more than 1 writer, duplication problem appears. How can I fix it?
    writer_task = [asyncio.create_task(save_data(q, timeout)) for _ in range(1)]
    calculate_task = asyncio.create_task(measure_real_time_hyped_taker_volume_task(timedelta, multiplier, timePeriod))
 
    # Run tasks concurrently
    await asyncio.gather(*producers_tasks, *writer_task, calculate_task)
    await q.join()

    # await client.close_connection()

if __name__ == "__main__":
    # Run the event loop
    asyncio.run(main())
