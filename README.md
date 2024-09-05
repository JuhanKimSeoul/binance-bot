# binance-bot

simple app of real-time listening to binance websocket server using binance provided package
and use matplotlib packages to send the useful information by telegram when certain event occurs.

## Features

- ✅ parse the payload and check if the market taker volume is higher with some conditions. If the condition meets, then make an image and send it to registered chat room of telegram.

- ☑️ improve it by allowing dynamic changing strategy. This can be possible when making webapp and making the original logic into a part of services.
- ☑️ make a small redis cache into the app to store the temporary information like ticker, taker buy volume, taker sell volume, volume...
- ☑️ the webapp page just need a textbox which receives the input of sql to dynamically changing the strategy of when to make and send the alarm by telegram.
- ☑️ the webapp page also need a textbox and register and delete button for receiving telegram key and chat room id to dynamically registering and deleting the user who will receive the alarm.

## Installation

To install this project, clone the repository and run the following command:

```bash
git clone https://github.com/JuhanKimSeoul/binance-bot.git
cd your-repo

python3 -m venv .venv

python3 -m pip install -r requirements.txt

python3 main.py
```
