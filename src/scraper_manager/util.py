import requests
import json
from datetime import datetime

# get tickers from db
def get_tickers(offset: int, limit: int):
    r = requests.get(f"https://database.financeapp.lucas.engineering/tickers?offset={offset}&limit={limit}")
    #print(r.status_code)
    l = []
    for tick in r.json():
        l.append(tick["ticker"])
    return l

def get_ticker_count():
    r = requests.get(f"https://database.financeapp.lucas.engineering/tickers/count")
    count = r.json()
    return count

def get_period(ticker: str):
    r = requests.get(f"https://database.financeapp.lucas.engineering/history/last_date?ticker_name={ticker}")
    if r.json():
        print(f"Latest Date for {ticker}: {r.json()}")
        latest_date = datetime.fromisoformat(r.json())
        date_delta = datetime.now() - latest_date
        period = date_delta.days
        print(f"Period set to: {period}d")
        return str(period) + 'd'
    else:
        return None

def get_history(ticker: str, period: str):
    r = requests.get(f"https://yfinance.financeapp.lucas.engineering/history?ticker_name={ticker}&period={period}")
    if r.status_code == 500:
        return "Ticker Not Found", 500
    else:
        return r.json(), r.status_code

def save_batch_history(batch_history):
    r = requests.post("https://database.financeapp.lucas.engineering/history/batch", data = batch_history)
    return r.json(), r.status_code

def transform_payload(payload: dict, ticker_name: str) -> list:
    history_batch = []
    for _date, _price in payload["Open"].items():
        new_history = {
            "price_type": "Open",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    for _date, _price in payload["High"].items():
        new_history = {
            "price_type": "High",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    for _date, _price in payload["Low"].items():
        new_history = {
            "price_type": "Low",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    for _date, _price in payload["Close"].items():
        new_history = {
            "price_type": "Close",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    for _date, _price in payload["Volume"].items():
        new_history = {
            "price_type": "Volume",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    for _date, _price in payload["Dividends"].items():
        new_history = {
            "price_type": "Dividends",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    for _date, _price in payload["Stock Splits"].items():
        new_history = {
            "price_type": "Stock Splits",
            "datetime": _date,
            "price": _price,
            "ticker_name": ticker_name
        }
        history_batch.append(new_history)
    json_object = json.dumps(history_batch) 
    return json_object
#print(get_period("MSFT"))

# https://financeapp.lucas.engineering/tickers?offset=0&limit=100


# check db for latest date

# use api to retrieve the data

# add only the needed data to the db
#save history

