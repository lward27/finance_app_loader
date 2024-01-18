import requests
import json
from datetime import datetime, timedelta

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
        todays_date = datetime.now()
        period = count_weekdays(latest_date, todays_date)
        print(f"Period set to: {period}d")
        return (str(period) + 'd'), latest_date
    else:
        return None, None

# Note - this count will still include holidays, in order to handle holidays, an additional date check
# happens when transforming the payload.
def count_weekdays(start_date, end_date):
    number_of_days = (end_date - start_date).days
    number_of_weekdays = 0
    for i in range(number_of_days):
        date_to_check = start_date + timedelta(days=(i+1))
        if(date_to_check.isoweekday() <= 5):
            number_of_weekdays += 1
    return number_of_weekdays

def get_history(ticker: str, period: str):
    r = requests.get(f"https://yfinance.financeapp.lucas.engineering/history?ticker_name={ticker}&period={period}")
    if r.status_code == 500:
        return "Ticker Not Found", 500
    else:
        return r.json(), r.status_code

def save_batch_history(batch_history):
    r = requests.post("https://database.financeapp.lucas.engineering/history/batch", data = batch_history)
    return r.json(), r.status_code

#TODO: removing the timezone offset in the way that I did made the date comparison false... 
# if the date is the same, removing the timezone offset makes it different....
# for instance: 
def transform_payload(payload: dict, ticker_name: str, latest_date) -> list:
    history_batch = []
    latest_date = latest_date.replace(hour=0)

    for _date, _price in payload["Open"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
            new_history = {
                "price_type": "Open",
                "datetime": _date,
                "price": _price,
                "ticker_name": ticker_name
            }
            history_batch.append(new_history)
    for _date, _price in payload["High"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
            new_history = {
                "price_type": "High",
                "datetime": _date,
                "price": _price,
                "ticker_name": ticker_name
            }
            history_batch.append(new_history)
    for _date, _price in payload["Low"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
            new_history = {
                "price_type": "Low",
                "datetime": _date,
                "price": _price,
                "ticker_name": ticker_name
            }
            history_batch.append(new_history)
    for _date, _price in payload["Close"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
            new_history = {
                "price_type": "Close",
                "datetime": _date,
                "price": _price,
                "ticker_name": ticker_name
            }
            history_batch.append(new_history)
    for _date, _price in payload["Volume"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
            new_history = {
                "price_type": "Volume",
                "datetime": _date,
                "price": _price,
                "ticker_name": ticker_name
            }
            history_batch.append(new_history)
    for _date, _price in payload["Dividends"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
            new_history = {
                "price_type": "Dividends",
                "datetime": _date,
                "price": _price,
                "ticker_name": ticker_name
            }
            history_batch.append(new_history)
    for _date, _price in payload["Stock Splits"].items():
        if(datetime.fromisoformat((_date[:19])) > latest_date):
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

