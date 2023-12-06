from scraper_manager import util
import time


def main():
    total_number_of_tickers = util.get_ticker_count()
    print(f"Total number of tickers to scrape: {total_number_of_tickers}")
    start_ticker = 0 # starting position
    print(f"Starting at {start_ticker}")
    period = "max" # 10 years!
    print(f"Default behavior is max history for new tickers")

    while start_ticker < total_number_of_tickers:
        # Grab 10 tickers at a time
        tickers = util.get_tickers(start_ticker, 10)
        print("\n")
        print(f"Processing Batch: {tickers}")
        start_ticker += 10

        # Process them with a small sleep interval between - handle yfinance issues...
        for ticker in tickers:
            check_period = util.get_period(ticker)
            if check_period == None: #skip this one, it's borked!
                print(f"{ticker} returns no period - probably not in yfinance system, skipping!")
                continue
            if check_period == '0d': #skip this one, it's already been updated!
                print(f"{ticker} returns 0d period - most have already been updated today ;)")
                continue
            period = check_period
            payload, response_code = util.get_history(ticker, period)
            if response_code == 200:
                if payload == "Ticker Not Found":
                    print(f"{ticker}: Not Found (but has been found before... hmm?)")
                    #TODO: delete ticker
                else:
                    transformed_payload = util.transform_payload(payload, ticker)
                    #print(transformed_payload)
                    message, status_code = util.save_batch_history(transformed_payload)
                    if status_code == 201:
                        print(f"{ticker}: successfully saved - period: {period}")
                    else:
                        print(f"{ticker}: Something went wrong saving to the database.")
            else:
                print(f"{ticker}: Something went wrong scraping YFinance.")
            time.sleep(5)

if __name__ == "__main__":
    main()