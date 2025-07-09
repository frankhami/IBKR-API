from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import pandas as pd
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, wait
import time


class IBapi(EWrapper, EClient):
    def __init__(self, semaphore):
        EClient.__init__(self, self)
        self.lock = threading.Lock()
        self.semaphore = semaphore
        self.data = []
        self.results = {}
        self.symbol_map = {}

    def historicalData(self, reqId, bar):
        with self.lock:
            symbol = self.symbol_map.get(reqId)
            self.data.append([bar.date, symbol, bar.open, bar.high, bar.low, bar.close, bar.volume])
            self.results[reqId] = True
            self.semaphore.release()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson):
        with self.lock:
            print(f"Error: {reqId}, {errorCode}, {errorString}")
            self.results[reqId] = False
            self.semaphore.release()


def run_loop(app):
    app.run()

def load_symbols():
    # should fix this to be a df and not a list
    column_names = ['Symbol', 'Security Name', 'Market Category', 'Test Issue', 'Financial Status', 'Round Lot Size', 'ETF', 'NextShares']
    nasdaqlisted = pd.read_csv('nasdaqlisted.txt', sep='|', names=column_names)['Symbol'].tolist()

    otherlisted = pd.read_csv('otherlisted.txt', sep='|', usecols=['NASDAQ Symbol', 'Exchange'])

    return nasdaqlisted, otherlisted

def request_nasdaq_historical_data(reqId, symbol, app):
    with app.semaphore:
        time.sleep(4)
        app.results[reqId] = None
        app.symbol_map[reqId] = symbol
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.primaryExchange = "NASDAQ"
        contract.currency = 'USD'
        end_date_time = datetime.datetime.now().strftime("%Y%m%d") + f" 09:30:00" + " US/Eastern"
        duration_str = "1 D"
        bar_size_setting = "5 mins"
        what_to_show = "TRADES"
        use_rth = 1
        format_date = 2
        keep_up_to_date = False

        app.reqHistoricalData(reqId, contract, end_date_time, duration_str, bar_size_setting, what_to_show, use_rth, format_date, keep_up_to_date, [])

def request_other_historical_data(reqId, symbol, exchange, app):
    with app.semaphore:
        time.sleep(4)
        exchange_map = {'A': 'AMEX', 'N': 'NYSE', 'P': 'ARCA', 'Z': 'BATS', 'V': 'IEXG'}
        exchange = exchange_map.get(exchange, None)

        if not exchange:
            return
        
        app.results[reqId] = None
        app.symbol_map[reqId] = symbol
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.primaryExchange = exchange
        contract.currency = 'USD'
        end_date_time = datetime.datetime.now().strftime("%Y%m%d") + f" 09:30:00" + " US/Eastern"
        duration_str = "1 D"
        bar_size_setting = "5 mins"
        what_to_show = "TRADES"
        use_rth = 1
        format_date = 2
        keep_up_to_date = False

        app.reqHistoricalData(reqId, contract, end_date_time, duration_str, bar_size_setting, what_to_show, use_rth, format_date, keep_up_to_date, [])

def save_csv(app):
    df = pd.DataFrame(app.data, columns=['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df.to_csv('historical_data.csv', index=False)

def main():
    nasdaq, other = load_symbols()
    semaphore = threading.Semaphore(15)
    app = IBapi(semaphore)
    app.connect('127.0.0.1', 4001, clientId=1)
    api_thread = threading.Thread(target=run_loop, args=(app,), daemon=True)
    api_thread.start()
    time.sleep(1)

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(request_other_historical_data, i, row['NASDAQ Symbol'], row['Exchange'], app)
                for i, row in other.iterrows()]
        wait(futures)

    while any(result is None for result in app.results.values()):
        time.sleep(0.1)

    app.results.clear()
    app.symbol_map.clear()

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(request_nasdaq_historical_data, i, symbol, app) for i, symbol in enumerate(nasdaq[1:-1])]
        wait(futures)

    while any(result is None for result in app.results.values()):
        time.sleep(0.1)

    app.disconnect()

    save_csv(app)


if __name__ == '__main__':
    main()
