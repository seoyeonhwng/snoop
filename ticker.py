import sys
from datetime import datetime, timedelta
import time
from copy import deepcopy

from data_factory import DataFactory
from manager.db_manager import DbManager


class Ticker:
    def __init__(self):
        self.data_factory = DataFactory()
        self.db_manager = DbManager()

    def run(self, _start_date, _end_date):
        markets = ["KOSPI", "KOSDAQ"]
        _init_start_date = deepcopy(_start_date)
        _start_date = datetime.strptime(_start_date, '%Y%m%d')
        _end_date = datetime.strptime(_end_date, '%Y%m%d')
        while True:
            if _start_date == _end_date:
                break
            for market in markets:
                print(f"[ticker] requesting for {_start_date} / {market}")
                tickers = self.data_factory.get_ticker_info(market, _start_date.strftime('%Y%m%d'))
                self.db_manager.insert_ticker(tickers)
                time.sleep(0.3)
            _start_date = _start_date + timedelta(days=1)
        print(f"[ticker] {_init_start_date} ~ {_end_date.strftime('%Y%m%d')} loaded")


if __name__ == "__main__":
    t = Ticker()

    start_date = sys.argv[1] if len(sys.argv) >= 2 else None
    end_date = sys.argv[2] if len(sys.argv) >= 3 else None
    if (start_date and len(start_date) != 8) or (end_date and len(end_date) != 8):
        print('[WARNING] date SHOULD BE LENGTH OF 8')
        exit(0)
    if end_date and int(start_date) >= int(end_date):
        print('[WARNING] end_date SHOULD BE LATER THAN start_date')
        exit(0)

    t.run(start_date, end_date)
