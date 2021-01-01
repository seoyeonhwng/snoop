import threading

from pykrx import stock
from manager.utils import get_current_time
from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.dart import Dart
from manager.naver import Naver

NO_DATA_MSG = "{target_date}에는 거래내역 없습니다."


class DataFactory:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.dart = Dart()
        self.naver = Naver()

    def get_empty_ticker(self, k, v, _market, _target_date, _market_rank):
        ticker = {
            'stock_code': k,
            'business_date': _target_date,
            'open': v.get('open'),
            'high': v.get('high'),
            'low': v.get('low'),
            'close': v.get('close'),
            'volume': v.get('volume'),
            'quote_volume': v.get('quote_volume'),
            'market_capitalization': v.get('market_capitalization'),
            'market': _market,
            'market_rank': _market_rank,
            'market_ratio': v.get('market_ratio'),
            'operating_share': v.get('operating_share'),
            'created_at': get_current_time(),
        }
        return ticker

    def get_empty_corporate(self):
        corporate = {
            'stock_code': '',
            'corp_code': '',
            'corp_name': '',
            'corp_shorten_name': '',
            'industry_code': '',
            'is_validated': False,
            'market': '',
            'market_capitalization': '',
            'market_rank': 0,
            'updated_at': get_current_time()
        }
        return corporate

    def get_ticker_info(self, _market, _target_date):
        ticker_df = stock.get_market_ohlcv_by_ticker(_target_date, _market)
        ticker_df = ticker_df.rename({'종목명': 'corp_name', '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close',
                                      '거래량': 'volume', '거래대금': 'quote_volume', '시가총액': 'market_capitalization',
                                      '시총비중': 'market_ratio', '상장주식수': 'operating_share'}, axis='columns')
        tickers = ticker_df.sort_values(by='market_capitalization', ascending=False).to_dict('index').items()
        ticker_info = []
        for i, (k, v) in enumerate(tickers):
            ticker_info.append(self.get_empty_ticker(k, v, _market, _target_date, i+1))
        return ticker_info

    def fill_ticker_corporate(self, _corporates, _target_date):
        ticker = {d.get('stock_code'): d for d in self.db_manager.select_ticker_info(_target_date)}

        for c in _corporates:
            tmp = ticker.get(c.get('stock_code'))
            if tmp:  # only check 'is_validated'
                c['is_validated'] = True
                c['market'] = tmp.get('market')
                c['market_capitalization'] = tmp.get('market_capitalization')
                c['market_rank'] = tmp.get('market_rank')
        return _corporates

    def run(self):
        target_date = get_current_time('%Y%m%d')
        self.logger.info(f"{target_date} Data Factory Start!")

        tg_msg = f"💌 DATA FACTORY\n\n"

        # [step1] update_industry from naver
        self.naver.update_industry()
        step1_msg = "[step1] update_industry from naver"
        tg_msg += f"{step1_msg}\n"
        self.logger.info(f"{step1_msg}")

        # [step2] bulk insert executive
        self.dart.insert_executive(target_date)
        step2_msg = "[step2] bulk insert executive"
        tg_msg += f"{step2_msg}\n"
        self.logger.info(f"{step2_msg}")

        tickers = stock.get_market_ticker_list(target_date)
        if not tickers:
            tg_msg += f"\n\n{target_date} Partially Loaded:)"
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return

        # [step3] bulk insert ticker
        markets = ["KOSPI", "KOSDAQ"]
        for market in markets:
            tickers = self.get_ticker_info(market, target_date)
            if not self.db_manager.insert_bulk_row('ticker', tickers):
                return
        step3_msg = "[step3] bulk insert ticker"
        tg_msg += f"{step3_msg}\n"
        self.logger.info(f"{step3_msg}")

        # [step4] bulk insert corporate
        self.db_manager.unvalidate_corporates()
        corporates = self.dart.build_corporate_list(self.get_empty_corporate())  # from dart
        corporates = self.naver.fill_industry_corporate(corporates)  # from naver
        corporates = self.fill_ticker_corporate(corporates, target_date)  # from ticker
        self.db_manager.update_or_insert_corporate([tuple(c.values()) for c in corporates])
        step4_msg = "[step4] bulk insert corporate"
        tg_msg += f"{step4_msg}\n"
        self.logger.info(f"{step4_msg}")

        tg_msg += f"\n\n{target_date} Fully Loaded:)"
        threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()


if __name__ == "__main__":
    d = DataFactory()
    d.run()
