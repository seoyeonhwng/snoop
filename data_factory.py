import threading
from collections import defaultdict

from utils.commons import get_current_time
from utils.config import MINIMUM_TOTAL_AMOUNT
from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.dart import Dart
from manager.naver import Naver
from manager.krx import Krx

NO_DATA_MSG = "{target_date}에는 거래내역 없습니다."


class DataFactory:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.dart = Dart()
        self.naver = Naver()
        self.krx = Krx()

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

    def insert_corp_frequency(self, end_date):
        start_date = self.db_manager.get_last_business_date(end_date, 6)
        data = self.db_manager.get_disclosure_data(start_date, end_date)

        groupby_rcept = defaultdict(list)
        for d in data:
            groupby_rcept[d['rcept_no']].append(d)
        
        corp_frequency = defaultdict(set)
        for rcept in groupby_rcept.values():
            total_amount = abs(sum([r['delta_volume'] * r['unit_price'] for r in rcept]))
            if total_amount < MINIMUM_TOTAL_AMOUNT:
                continue

            stock_code, disclosed_on = rcept[0]['stock_code'], rcept[0]['disclosed_on']
            corp_frequency[stock_code].add(disclosed_on)

        frequency_data = [{'business_date': end_date, 'period': 'W', 'stock_code': k, 'count': len(v), 'created_at': get_current_time()} for k, v in corp_frequency.items()]
        self.db_manager.insert_bulk_row('frequency', frequency_data)

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

        tickers = self.krx.get_ticker_info(target_date)  # check if market opened
        if not tickers:
            tg_msg += f"\n\n{target_date} Partially Loaded:)"
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return

        # [step3] calculate the number of apperances by company (7days)
        self.insert_corp_frequency(target_date)
        step3_msg = "[step3] calculate_corp_frequency"
        tg_msg += f"{step3_msg}\n"
        self.logger.info(f"{step3_msg}")

        # [step4] bulk insert ticker
        markets = ["KOSPI", "KOSDAQ"]
        for market in markets:
            tickers = self.krx.get_ticker_info(target_date, market)
            if not self.db_manager.insert_bulk_row('ticker', tickers):
                return
        step4_msg = "[step4] bulk insert ticker"
        tg_msg += f"{step4_msg}\n"
        self.logger.info(f"{step4_msg}")

        # [step5] bulk insert corporate
        self.db_manager.unvalidate_corporates()
        corporates = self.dart.build_corporate_list(self.get_empty_corporate())  # from dart
        corporates = self.naver.fill_industry_corporate(corporates)  # from naver
        corporates = self.fill_ticker_corporate(corporates, target_date)  # from ticker
        self.db_manager.update_or_insert_corporate([tuple(c.values()) for c in corporates])
        step5_msg = "[step5] bulk insert corporate"
        tg_msg += f"{step5_msg}\n"
        self.logger.info(f"{step5_msg}")

        tg_msg += f"\n\n{target_date} Fully Loaded:)"
        threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()


if __name__ == "__main__":
    d = DataFactory()
    d.run()
