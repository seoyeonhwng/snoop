import sys
import threading
import time
import logging

from datetime import datetime

from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager
from manager.parsing_manager import ParsingManager
from manager.utils import get_current_time

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

NO_DATA_MSG = "조회된 데이터가 없습니다."


class Snoopy:
    def __init__(self):
        self.logger = logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.api_manager = ApiManager()
        self.parsing_manager = ParsingManager()

    def get_executive_data(self, data):
        """
        임원 관련 보고서와 유가, 코스닥 데이터만 가지고 온다.
        """
        f = lambda x: x['report_nm'] == '임원ㆍ주요주주특정증권등소유상황보고서' and x['corp_cls'] in ['Y', 'K']
        return [d for d in data if f(d)]

    def run(self, _start_date=None, _end_date=None):
        if not _start_date:
            _start_date = get_current_time('%Y%m%d', -1)
        if not _end_date:
            _end_date = _start_date

        params = {
            'bgn_de': _start_date,
            'end_de': _end_date,
            'page_count': 100
        }

        response = self.api_manager.get_json('list', params)
        if response['status'] not in ['000', '013']:
            tg_msg = f"[ERROR] status code - {response['status']}"
            self.logger.info(tg_msg)
            threading.Thread(target=self.tg_manager.send_msg, args=(tg_msg,)).start()
            return

        if response['status'] == '013':
            tg_msg = NO_DATA_MSG
            self.logger.info(tg_msg)
            threading.Thread(target=self.tg_manager.send_msg, args=(tg_msg,)).start()
            return

        data, total_page = response['list'], response['total_page']
        for i in range(2, total_page + 1):
            self.logger.info(f"{_start_date} ~ {_end_date}: current page {i} / {total_page}")
            time.sleep(0.5)  # to prevent IP ban

            params['page_no'] = i
            response = self.api_manager.get_json('list', params)
            if response['status'] != '000':
                continue

            data += response['list']

        executive_data = self.get_executive_data(data)
        parsed = self.parsing_manager.parsing(executive_data)

        for rcept, detail in parsed.items():
            stock_detail = []
            for d in detail:
                d['created_at'] = get_current_time()
                stock_detail.append(tuple(d.values()))
            self.logger.info(f"DB insert on {rcept}")
            self.db_manager.insert_executive(stock_detail)


if __name__ == "__main__":
    start_date, end_date = None, None
    if len(sys.argv) == 2:
        start_date = sys.argv[1]
        if len(start_date) != 8:
            print('[WARNING] start_date SHOULD BE LENGTH OF 8')
            exit(0)
    elif len(sys.argv) == 3:
        start_date, end_date = sys.argv[1], sys.argv[2]
        if (len(start_date) != 8) or (len(end_date) != 8):
            print('[WARNING] start_date, end_date SHOULD BE LENGTH OF 8')
            exit(0)
        if int(start_date) >= int(end_date):
            print('[WARNING] end_date SHOULD BE LATER THAN start_date')
            exit(0)

    s = Snoopy()
    s.run(start_date, end_date)
