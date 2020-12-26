import sys
import threading
import time
import logging

import collections

from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager
from manager.dart import Dart
from manager.utils import get_current_time

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

NO_DATA_MSG = "조회된 데이터가 없습니다."
FINISH_MSG = "데이터 로드 성공하였습니다."


class Snoopy:
    def __init__(self):
        self.logger = logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.api_manager = ApiManager()
        self.dart = Dart()

    def __get_executive_data(self, data):
        """
        임원 관련 보고서와 유가, 코스닥 데이터만 가지고 온다.
        """
        f = lambda x: x.get('report_nm') == '임원ㆍ주요주주특정증권등소유상황보고서' and x.get('corp_cls') in ['Y', 'K']
        return [d for d in data if f(d)]

    def __parse_disclosure_data(self):
        data = self.db_manager.get_disclosure_data(get_current_time('%Y-%m-%d', -1))
        disclosure_data = {d.get('rcept_no'):d for d in data}

        company_infos = self.db_manager.get_company_infos()
        company_infos = {c.get('stock_code'):c for c in company_infos}

        parsed_data = collections.defaultdict(int)
        for data in disclosure_data.values():
            company_name = company_infos.get(data.get('stock_code')).get('name')
            parsed_data[company_name] += 1

        return parsed_data

    def __generate_message(self, data):
        message = '## Hi! Im Snoopy :)\n\n'
        message += f'* {get_current_time("%Y.%m.%d", -1)} 기준\n* KOSPI, KOSDAQ 대상\n'
        message += '* 발생횟수 내림차순\n\n'

        if not data:
            message += f'{NO_DATA_MSG}\n'
            return message

        data = sorted(data.items(), key=lambda x:x[1], reverse=True)
        for company_name, count in data:
            message += f'. {company_name} ({count}건)\n'
        return message

    def load_data(self, _start_date=None, _end_date=None):
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
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return

        if response['status'] == '013':
            self.logger.info(NO_DATA_MSG)
            threading.Thread(target=self.tg_manager.send_warning_message, args=(NO_DATA_MSG,)).start()
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

        executive_data = self.__get_executive_data(data)
        parsed = self.dart.parsing(executive_data)

        for rcept, detail in parsed.items():
            stock_detail = []
            for d in detail:
                d['created_at'] = get_current_time()
                stock_detail.append(tuple(d.values()))
            self.logger.info(f"DB insert on {rcept}")
            self.db_manager.insert_executive(stock_detail)

        threading.Thread(target=self.tg_manager.send_warning_message, args=(FINISH_MSG,)).start()

    def send_daily_notice(self):
        targets = self.db_manager.get_targets()
        targets = set([t.get('chat_id') for t in targets])

        parsed_data = self.__parse_disclosure_data()
        message = self.__generate_message(parsed_data)

        self.tg_manager.send_message(targets, message)

    def run(self):
        print('==== RUN ====')
        self.tg_manager.run()


if __name__ == "__main__":
    s = Snoopy()

    command = sys.argv[1]
    if command == 'run':
        s.run()
    elif command == 'send':
        s.send_daily_notice()
    elif command == 'data':
        start_date = sys.argv[2] if len(sys.argv) >= 3 else None
        end_date = sys.argv[3] if len(sys.argv) >= 4 else None
        if (start_date and len(start_date) != 8) or (end_date and len(end_date) != 8):
            print('[WARNING] date SHOULD BE LENGTH OF 8')
            exit(0)
        if end_date and int(start_date) >= int(end_date):
            print('[WARNING] end_date SHOULD BE LATER THAN start_date')
            exit(0)

        s.load_data(start_date, end_date)
    else:
        print('[WARNING] invalid command !! Only [run|send|data]')
