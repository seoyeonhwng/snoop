import sys
import os
import time
import collections
import threading
import multiprocessing as mp
from datetime import datetime

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager
from manager.utils import get_current_time, read_config

NO_DATA_MSG = '어제는 아쉽게도 알려줄 내용이 없어🥺'


class Snoopy:
    def __init__(self):
        self.logger = LogManager().logger
        self.mode = read_config().get('mode')
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.api_manager = ApiManager()

    def __get_executive_data(self, data):
        """
        임원 관련 보고서와 유가, 코스닥 데이터만 가지고 온다.
        """
        f = lambda x: x.get('report_nm') == '임원ㆍ주요주주특정증권등소유상황보고서' and x.get('corp_cls') in ['Y', 'K']
        return [d for d in data if f(d)]

    def __generate_message(self, data, target_date):
        message = f'💌 굿모닝\! 나는 __*스눕*__이야 \n      어제의 스눕 결과를 알려줄게👀\n\n'
        message += f'✔️ ' + target_date.replace("-", "\/") + ' / KOSPI, KOSDAQ 대상\n'
        message += f'✔️ 순수 장내매수, 장내매도 한정\n'
        message += f'✔️ 공시횟수, 시가총액 내림차순\n\n\n'

        if not data:
            message += f'{NO_DATA_MSG}\n'
            return message

        industry_corporates = collections.defaultdict(list)
        for d in sorted(data, key=lambda data:(data['count'], int(data['market_capitalization'])), reverse=True):
            industry_corporates[d['industry_name']].append(d)

        for industry_name, corps in industry_corporates.items():
            message += f'📌 *{industry_name}*\n'
            for c in corps:
                cap_info = f'_{c["market"]}_ {c["market_rank"]}위'
                corp_name = c["corp_name"].replace('.', '\.')
                message += f'• {corp_name} \({cap_info}\) \- {c["count"]}건\n'
            message += '\n'

        return message

    def send_daily_notice(self, target_date):
        target_date = get_current_time('%Y%m%d', -1) if not target_date else target_date
        target_date = datetime.strptime(target_date.replace('-', ''), '%Y%m%d').strftime('%Y-%m-%d')

        targets = self.db_manager.get_targets()
        targets = set([t.get('chat_id') for t in targets])

        data = self.db_manager.get_disclosure_data(target_date)
        message = self.__generate_message(data, target_date)

        self.tg_manager.send_message(targets, message)

    def run(self):
        self.logger.info('Snoop Bot Started')
        print('[Snoop Bot is Running!]')
        self.tg_manager.run()

    def watchdog(self):
        if self.mode == "dev":
            self.logger.info(f'no logging since {self.mode}')
            return
        while True:
            try:
                self.logger.info('running')
                # TODO. send /help
                time.sleep(10)
            except Exception as e:
                msg = f"[watchdog error] {e}"
                self.logger.critical(msg)
                threading.Thread(target=self.tg_manager.send_warning_message, args=(msg,)).start()
                os._exit(1)


if __name__ == "__main__":
    s = Snoopy()

    command = sys.argv[1]
    if command == 'run':
        mp.set_start_method("fork")
        mp.Process(target=s.run, args=()).start()
        mp.Process(target=s.watchdog, args=()).start()
    elif command == 'send':
        date = sys.argv[2] if len(sys.argv) >= 3 else None
        s.send_daily_notice(date)
    else:
        print('[WARNING] invalid command !! Only [run|send]')
