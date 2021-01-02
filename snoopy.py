import sys
import os
import time
import collections
import threading
import multiprocessing as mp
from datetime import datetime

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.api_manager import ApiManager
from manager.tg_manager import TgManager
from manager.commander import Commander
from utils.commons import get_current_time
from utils.config import BOT_TOKEN
from utils.config import MODE
from utils.config import TG_WORKERS

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


NO_DATA_MSG = '어제는 아쉽게도 알려줄 내용이 없어🥺'


class Snoopy:
    def __init__(self):
        self.logger = LogManager().logger
        self.mode = MODE
        self.db_manager = DbManager()
        self.api_manager = ApiManager()
        self.tg_manager = TgManager()
        self.commander = Commander()

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

        self.logger.info(f'{target_date}/{len(targets)} start')
        self.tg_manager.send_all_message(targets, message)
        self.logger.info(f'{target_date}/{len(targets)} end')

    def run(self):
        self.logger.info('Snoop Bot Started')

        updater = Updater(token=BOT_TOKEN, use_context=True, workers=TG_WORKERS)
        dispatcher = updater.dispatcher

        start_handler = CommandHandler('start', self.commander.tg_start, run_async=False)
        snoop_handler = CommandHandler(['snoop', 's'], self.commander.tg_snoop, pass_args=True, run_async=False)
        detail_handler = CommandHandler(['detail', 'd'], self.commander.tg_detail, pass_args=True, run_async=False)
        company_handler = CommandHandler(['company', 'c'], self.commander.tg_company, pass_args=True, run_async=False)
        executive_handler = CommandHandler(['executive', 'e'], self.commander.tg_executive, pass_args=True, run_async=False)
        whoami_handler = CommandHandler(['whoami', 'w'], self.commander.tg_whoami, pass_args=True, run_async=False)
        hi_handler = CommandHandler('hi', self.commander.tg_hi, pass_args=True, run_async=False)
        help_handler = CommandHandler(['help', 'h'], self.commander.tg_help, pass_args=True, run_async=False)
        feedback_handler = CommandHandler(['feedback', 'f'], self.commander.tg_feedback, pass_args=True, run_async=False)
        error_handler = MessageHandler(Filters.text & ~Filters.command, self.commander.tg_help, run_async=False)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(snoop_handler)
        dispatcher.add_handler(detail_handler)
        dispatcher.add_handler(company_handler)
        dispatcher.add_handler(executive_handler)
        dispatcher.add_handler(whoami_handler)
        dispatcher.add_handler(hi_handler)
        dispatcher.add_handler(help_handler)
        dispatcher.add_handler(feedback_handler)

        dispatcher.add_handler(error_handler)

        updater.start_polling()
        updater.idle()

    def watchdog(self):
        if self.mode == "dev":
            self.logger.info(f'no logging since {self.mode}')
            return
        while True:
            try:
                self.logger.debug('running')
                time.sleep(30)
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
