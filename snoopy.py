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

    def __get_signal(self, amount):
        if amount <= 50000000:
            return '❗️'
        if amount <= 100000000:
            return '‼️'
        return '🔥'

    def __generate_message(self, data, target_date):
        message = f'💌 굿모닝\! 나는 __*스눕*__이야 \n      ' + target_date.replace("-", "\/") + '의 스눕 결과를 알려줄게👀\n\n'
        message += '✔️ 장내매수\/매도 \(KOSPI\/KOSDAQ 보통주\)\n'
        message += '✔️ 임원별 총 거래 금액 알림 지표\n'
        message += '      🔥: 1억\~  ‼️: 5천\~1억 ❗: 1천\~5천\n'
        message += '✔️ 특정 회사의 상세 스눕이 궁금하면 👉 \/d\n\n\n'

        if not data:
            message += f'{NO_DATA_MSG}\n'
            return message
  
        rcept_groupby_data = collections.defaultdict(list)
        for d in data: 
            rcept_groupby_data[d['rcept_no']].append(d)
        
        corp_infos, industry_corp_map = {}, collections.defaultdict(set)
        for rcept in rcept_groupby_data.values():
            total_amount = sum([r['delta_volume'] * r['unit_price'] for r in rcept])
            if total_amount < 10000000:
                continue

            corp_code, corp_name, industry_name = rcept[0]['corp_code'], rcept[0]['corp_name'], rcept[0]['industry_name'] 
            industry_corp_map[industry_name].add(corp_code)

            if corp_code not in corp_infos:
                corp_infos[corp_code] = {
                    'corp_name': corp_name.replace('-', '\-').replace('.', '\.'),
                    'count' : 1,
                    'max_total_amount': total_amount
                }
            else:
                corp_infos[corp_code]['count'] += 1
                corp_infos[corp_code]['max_total_amount'] = max(corp_infos[corp_code]['max_total_amount'], total_amount)

        for industry_name in [d['industry_name'] for d in self.db_manager.get_industry_list()]:
            corporates = industry_corp_map.get(industry_name)
            if not corporates:
                continue
            
            message += f'🐮 *{industry_name}*\n'
            for corp in corporates:
                info = corp_infos.get(corp)
                message += f'• {info["corp_name"]}\({info["count"]}건\) {self.__get_signal(info["max_total_amount"])}\n'
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
        error_handler = MessageHandler(Filters.text & ~Filters.command, self.commander.tg_command, run_async=False)

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
