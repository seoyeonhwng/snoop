import sys
import os
import time
import threading
import multiprocessing as mp

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.api_manager import ApiManager
from manager.tg_manager import TgManager
from manager.mail_manager import MailManager
from manager.msg_manager import MsgManager
from manager.commander import Commander
from utils.commons import get_current_time
from utils.commons import convert_format
from utils.config import BOT_TOKEN
from utils.config import MODE
from utils.config import TG_WORKERS

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


class Snoopy:
    def __init__(self):
        self.logger = LogManager().logger
        self.mode = MODE
        self.db_manager = DbManager()
        self.api_manager = ApiManager()
        self.tg_manager = TgManager()
        self.mail_manager = MailManager()
        self.msg_manager = MsgManager()
        self.commander = Commander()

    def send_daily_notice(self, target_date):
        target_date = get_current_time('%Y%m%d', -1) if not target_date else target_date
        message = self.msg_manager.get_snoop_message(target_date)

        tg_targets = self.db_manager.get_targets()
        tg_targets = set([t.get('chat_id') for t in tg_targets])
        self.logger.info(f'{target_date}/{len(tg_targets)} tg start')
        self.tg_manager.send_all_message(tg_targets, message)
        self.logger.info(f'{target_date}/{len(tg_targets)} tg end')

        mail_title = f'[SNOOP] {convert_format(target_date, "%Y%m%d", "%Y/%m/%d")} 레포트'
        mail_targets = list()
        mail_targets.append({'mail': 'hjh@kia.com'})
        mail_targets.append({'mail': 'neosouler@gmail.com'})
        mail_targets.append({'mail': 'seoyoniee22@gmail.com'})
        self.logger.info(f'{target_date}/{len(mail_targets)} mail start')
        self.mail_manager.send_mail(mail_targets, mail_title, message)
        self.logger.info(f'{target_date}/{len(mail_targets)} mail end')

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
        buy_handler = CommandHandler('buy', self.commander.tg_buy, pass_args=True, run_async=False)
        sell_handler = CommandHandler('sell', self.commander.tg_sell, pass_args=True, run_async=False)
        notice_handler = CommandHandler('notice', self.commander.tg_notice, pass_args=True, run_async=False)
        # TODO
        mail_handler = CommandHandler('mail', self.commander.tg_mail, pass_args=True, run_async=False)

        error_handler = MessageHandler(Filters.text | Filters.command, self.commander.tg_command, run_async=False)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(snoop_handler)
        dispatcher.add_handler(detail_handler)
        dispatcher.add_handler(company_handler)
        dispatcher.add_handler(executive_handler)
        dispatcher.add_handler(whoami_handler)
        dispatcher.add_handler(hi_handler)
        dispatcher.add_handler(help_handler)
        dispatcher.add_handler(feedback_handler)
        dispatcher.add_handler(buy_handler)
        dispatcher.add_handler(sell_handler)
        dispatcher.add_handler(notice_handler)

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
        # mp.Process(target=s.watchdog, args=()).start()
    elif command == 'send':
        date = sys.argv[2] if len(sys.argv) >= 3 else None
        s.send_daily_notice(date)
    else:
        print('[WARNING] invalid command !! Only [run|send]')
