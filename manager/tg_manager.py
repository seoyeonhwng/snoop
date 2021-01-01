import telegram
import datetime
import collections
import urllib
import requests
import re
import threading

from manager.commander import Commander
from manager.utils import read_config, get_current_time, REASON_CODE, STOCK_TYPE_CODE
from telegram.ext import Updater, Dispatcher, CommandHandler, ConversationHandler, MessageHandler, Filters


class TgManager:
    def __init__(self):
        self.config = read_config()
        self.bot = telegram.Bot(token=self.config.get("tg_bot_token"))
        self.watchdog_bot = telegram.Bot(token=self.config.get("tg_watchdog_bot_token"))
        self.warning_bot = telegram.Bot(token=self.config.get("tg_warning_bot_token"))
        self.commander = Commander()

    def send_message(self, targets, message):
        for target in targets:
            self.bot.send_message(target, message, timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    # def send_watchdog_message(self):
    #     for admin in self.config.get("admin_ids"):
    #         self.watchdog_bot.send_message(admin, self.commander.tg_watchdog(), timeout=30)

    def send_warning_message(self, message):
        message += f'\n\n{get_current_time()}'
        for admin in self.config.get("admin_ids"):
            self.warning_bot.send_message(admin, message, timeout=30)
  
    def run(self):
        updater = Updater(token=self.config.get("tg_bot_token"), use_context=True)
        dispatcher = updater.dispatcher

        start_handler = CommandHandler('start', self.commander.tg_start)
        hi_handler = CommandHandler('hi', self.commander.tg_hi, pass_args=True)
        help_handler = CommandHandler(['help', 'h'], self.commander.tg_help, pass_args=True)
        whoami_handler = CommandHandler(['whoami', 'w'], self.commander.tg_whoami, pass_args=True)
        detail_handler = CommandHandler(['detail', 'd'], self.commander.tg_detail, pass_args=True)
        snoop_handler = CommandHandler(['snoop', 's'], self.commander.tg_snoopy, pass_args=True)
        company_handler = CommandHandler(['company', 'c'], self.commander.tg_company, pass_args=True)
        executive_handler = CommandHandler(['executive', 'e'], self.commander.tg_executive, pass_args=True)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(hi_handler)
        dispatcher.add_handler(help_handler)
        dispatcher.add_handler(whoami_handler)
        dispatcher.add_handler(detail_handler)
        dispatcher.add_handler(snoop_handler)
        dispatcher.add_handler(company_handler)
        dispatcher.add_handler(executive_handler)

        error_handler = MessageHandler(~Filters.regex(r'\/[start|snoop|s|detail|d|company|c|executive|e|whoami|w|hi|help|h]'), 
                                       self.commander.tg_help)
        dispatcher.add_handler(error_handler)

        updater.start_polling()
        updater.idle()
