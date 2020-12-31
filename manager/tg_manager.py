import telegram
import datetime
import collections
import urllib
import requests
import re
import threading

from manager.commander import Commander
from manager.db_manager import DbManager
from manager.utils import read_config, get_current_time, REASON_CODE, STOCK_TYPE_CODE
from telegram.ext import Updater, Dispatcher, CommandHandler, ConversationHandler, MessageHandler, Filters


class TgManager:
    def __init__(self):
        self.config = read_config()
        self.bot = telegram.Bot(token=self.config.get("tg_bot_token"))
        self.warning_bot = telegram.Bot(token=self.config.get("tg_warning_bot_token"))
        self.db_manager = DbManager()
        self.commander = Commander()

    def send_message(self, targets, message):
        for target in targets:
            self.bot.send_message(target, message, timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def send_warning_message(self, message):
        message += f'\n\n{get_current_time()}'
        for admin in self.config.get("admin_ids"):
            self.warning_bot.send_message(admin, message, timeout=30)
  
    def run(self):
        updater = Updater(token=self.config.get("tg_bot_token"), use_context=True)
        dispatcher = updater.dispatcher

        start_handler = CommandHandler('start', self.commander.start)
        subscribe_handler = CommandHandler('subscribe', self.commander.subscribe, pass_args=True)
        detail_handler = CommandHandler(['detail', 'd'], self.commander.detail, pass_args=True)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(subscribe_handler)
        dispatcher.add_handler(detail_handler)

        updater.start_polling()
        updater.idle()
