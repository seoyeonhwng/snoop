import telegram

from utils.commons import get_current_time
from utils.config import BOT_TOKEN, WARNING_BOT_TOKEN, WATCHDOG_BOT_TOKEN, ADMIN_IDS, TG_CONN_POOL

from telegram.utils.request import Request

MAX_MSG_LENGTH = 4096


class TgManager:
    def __init__(self):
        self.bot = telegram.Bot(token=BOT_TOKEN, request=Request(con_pool_size=TG_CONN_POOL))
        self.watchdog_bot = telegram.Bot(token=WATCHDOG_BOT_TOKEN)
        self.warning_bot = telegram.Bot(token=WARNING_BOT_TOKEN)

    def send_message(self, target, message):
        q = len(message) // MAX_MSG_LENGTH
        if q == 0:
            self.bot.send_message(target, message, timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
            return

        for i in range(q):
            self.bot.send_message(target, message[MAX_MSG_LENGTH*i:MAX_MSG_LENGTH*(i+1)], timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2, disable_web_page_preview=True)
        self.bot.send_message(target, message[MAX_MSG_LENGTH*q:], timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

    def send_all_message(self, targets, message):
        for target in targets:
            self.bot.send_message(target, message, timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

    def send_warning_message(self, message):
        for admin in ADMIN_IDS:
            self.warning_bot.send_message(admin, message, timeout=30, disable_web_page_preview=True)
