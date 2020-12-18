import telegram
import datetime

from manager.utils import read_config


class TgManager:
    def __init__(self):
        self.cfg = read_config()
        self.token = self.cfg.get("tg_bot_token")
        self.bot = telegram.Bot(token=self.token)
        self.chat_ids = self.cfg.get("tg_bot_chat_ids")

    def send_message(self, tg_msg):
        # for u in self.bot.getUpdates():
        #     print(u)

        tg_msg += f'\n{datetime.datetime.now()}'
        for c in self.chat_ids:
            self.bot.send_message(c, tg_msg, timeout=30)
