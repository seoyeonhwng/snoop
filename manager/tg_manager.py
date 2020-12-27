import telegram
import datetime

from manager.db_manager import DbManager
from manager.utils import read_config, get_current_time
from telegram.ext import Updater, Dispatcher, CommandHandler, ConversationHandler, MessageHandler, Filters

NICKNAME = 0


class TgManager:
    def __init__(self):
        self.cfg = read_config()
        self.bot = telegram.Bot(token=self.cfg.get("tg_bot_token"))
        self.warning_bot = telegram.Bot(token=self.cfg.get("tg_warning_bot_token"))
        self.db_manager = DbManager()

    def get_empty_user_data(self, chat_id, nickname):
        user_data = {
            'chat_id': chat_id,
            'nickname': nickname,
            'role': '02', # user
            'is_paid': True,
            'is_active': True,
            'created_at': get_current_time(),
            'expired_at': get_current_time(None, 365),
            'canceled_at': None
        }
        return user_data

    def start(self, update, context):
        greeting_msg = "Hi! I'm snoopy.\nIf you want to subscribe me,\nplease enter the command below :)\n\n"
        greeting_msg += "/subscribe {nickname}\n(ex. /subscribe snoopy)"

        context.bot.send_message(chat_id=update.effective_chat.id, text=greeting_msg)

    def subscribe(self, update, context):
        chat_id, nickname = update.effective_chat.id, ''.join(context.args)
        if not self.db_manager.is_valid_chatid(chat_id):
            return context.bot.send_message(chat_id=chat_id, text='이미 구독 중 입니다.')
        if not self.db_manager.is_valid_nickname(nickname):
            return context.bot.send_message(chat_id=chat_id, text=f'{nickname}은 이미 사용 중입니다.')

        user_data = self.get_empty_user_data(chat_id, nickname)
        if self.db_manager.insert_bulk_row('user', user_data):
            context.bot.send_message(chat_id=chat_id, text=f"{nickname}님 구독 완료되었습니다!") 
        else:
            self.send_warning_message(f"{nickname}님 구독 실패하었습니다!")
        
    def send_message(self, targets, message):
        message += '\n\n' + str(get_current_time()).replace('-', '\-').replace('.', '\.')
        for target in targets:
            self.bot.send_message(target, message, timeout=30, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def send_warning_message(self, message):
        admins = self.db_manager.get_admin()
        admins = [d.get('chat_id') for d in admins]

        message += f'\n\n{get_current_time()}'
        for admin in admins:
            self.warning_bot.send_message(admin, message, timeout=30)
  
    def run(self):
        updater = Updater(token=self.cfg.get("tg_bot_token"), use_context=True)
        dispatcher = updater.dispatcher

        start_handler = CommandHandler('start', self.start)
        subscribe_handler = CommandHandler('subscribe', self.subscribe, pass_args=True)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(subscribe_handler)

        updater.start_polling()

