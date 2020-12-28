import telegram
import datetime
import collections
import urllib
import requests

from manager.db_manager import DbManager
from manager.utils import read_config, get_current_time, REASON_CODE, STOCK_TYPE_CODE
from telegram.ext import Updater, Dispatcher, CommandHandler, ConversationHandler, MessageHandler, Filters

REVERSE_REASON_CODE = {v:k for k, v in REASON_CODE.items()}
REVERSE_STOCK_TYPE_CODE = {v:k for k, v in STOCK_TYPE_CODE.items()}

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

    def get_short_url(self, rcept_no):
        api_url = "http://tinyurl.com/api-create.php?"
        long_url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}'

        resp = requests.get(api_url + urllib.parse.urlencode({'url': long_url}))
        if resp.status_code != 200:
            print('[ERROR] in get_short_url')
            return

        return resp.text

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

    def detail(self, update, context):
        chat_id = update.effective_chat.id
        corp_name = context.args[0]
        target_date = context.args[1] if len(context.args) >= 2 else get_current_time('%Y%m%d', -1)
        # TODO input으로 들어온 날짜 형태를 %Y%m%d로 통일시켜야함

        corp_info = self.db_manager.get_corporate_info(corp_name)
        details = self.db_manager.get_executive_detail(corp_name, target_date)
    
        message = self.generate_message_header(corp_info, target_date)
        message += self.generate_message_body(details)
        context.bot.send_message(chat_id, message)

    def generate_message_header(self, corp_info, target_date):
        message = f'## {target_date} {corp_info["corp_name"]} 변동 내역\n\n'
        message += f'** {corp_info["market"]} {corp_info["market_rank"]}위\n'
        message += f'** 시가총액 {int(corp_info["market_capitalization"]):,}원\n\n\n'
        return message
    
    def generate_message_body(self, data):
        if not data:
            return '해당 날짜에 변동 내역이 없습니다.'

        details = collections.defaultdict(list)
        for d in data:
            details[d['executive_name']].append(d)
        
        message = ''
        for e_name, infos in details.items():
            message += f'[{e_name}] {self.get_short_url(infos[0]["rcept_no"])}\n'
            for info in infos:
                traded_on = info['traded_on'].strftime('%m/%d')
                reason_code = REVERSE_REASON_CODE.get(info['reason_code'])
                stock_type = REVERSE_STOCK_TYPE_CODE.get(info['stock_type'])
                delta = f'▲{info["delta_volume"]:,}' if info["delta_volume"] > 0 else f'▼{-info["delta_volume"]:,}'
                message += f'. {traded_on} | {reason_code} | {stock_type} ({delta}주 / {int(info["unit_price"]):,}원)\n'
            message += '\n\n'
        return message

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
        detail_handler = CommandHandler('detail', self.detail, pass_args=True)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(subscribe_handler)
        dispatcher.add_handler(detail_handler)

        updater.start_polling()

