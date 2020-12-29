import telegram
import datetime
import collections
import urllib
import requests
import re
import threading

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
        if len(context.args) > 2:
            return context.bot.send_message(chat_id, '/detail {회사 이름} {yyyymmdd} 형태로 입력해주세요.')
        
        corp_name = context.args[0]
        target_date = context.args[1] if len(context.args) == 2 else get_current_time('%Y%m%d', -1)
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, '회사 이름을 공백 없이 또는 날짜를 yyyymmdd 형태로 입력해주세요.')
 
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, '해당 회사가 존재하지 않습니다.')

        details = self.db_manager.get_executive_detail(corp_name, target_date)
        message = self.generate_message_header(corp_info, target_date)
        message += self.generate_message_body(details)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()

    def generate_message_header(self, corp_info, target_date):
        target_date = target_date[:4] + '\-' + target_date[4:6] + '\-' + target_date[6:]
        message = f'\#\# {target_date} {corp_info["corp_name"]} 변동 내역\n\n'
        message += f'\*\* {corp_info["market"]} {corp_info["market_rank"]}위\n'
        message += f'\*\* 시가총액 {int(corp_info["market_capitalization"]):,}원\n\n\n'
        return message
    
    def generate_message_body(self, data):
        if not data:
            return r'해당 날짜에 변동 내역이 없습니다\.'

        details = collections.defaultdict(list)
        for d in data:
            details[d['executive_name']].append(d)
        
        message = ''
        for e_name, infos in details.items():
            report_url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={infos[0]["rcept_no"]}'
            message += f'[\[{e_name}\]]({report_url})\n'
            
            for info in infos:
                traded_on = info['traded_on'].strftime('%m/%d').replace('/', '\/')
                reason_code = REVERSE_REASON_CODE.get(info['reason_code'])
                stock_type = REVERSE_STOCK_TYPE_CODE.get(info['stock_type'])
                delta = f'▲{info["delta_volume"]:,}' if info["delta_volume"] > 0 else f'▼{-info["delta_volume"]:,}'
                message += f'\. {traded_on} \| {reason_code} \| {stock_type} \({delta}주 \/ {int(info["unit_price"]):,}원\)\n'
            message += '\n'
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
        detail_handler = CommandHandler(['detail', 'd'], self.detail, pass_args=True)

        dispatcher.add_handler(start_handler)
        dispatcher.add_handler(subscribe_handler)
        dispatcher.add_handler(detail_handler)

        updater.start_polling()
        updater.idle()

