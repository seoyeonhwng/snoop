import collections
import telegram
import re
import threading

from manager.db_manager import DbManager
from manager.log_manager import LogManager
from manager.utils import get_current_time, REVERSE_REASON_CODE, REVERSE_STOCK_TYPE_CODE


class Commander:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()

    def get_empty_user_data(self, chat_id, nickname):
        user_data = {
            'chat_id': chat_id,
            'nickname': nickname,
            'role': '02',  # user
            'is_paid': True,
            'is_active': True,
            'created_at': get_current_time(),
            'expired_at': get_current_time(None, 365),
            'canceled_at': None
        }
        return user_data

    def start(self, update, context):
        greeting_msg = "안녕\? 나는 __*스눕*__이라고해\.\n아래 형태로 너의 별명을 알려줘\!\n\n"
        greeting_msg += "💡 \/subscribe \{별명\}\n      \(ex\. \/subscribe 스눕이\)"

        context.bot.send_message(chat_id=update.effective_chat.id, text=greeting_msg, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def subscribe(self, update, context):
        chat_id, nickname = update.effective_chat.id, ''.join(context.args)
        self.logger.info(f'{chat_id} - {nickname}')
        user_info = self.db_manager.get_user_info(chat_id)

        if user_info:
            msg = f'{user_info[0]["nickname"]}!\n우리 이미 친구잖아😊'
            return context.bot.send_message(chat_id=chat_id, text=msg)
      
        if not self.db_manager.is_valid_nickname(nickname):
            msg = '앗! 다른 친구가 이미 사용 중인 별명이야🥺\n다른 별명 없어?\n\n'
            msg += "💡 /subscribe {별명}\n      (ex. /subscribe 스눕이)"
            return context.bot.send_message(chat_id=chat_id, text=msg)

        user_data = self.get_empty_user_data(chat_id, nickname)
        self.db_manager.insert_bulk_row('user', [user_data])
        return context.bot.send_message(chat_id=chat_id, text=f"{nickname}! 만나서 반가워😊 /help") 

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
        message = self.generate_detail_message_header(corp_info, target_date)
        message += self.generate_detail_message_body(details)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()

    def generate_detail_message_header(self, corp_info, target_date):
        target_date = target_date[:4] + '\-' + target_date[4:6] + '\-' + target_date[6:]
        message = f'📈 {target_date} __*{corp_info["corp_name"]}*__ 변동 내역\n\n'
        message += f'✔️ {corp_info["market"]} {corp_info["market_rank"]}위\n'
        message += f'✔️ 시가총액 {int(corp_info["market_capitalization"]):,}원\n\n\n'
        return message
    
    def generate_detail_message_body(self, data):
        if not data:
            return "아쉽게도 알려줄 내용이 없어😭"
           
        details = collections.defaultdict(list)
        for d in data:
            details[d['executive_name']].append(d)
        
        message = ''
        for e_name, infos in details.items():
            report_url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={infos[0]["rcept_no"]}'
            message += f'👨‍💼[{e_name}]({report_url})👩‍💼\n'
            
            for info in infos:
                traded_on = info['traded_on'].strftime('%m/%d').replace('/', '\/')
                reason_code = REVERSE_REASON_CODE.get(info['reason_code'])
                stock_type = REVERSE_STOCK_TYPE_CODE.get(info['stock_type'])
                delta = f'▲{info["delta_volume"]:,}' if info["delta_volume"] > 0 else f'▼{-info["delta_volume"]:,}'
                message += f'• {traded_on} \| {reason_code} \| {stock_type} \({delta}주 \/ {int(info["unit_price"]):,}원\)\n'
            message += '\n'
        return message
