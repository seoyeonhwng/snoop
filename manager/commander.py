import collections
import telegram
import re
import threading
from datetime import datetime

from manager.db_manager import DbManager
from manager.log_manager import LogManager
from manager.utils import get_current_time, REVERSE_REASON_CODE, REVERSE_STOCK_TYPE_CODE

INVALID_USER_MSG = '💵🤲 ...'
INVALID_CMD_MSG = '앗! 다시 말해줄래?\n\n'
NO_DATA_MSG = '아쉽게도 알려줄 내용이 없어🥺'

# JUSTIN = "72309198"


class Commander:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()

    def __get_user_data(self, chat_id, nickname):
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
    
    def __is_valid_user(self, chat_id):
        user_info = self.db_manager.get_user_info(chat_id)
        if user_info and user_info[0]['is_paid'] and user_info[0]['is_active']:
            return True
        return False

    def __get_command_example(self, command):
        if command == 'start':
            return '💡 \/subscribe \{별명\}\n      \(ex\. \/subscribe 스눕이\)'
        if command == 'subscribe':
            return '💡/subscribe {별명}\n      (ex. /subscribe 스눕이)'
        if command == 'd':
            return '💡/d 스눕전자 20201001\n      (날짜가 없으면 어제!)'
        if command == 's':
            return '💡/s 20201001\n      (날짜가 없으면 어제!)'
        if command == 'c':
            return '💡/c 스눕전자 10\n      (개수 없으면 5개!)'
        if command == 'e':
            return '💡/e 스눕전자 황스눕 10\n      (개수 없으면 5개!)'

    def __get_greeting(self):
        current_hour = int(get_current_time('%H'))
        if 0 <= current_hour < 8:
            return r'졸려\.\.\.'
        if 8 <= current_hour < 12:
            return r'굿모닝\!'
        if 12 <= current_hour < 18:
            return r'굿애프터눈\!'
        if 18 <= current_hour < 24:
            return r'굿이브닝\!'

    # def tg_watchdog(self):
    #     return f'안녕 {self.db_manager.get_user_info(JUSTIN)[0]["nickname"]}!\n'

    def tg_start(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}')

        greeting_msg = '안녕\? 나는 __*스눕*__이라고해\.\n아래 형태로 너의 별명을 알려줘\!\n\n'
        greeting_msg += self.__get_command_example('start')

        context.bot.send_message(chat_id=update.effective_chat.id, text=greeting_msg, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def tg_subscribe(self, update, context):
        chat_id, nickname = update.effective_chat.id, ''.join(context.args)
        self.logger.info(f'{chat_id}|{context.args}')

        if not nickname.strip():
            cmd_example = self.__get_command_example('subscribe')
            return context.bot.send_message(chat_id, f'{INVALID_CMD_MSG}{cmd_example}')

        self.logger.info(f'{chat_id} - {nickname}')
        user_info = self.db_manager.get_user_info(chat_id)

        if user_info:
            msg = f'{user_info[0]["nickname"]}!\n우리 이미 친구잖아😊'
            return context.bot.send_message(chat_id=chat_id, text=msg)
      
        if not self.db_manager.is_valid_nickname(nickname):
            msg = '앗! 다른 친구가 이미 사용 중인 별명이야🥺\n다른 별명 없어?\n\n'
            msg += '💡 /subscribe {별명}\n      (ex. /subscribe 스눕이)'
            return context.bot.send_message(chat_id=chat_id, text=msg)

        user_data = self.__get_user_data(chat_id, nickname)
        self.db_manager.insert_bulk_row('user', [user_data])
        return context.bot.send_message(chat_id=chat_id, text=f'{nickname}! 만나서 반가워😊 /help')

    def tg_whoami(self, update, context):
        chat_id = update.message.chat_id
        self.logger.info(f'{chat_id}|{context.args}')

        user_info = self.db_manager.get_user_info(chat_id)
        if not user_info:
            msg = '친구야 별명부터 얘기해줄래?\n\n'
            msg += '💡 /subscribe {별명}\n      (ex. /subscribe 스눕이)'
            return context.bot.send_message(chat_id=chat_id, text=msg)

        if not user_info[0].get('is_paid') or not user_info[0].get('is_active'):
            return context.bot.send_message(chat_id=chat_id, text=INVALID_USER_MSG)

        expired_at = user_info[0]["expired_at"].strftime('%Y%m%d')
        expired_at = expired_at[:4] + '/' + expired_at[4:6] + '/' + expired_at[6:]
        msg = f'안녕 {user_info[0]["nickname"]}!\n'
        msg += f'우리 {expired_at} 까지 사이좋게 지내보자😇'
        return context.bot.send_message(chat_id=chat_id, text=msg)

    def tg_detail(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        cmd_example = self.__get_command_example('d')
        if len(context.args) < 1 or len(context.args) > 2:
            return context.bot.send_message(chat_id, f'{INVALID_CMD_MSG}{cmd_example}')
        
        corp_name = context.args[0]
        target_date = context.args[1] if len(context.args) == 2 else get_current_time('%Y%m%d', -1)
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, f'{INVALID_CMD_MSG}{cmd_example}')
 
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, f'{INVALID_CMD_MSG}{cmd_example}')

        details = self.db_manager.get_executive_detail(corp_name, target_date)
        message = self.__generate_detail_message_header(corp_info[0], target_date)
        message += self.__generate_detail_message_body(details)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()

    def __generate_detail_message_header(self, corp_info, target_date):
        target_date = target_date[:4] + '\/' + target_date[4:6] + '\/' + target_date[6:]
        message = f'📈 {target_date} __*{corp_info["corp_name"]}*__ 변동 내역\n\n'
        message += f'✔️ {corp_info["market"]} {corp_info["market_rank"]}위\n'
        message += f'✔️ 시가총액 {int(corp_info["market_capitalization"]):,}원\n\n\n'
        return message
    
    def __generate_detail_message_body(self, data):
        if not data:
            return NO_DATA_MSG
           
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

    def tg_snoopy(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')
        
        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        cmd_example = self.__get_command_example('s')
        if len(context.args) > 1:
            return context.bot.send_message(chat_id, f'{INVALID_CMD_MSG}{cmd_example}')
        
        target_date = context.args[0] if len(context.args) == 1 else get_current_time('%Y%m%d', -1)
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, f'{INVALID_CMD_MSG}{cmd_example}')
        
        data = self.db_manager.get_disclosure_data(target_date)
        message = self.__generate_snoopy_messsage(data, target_date)
        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()

    def __generate_snoopy_messsage(self, data, target_date):
        target_date = datetime.strptime(target_date.replace('-', ''), '%Y%m%d').strftime('%Y/%m/%d')
        message = f'💌 {self.__get_greeting()} 나는 __*스눕*__이야\n'
        message += f'      ' + target_date.replace("/", "\/") + '의 스눕 결과를 알려줄게👀\n\n'
        message += f'✔️ KOSPI, KOSDAQ 대상\n'
        message += f'✔️ 순수 장내매수, 장내매도 한정\n'
        message += f'✔️ 공시횟수, 시가총액 내림차순\n\n\n'

        if not data:
            message += f'{NO_DATA_MSG}\n'
            return message

        industry_corporates = collections.defaultdict(list)
        for d in sorted(data, key=lambda data:(data['count'], int(data['market_capitalization'])), reverse=True):
            industry_corporates[d['industry_name']].append(d)

        for industry_name, corps in industry_corporates.items():
            message += f'📌 *{industry_name}*\n'
            for c in corps:
                cap_info = f'_{c["market"]}_ {c["market_rank"]}위'
                corp_name = c["corp_name"].replace('.', '\.')
                message += f'• {corp_name} \({cap_info}\) \- {c["count"]}건\n'
            message += '\n'
        return message
