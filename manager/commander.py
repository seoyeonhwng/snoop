import collections
import telegram
import re
import threading
from datetime import datetime

from manager.db_manager import DbManager
from manager.log_manager import LogManager
from manager.utils import get_current_time, REVERSE_REASON_CODE, REVERSE_STOCK_TYPE_CODE

INVALID_USER_MSG = '💵🤲 ...'
NO_DATA_MSG = '아쉽게도 알려줄 내용이 없어🥺'


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

    def __get_possible_error(self, command):
        msg = '✔️ 혹시 명령어만 입력하지는 않았어?\n✔️ 단어 사이 띄어쓰기는 잘했어?\n'
        if command == 'hi':
            return msg + '✔️ 별명 입력 잘했어?\n\n'
        if command == 'd':
            return msg + '✔️ 메시지에 있는 회사명으로 입력했어?\n✔️ 날짜 형식은 올바르게 적었어?\n\n'
        if command == 's':
            return msg + '✔️ 날짜 형식은 올바르게 적었어?\n\n'
        if command == 'c':
            return msg + '✔️ 메시지에 있는 회사명으로 입력했어?\n✔️ 개수는 숫자로 입력했어?\n\n'
        if command == 'e':
            return msg + '✔️ 메시지에 있는 회사명으로 입력했어?\n ✔️ 개수는 숫자로 입력했어?\n\n'

    def __get_cmd_description(self, command):
        if command == 'hi':
            return '🔔 /hi 는 회원 가입하는 기능이야!\n\n'
        if command == 's':
            return '🔔 /s 는 특정 날짜의 스눕 결과를\n      알려주는 기능이야!\n\n'
        if command == 'd':
            return '🔔 /d 는 특정 회사의 상세 스눕 결과를\n      알려주는 기능이야!\n\n'
        if command == 'c':
            return '🔔 /c 는 특정 회사의 최근 스눕 결과를\n      알려주는 기능이야!\n\n'
        if command == 'e':
            return '🔔 /e 는 특정 임원의 최근 스눕 결과를\n      알려주는 기능이야!\n\n'

    def __get_cmd_example(self, command):
        if command == 'start':
            return '💡 \/hi \[별명\]\n      \- 예\) \/hi 스눕이'
        if command == 'hi':
            return '💡 /hi [별명]\n      - 예) /hi 스눕이\n\n'
        if command == 'd':
            return '💡 /d [회사명] [날짜]\n      - 예) /d 스눕전자 20201001\n      - 날짜가 없으면 어제꺼!\n\n'
        if command == 's':
            return '💡 /s [날짜]\n      - 예) /s 20201001\n\n'
        if command == 'c':
            return '💡 /c [회사명] [개수]\n      - 예) /c 스눕전자 10\n      - 개수 없으면 5개!\n\n'
        if command == 'e':
            return '💡 /e [회사명] [임원이름] [개수]\n      - 예) /e 스눕전자 황스눕 10\n      - 개수 없으면 5개!\n\n'

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

    def tg_start(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}')

        greeting_msg = '안녕\? 나는 __*스눕*__이라고해\.\n아래 형태로 너의 별명을 알려줘\!\n\n'
        greeting_msg += self.__get_cmd_example('start')

        context.bot.send_message(chat_id=update.effective_chat.id, text=greeting_msg, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def tg_hi(self, update, context):
        chat_id, nickname = update.effective_chat.id, ''.join(context.args)
        self.logger.info(f'{chat_id} | {context.args}')

        invalid_cmd_msg = f'{self.__get_cmd_description("hi")}{self.__get_cmd_example("hi")}{self.__get_possible_error("hi")}'
        if not nickname.strip():
            return context.bot.send_message(chat_id, invalid_cmd_msg)

        user_info = self.db_manager.get_user_info(chat_id)
        if user_info:
            msg = f'{user_info[0]["nickname"]}!\n우리 이미 친구잖아😊'
            return context.bot.send_message(chat_id=chat_id, text=msg)
      
        if not self.db_manager.is_valid_nickname(nickname):
            msg = f'앗! 다른 친구가 이미 사용 중인 별명이야🥺\n다른 별명 없어?\n\n{self.__get_cmd_example("hi")}'
            return context.bot.send_message(chat_id=chat_id, text=msg)

        user_data = self.__get_user_data(chat_id, nickname)
        self.db_manager.insert_bulk_row('user', [user_data])
        return context.bot.send_message(chat_id=chat_id, text=f'{nickname}! 만나서 반가워😊 /help')

    def tg_help(self, update, context):
        chat_id = update.message.chat_id
        self.logger.info(f'{chat_id} | {context.args}')

        msg = f'만나서 반가워\!\n임원들의 주식거래에 기웃거리는 "[__*스눕*__](https://tinyurl.com/y9z7m6sa)"이라고해\.\n\n'
        msg += f'나는 매일 아침 8시에 전날의 스눕 결과를 알려주고,\n그 외의 상세 정보들도 알려줄 수 있어\.\n\n'
        msg += f'그리고 나는 챗봇이기 때문에,\n너가 지켜줘야 할 몇 가지 약속이 있어\!\n\n'
        msg += f'*1\. 나에게 말을 걸기 위해서는\n    항상 "/"로 시작을 해줘*\n'
        msg += f'*2\. 각 기능 별로 입력하는 값들에 대해서는,*\n    *꼭 띄어쓰기를 부탁해*\n\n'
        msg += f'그럼 이제 대화창에 "/"를 입력하면서\,\n'
        msg += f'우리 같이 놀아볼까\?\n\n'
        msg += f'참\! 나는 2018년 데이터부터 알려줄 수 있어\.\n그리고 👉 옆에 적힌 문구는 꼭 한번 클릭해봐\!\n\n\n'
        msg += f'\/hi \- 회원가입하기\n'
        msg += f'\/s \- \[s\]noop 조회하기\n'
        msg += f'\/d \- \[d\]etail\(상세\) 스눕 조회하기\n'
        msg += f'\/c \- 최근 \[c\]ompany\(회사\) 스눕 조회하기\n'
        msg += f'\/e \- 최근 \[e\]xecutive\(임원\) 스눕 조회하기\n'
        msg += f'\/w \- 회원정보 조회하기\n'
        msg += f'\/help \- 도움말 보기\n'
        threading.Thread(target=context.bot.send_message, args=(chat_id, msg, telegram.ParseMode.MARKDOWN_V2, True)).start()

    def tg_whoami(self, update, context):
        chat_id = update.message.chat_id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)

        user_info = self.db_manager.get_user_info(chat_id)
        if not user_info:
            msg = f'친구야 별명부터 얘기해줄래?\n\n{self.__get_cmd_example("hi")}'
            return context.bot.send_message(chat_id=chat_id, text=msg)

        expired_at = user_info[0]["expired_at"].strftime('%Y%m%d')
        expired_at = expired_at[:4] + '/' + expired_at[4:6] + '/' + expired_at[6:]
        msg = f'안녕 {user_info[0]["nickname"]}!\n'
        msg += f'우리 {expired_at} 까지 사이좋게 지내보자😇'
        return context.bot.send_message(chat_id=chat_id, text=msg)

    def tg_detail(self, update, context):
        invalid_cmd_msg = f'{self.__get_cmd_description("d")}{self.__get_cmd_example("d")}{self.__get_possible_error("d")}'
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        if len(context.args) < 1 or len(context.args) > 2:
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        
        corp_name = context.args[0]
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, invalid_cmd_msg)

        target_date = context.args[1] if len(context.args) == 2 else get_current_time('%Y%m%d', -1)
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, invalid_cmd_msg)
 
        data = self.db_manager.get_tg_detail_data(corp_name, target_date)
        target_date = target_date[:4] + '\/' + target_date[4:6] + '\/' + target_date[6:]

        message = f'📈 {target_date} __*{corp_info[0]["corp_name"]}*__ 변동 내역\n\n'
        message += self.__generate_detail_message_header(corp_info[0])
        message += self.__generate_detail_message_body(data)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()
     
    def tg_company(self, update, context):
        invalid_cmd_msg = f'{self.__get_cmd_description("c")}{self.__get_cmd_example("c")}{self.__get_possible_error("c")}'
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)

        if len(context.args) < 1 or len(context.args) > 2:
            return context.bot.send_message(chat_id, invalid_cmd_msg)

        corp_name = context.args[0]
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        
        count = context.args[1] if len(context.args) == 2 else '5'
        if not re.fullmatch(r'[0-9]+', count):
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        count = min(int(count), 10)

        data = self.db_manager.get_tg_company_data(corp_name, count)

        message = f'🏢 __*{corp_name}*__ TOP{count} 변동 내역\n\n'
        message += self.__generate_detail_message_header(corp_info[0])
        message += self.__generate_detail_message_body(data)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()

    def tg_executive(self, update, context):
        invalid_cmd_msg = f'{self.__get_cmd_description("e")}{self.__get_cmd_example("e")}{self.__get_possible_error("e")}'
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        if len(context.args) < 2 or len(context.args) > 3:
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        
        corp_name, executive_name = context.args[0], context.args[1]
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        
        count = context.args[2] if len(context.args) == 3 else '5'
        if not re.fullmatch(r'[0-9]+', count):
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        count = min(int(count), 10)

        data = self.db_manager.get_tg_executive_data(corp_name, executive_name, count)

        message = f'🏢 __*{corp_name}\({executive_name}\)*__ TOP{count} 변동 내역\n\n'
        message += self.__generate_detail_message_header(corp_info[0])
        message += self.__generate_executive_message_body(data)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()
    
    def __generate_executive_message_body(self, data):
        if not data:
            return NO_DATA_MSG
        
        details = collections.defaultdict(list)
        for d in data:
            details[d['rcept_no']].append(d)
        
        message = ''
        for rcept_no, infos in details.items():
            report_url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}'
            disclosed_on = str(infos[0]["disclosed_on"])[:10].replace('-', '\/')
            message += f'👉 [{disclosed_on}]({report_url})\n'

            for info in infos:
                traded_on = info['traded_on'].strftime('%m/%d').replace('/', '\/')
                reason_code = REVERSE_REASON_CODE.get(info['reason_code'])
                stock_type = REVERSE_STOCK_TYPE_CODE.get(info['stock_type'])
                delta = f'▲{info["delta_volume"]:,}' if info["delta_volume"] > 0 else f'▼{-info["delta_volume"]:,}'
                message += f'• {traded_on} \| {reason_code} \| {stock_type} \({delta}주 \/ {int(info["unit_price"]):,}원\)\n'
            message += '\n'
        return message

    def __generate_detail_message_header(self, corp_info):
        message = f'✔️ {corp_info["market"]} {corp_info["market_rank"]}위\n'
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
            message += f'👉 [{e_name}]({report_url})\n'
            
            for info in infos:
                traded_on = info['traded_on'].strftime('%m/%d').replace('/', '\/')
                reason_code = REVERSE_REASON_CODE.get(info['reason_code'])
                stock_type = REVERSE_STOCK_TYPE_CODE.get(info['stock_type'])
                delta = f'▲{info["delta_volume"]:,}' if info["delta_volume"] > 0 else f'▼{-info["delta_volume"]:,}'
                message += f'• {traded_on} \| {reason_code} \| {stock_type} \({delta}주 \/ {int(info["unit_price"]):,}원\)\n'
            message += '\n'
        return message

    def tg_snoopy(self, update, context):
        invalid_cmd_msg = f'{self.__get_cmd_description("s")}{self.__get_cmd_example("s")}{self.__get_possible_error("s")}'
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')
        
        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        if len(context.args) != 1:
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        
        target_date = context.args[0]
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, invalid_cmd_msg)
        
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

    