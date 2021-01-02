import collections
import telegram
import re
import threading
from datetime import datetime

from manager.db_manager import DbManager
from manager.log_manager import LogManager
from manager.tg_manager import TgManager
from utils.config import REVERSE_REASON_CODE, REVERSE_STOCK_TYPE_CODE
from utils.commons import get_current_time, read_message

INVALID_USER_MSG = '💵🤲 ...'
NO_DATA_MSG = '아쉽게도 알려줄 내용이 없어🥺'


class Commander:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()

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

        context.bot.send_message(chat_id=update.effective_chat.id, text=read_message('start.txt'), parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def tg_hi(self, update, context):
        chat_id, nickname = update.effective_chat.id, ''.join(context.args)
        self.logger.info(f'{chat_id}|{context.args}')

        if not nickname.strip():
            return context.bot.send_message(chat_id, read_message('hi_guide.txt'), parse_mode=telegram.ParseMode.MARKDOWN_V2)

        user_info = self.db_manager.get_user_info(chat_id)
        if user_info:
            text = read_message('hi_valid_user.txt').format(nickname=nickname)
            return context.bot.send_message(chat_id=chat_id, text=text, parse_mode=telegram.ParseMode.MARKDOWN_V2)
      
        if not self.db_manager.is_valid_nickname(nickname):
            return context.bot.send_message(chat_id=chat_id, text=read_message('hi_unvalid_nickname.txt'), parse_mode=telegram.ParseMode.MARKDOWN_V2)

        user_data = self.__get_user_data(chat_id, nickname)
        self.db_manager.insert_bulk_row('user', [user_data])
        return context.bot.send_message(chat_id=chat_id, text=read_message('hi_success.txt'), parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def tg_help(self, update, context):
        chat_id = update.message.chat_id
        self.logger.info(f'{chat_id}|{context.args}')
        guide = read_message('help.txt')

        threading.Thread(target=context.bot.send_message, args=(chat_id, guide, telegram.ParseMode.MARKDOWN_V2, True)).start()

    def tg_whoami(self, update, context):
        chat_id = update.message.chat_id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)

        user_info = self.db_manager.get_user_info(chat_id)
        if not user_info:
            return context.bot.send_message(chat_id=chat_id, text=read_message('w_unvalid_user.txt'), parse_mode=telegram.ParseMode.MARKDOWN_V2)

        expired_on = user_info[0]['expired_at'].strftime('%Y/%m/%d').replace('/', r'\/')
        nickname = user_info[0]['nickname']
        message = read_message('w_success.txt').format(nickname=nickname, expired_on=expired_on)
        return context.bot.send_message(chat_id=chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN_V2)

    def tg_detail(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        guide = read_message('d_guide.txt')
        if len(context.args) < 1 or len(context.args) > 2:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        
        corp_name = context.args[0]
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)

        target_date = context.args[1] if len(context.args) == 2 else get_current_time('%Y%m%d', -1)
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
 
        data = self.db_manager.get_tg_detail_data(corp_name, target_date)
        target_date = target_date[:4] + '\/' + target_date[4:6] + '\/' + target_date[6:]

        message = f'📈 {target_date} __*{corp_info[0]["corp_name"]}*__ 변동 내역\n\n'
        message += self.__generate_detail_message_header(corp_info[0])
        message += self.__generate_detail_message_body(data)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()
     
    def tg_company(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)

        guide = read_message('c_guide.txt')
        if len(context.args) < 1 or len(context.args) > 2:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)

        corp_name = context.args[0]
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        
        count = context.args[1] if len(context.args) == 2 else '5'
        if not re.fullmatch(r'[0-9]+', count):
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        count = min(int(count), 10)

        data = self.db_manager.get_tg_company_data(corp_name, count)

        message = f'🏢 __*{corp_name}*__ TOP{count} 변동 내역\n\n'
        message += self.__generate_detail_message_header(corp_info[0])
        message += self.__generate_detail_message_body(data)

        threading.Thread(target=context.bot.send_message, args=(chat_id, message, telegram.ParseMode.MARKDOWN_V2)).start()

    def tg_executive(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')

        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        guide = read_message('e_guide.txt')
        if len(context.args) < 2 or len(context.args) > 3:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        
        corp_name, executive_name = context.args[0], context.args[1]
        corp_info = self.db_manager.get_corporate_info(corp_name)
        if not corp_info:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        
        count = context.args[2] if len(context.args) == 3 else '5'
        if not re.fullmatch(r'[0-9]+', count):
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
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

        message += '\n특정 회사의 최근 스눕이 궁금하면 👉 /c\n특정 임원의 최근 스눕이 궁금하면 👉 /e'
        return message

    def tg_snoopy(self, update, context):
        chat_id = update.effective_chat.id
        self.logger.info(f'{chat_id}|{context.args}')
        
        if not self.__is_valid_user(chat_id):
            return context.bot.send_message(chat_id, INVALID_USER_MSG)
        
        guide = read_message('s_guide.txt')
        if len(context.args) != 1:
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        
        target_date = context.args[0]
        if not re.fullmatch(r'[0-9]{8}', target_date):
            return context.bot.send_message(chat_id, guide, parse_mode=telegram.ParseMode.MARKDOWN_V2)
        
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
        message += '\n상세 스눕이 궁금하면 👉 /d'

        return message

    def __log_and_notify(self, func_name, log_msg, tg_target, tg_msg):
        self.logger.info(f'{func_name}|{log_msg}')
        self.tg_manager.send_message(tg_target, tg_msg)

    def __get_feedback_template(self, chat_id, content):
        feedback_data = {
            'chat_id': chat_id,
            'content': content,
            'created_at': get_current_time()
        }
        return feedback_data

    def tg_feedback(self, update, context):
        chat_id, content = update.effective_chat.id, ' '.join(context.args)
        log_msg = f'{chat_id}|{context.args}'

        tg_msg = read_message('f_guide.txt')
        if not content.strip():
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_feedback',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        tg_msg = f'좋은 의견 고마워\! 더욱 발전하는 스눕이가 될께😎'
        feedback = self.__get_feedback_template(chat_id=chat_id, content=content)
        self.db_manager.insert_row('feedback', feedback)
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_feedback',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )
