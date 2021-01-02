import collections
import re
import time
from datetime import datetime

from manager.db_manager import DbManager
from manager.log_manager import LogManager
from manager.tg_manager import TgManager
from utils.config import REVERSE_REASON_CODE, REVERSE_STOCK_TYPE_CODE
from utils.commons import get_current_time, read_message

MAX_NICKNAME_BYTE = 30
# INVALID_USER_MSG = '💵🤲 \.\.\.'
INVALID_USER_MSG = '아직 우린 친구가 아니야🥺\n회원가입부터 해줄래\?\n\n\/hi 명령어로 할 수 있어\!'
NO_DATA_MSG = '아쉽게도 알려줄 내용이 없어🥺'


class Commander:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()

    def __log_and_notify(self, func_name, log_msg, tg_target, tg_msg):
        self.logger.info(f'{func_name}|{log_msg}')
        self.tg_manager.send_message(tg_target, tg_msg)

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

    def __get_feedback_data(self, chat_id, content):
        feedback_data = {
            'chat_id': chat_id,
            'content': content,
            'created_at': get_current_time()
        }
        return feedback_data

    def __is_valid_value_format(self, key, value):
        if key == 'input':
            return value.get('length')[0] <= len(value.get('args')) <= value.get('length')[1]
        if key == 'corp_name':
            return True if self.db_manager.get_corporate_info(value) else False
        if key == 'target_date':
            return True if re.fullmatch(r'[0-9]{8}', value) else False
        if key == 'count':
            return True if re.fullmatch(r'[0-9]+', value) else False

    def __is_valid_params(self, params):
        for k, v in params.items():
            if not self.__is_valid_value_format(k, v):
                return False
        return True
    
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

        message += '\n특정 회사의 상세 스눕이 궁금하면 👉 /d\n특정 회사의 최근 스눕이 궁금하면 👉 /c\n특정 임원의 최근 스눕이 궁금하면 👉 /e'
        return message

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

    def tg_start(self, update, context):
        chat_id = update.effective_chat.id
        log_msg = f'{chat_id}|{context.args}'

        tg_msg = read_message('start.txt')
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_start',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_snoop(self, update, context):
        chat_id = update.effective_chat.id
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = INVALID_USER_MSG
        if not self.__is_valid_user(chat_id):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_snoop',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # check input condition
        params = {
            'input': {'args': context.args, 'length': (1, 1)},
            'target_date': context.args[0] if len(context.args) > 0 else get_current_time('%Y%m%d', -1),
        }
        tg_msg = read_message('s_guide.txt')
        if not self.__is_valid_params(params):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_snoop',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # process & send
        target_date = params.get('target_date')
        data = self.db_manager.get_disclosure_data(target_date)
        tg_msg = self.__generate_snoopy_messsage(data, target_date)
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_snoop',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_detail(self, update, context):
        chat_id = update.effective_chat.id
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = INVALID_USER_MSG
        if not self.__is_valid_user(chat_id):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_detail',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # check input condition
        params = {
            'input': {'args': context.args, 'length': (1, 2)},
            'corp_name': context.args[0] if len(context.args) > 0 else None,
            'target_date': context.args[1] if len(context.args) > 1 else get_current_time('%Y%m%d', -1),
        }
        tg_msg = read_message('d_guide.txt')
        if not self.__is_valid_params(params):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_detail',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # process & send
        corp_name, target_date = context.args[0], params.get('target_date')
        corp_info = self.db_manager.get_corporate_info(corp_name)
        data = self.db_manager.get_tg_detail_data(corp_name, target_date)
        target_date = target_date[:4] + '\/' + target_date[4:6] + '\/' + target_date[6:]

        tg_msg = f'📈 {target_date} __*{corp_info[0]["corp_name"]}*__ 변동 내역\n\n'
        tg_msg += self.__generate_detail_message_header(corp_info[0])
        tg_msg += self.__generate_detail_message_body(data)

        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_detail',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_company(self, update, context):
        chat_id = update.effective_chat.id
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = INVALID_USER_MSG
        if not self.__is_valid_user(chat_id):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_company',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # check input condition
        params = {
            'input': {'args': context.args, 'length': (1, 2)},
            'corp_name': context.args[0] if len(context.args) > 0 else None,
            'count': context.args[1] if len(context.args) > 1 else str(5),
        }
        tg_msg = read_message('c_guide.txt')
        if not self.__is_valid_params(params):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_company',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # process & send
        corp_name = context.args[0]
        count = min(int(params.get('count')), 10)
        corp_info = self.db_manager.get_corporate_info(corp_name)
        data = self.db_manager.get_tg_company_data(corp_name, count)

        tg_msg = f'🏢 __*{corp_name}*__ TOP{count} 변동 내역\n\n'
        tg_msg += self.__generate_detail_message_header(corp_info[0])
        tg_msg += self.__generate_detail_message_body(data)

        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_company',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_executive(self, update, context):
        chat_id = update.effective_chat.id
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = INVALID_USER_MSG
        if not self.__is_valid_user(chat_id):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_executive',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # check input condition
        params = {
            'input': {'args': context.args, 'length': (2, 3)},
            'corp_name': context.args[0] if len(context.args) > 0 else None,
            'count': context.args[2] if len(context.args) > 2 else str(5),
        }
        tg_msg = read_message('e_guide.txt')
        if not self.__is_valid_params(params):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_executive',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # process & send
        corp_name, executive_name = context.args[0], context.args[1]
        count = min(int(params.get('count')), 10)
        corp_info = self.db_manager.get_corporate_info(corp_name)
        data = self.db_manager.get_tg_executive_data(corp_name, executive_name, count)

        tg_msg = f'🏢 __*{corp_name}\({executive_name}\)*__ TOP{count} 변동 내역\n\n'
        tg_msg += self.__generate_detail_message_header(corp_info[0])
        tg_msg += self.__generate_executive_message_body(data)

        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_executive',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_whoami(self, update, context):
        chat_id = update.message.chat_id
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = INVALID_USER_MSG
        if not self.__is_valid_user(chat_id):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_whoami',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # process & send
        user_info = self.db_manager.get_user_info(chat_id)
        expired_on = user_info[0]['expired_at'].strftime('%Y/%m/%d').replace('/', r'\/')
        nickname = user_info[0]['nickname']
        tg_msg = read_message('w_success.txt').format(nickname=nickname, expired_on=expired_on)
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_whoami',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_hi(self, update, context):
        chat_id, nickname = update.effective_chat.id, ' '.join(context.args)
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = read_message('hi_valid_user.txt')
        user_info = self.db_manager.get_user_info(chat_id)
        if user_info:
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_hi',
                log_msg,
                chat_id,
                tg_msg.format(nickname=user_info[0].get('nickname')),
                update=update
            )
            return

        # check input condition
        tg_msg = None
        if not nickname.strip() or len(nickname.encode('utf-8')) >= MAX_NICKNAME_BYTE:
            tg_msg = read_message('hi_guide.txt')
        elif not self.db_manager.is_valid_nickname(nickname):
            tg_msg = read_message('hi_unvalid_nickname.txt')
        if tg_msg:
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_hi',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # process & send
        user_data = self.__get_user_data(chat_id, nickname)
        self.db_manager.insert_row('user', user_data)

        tg_msg = read_message('hi_success.txt').format(nickname=nickname)
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_hi',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_help(self, update, context):
        chat_id = update.message.chat_id
        log_msg = f'{chat_id}|{context.args}'

        # process & send
        tg_msg = read_message('help1.txt')
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_help',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

        time.sleep(1.2)
        tg_msg = read_message('help2.txt')
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_help',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

        time.sleep(1.2)
        tg_msg = read_message('help3.txt')
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_help',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_feedback(self, update, context):
        chat_id, content = update.effective_chat.id, ' '.join(context.args)
        log_msg = f'{chat_id}|{context.args}'

        # check if user is valid
        tg_msg = INVALID_USER_MSG
        if not self.__is_valid_user(chat_id):
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_feedback',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        # check input condition
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

        # process & send
        feedback = self.__get_feedback_data(chat_id=chat_id, content=content)
        self.db_manager.insert_row('feedback', feedback)

        tg_msg = f'좋은 의견 고마워\! 더욱 발전하는 스눕이가 될게😎'
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_feedback',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )

    def tg_command(self, update, context):
        chat_id = update.message.chat_id
        log_msg = f'{chat_id}|{context.args}'

        # process & send
        tg_msg = read_message('help3.txt')
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_command',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )
