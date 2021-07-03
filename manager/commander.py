import collections
import re
import time
import threading
from datetime import datetime

from manager.db_manager import DbManager
from manager.log_manager import LogManager
from manager.tg_manager import TgManager
from manager.msg_manager import MsgManager
from utils.commons import get_current_time, read_message, convert_to_str
from utils.config import ADMIN_IDS
from trading.checker import get_profit_ratio

MAX_NICKNAME_BYTE = 30
# INVALID_USER_MSG = '💵🤲 \.\.\.'
INVALID_USER_MSG = '아직 우린 친구가 아니야🥺\n회원가입부터 해줄래\?\n\n\/hi 명령어로 할 수 있어\!'


class Commander:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.msg_manager = MsgManager()

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

    def __get_params_guide(self, param):
        if param == 'input':
            return ''
        if param == 'corp_name':
            return '🚫 회사의 정식 명칭을 입력해줘\.\n\n'
        if param == 'target_date':
            return '🚫 입력한 날짜 형식을 확인해줘\.\n\n'
        if param == 'count':
            return '🚫 개수는 숫자를 입력해줘\.\n\n'

    def __is_valid_value_format(self, key, value):
        if key == 'input':
            return value.get('length')[0] <= len(value.get('args')) <= value.get('length')[1]
        if key == 'corp_name':
            return True if self.db_manager.get_corporate_info(value) else False
        if key == 'target_date':
            return True if re.fullmatch(r'[0-9]{8}', value) else False
        if key == 'count':
            return True if re.fullmatch(r'[0-9]+', value) else False

    def __get_unvalid_params(self, params):
        for k, v in params.items():
            if not self.__is_valid_value_format(k, v):
                return k
        return None
    
    def __is_valid_user(self, chat_id):
        user_info = self.db_manager.get_user_info(chat_id)
        if user_info and user_info[0]['is_paid'] and user_info[0]['is_active']:
            return True
        return False

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
        unvalid_param = self.__get_unvalid_params(params)

        if unvalid_param:
            tg_msg = self.__get_params_guide(unvalid_param)
            tg_msg += read_message('s_guide.txt')
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
        tg_msg = self.msg_manager.get_snoop_message(target_date)

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
        unvalid_param = self.__get_unvalid_params(params)

        if unvalid_param:
            tg_msg = self.__get_params_guide(unvalid_param)
            tg_msg += read_message('d_guide.txt')
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
        tg_msg = self.msg_manager.get_detail_message(corp_name, target_date)

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
        unvalid_param = self.__get_unvalid_params(params)

        if unvalid_param:
            tg_msg = self.__get_params_guide(unvalid_param)
            tg_msg += read_message('c_guide.txt')
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
        corp_name, count = context.args[0], min(int(params.get('count')), 10)
        tg_msg = self.msg_manager.get_company_message(corp_name, count)

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
        unvalid_param = self.__get_unvalid_params(params)

        if unvalid_param:
            tg_msg = self.__get_params_guide(unvalid_param)
            tg_msg += read_message('e_guide.txt')

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
        tg_msg = self.msg_manager.get_executive_message(corp_name, executive_name, count)

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

        time.sleep(2.5)
        last_business_date = convert_to_str(self.db_manager.get_last_business_date(), '%Y%m%d')
        tg_msg = self.msg_manager.get_snoop_message(last_business_date)
        context.dispatcher.run_async(
            self.__log_and_notify,
            'tg_hi_snoop',
            log_msg,
            chat_id,
            tg_msg,
            update=update
        )
        
        # notice to admins
        tg_msg = f'[신규 회원 도착]\n{nickname}'
        threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()

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

        # notice to admins
        tg_msg = f'[피드백 도착]\n{content}'
        threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()

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

    def tg_buy(self, update, context):
        if str(update.message.chat_id) not in ADMIN_IDS:
            return

        corp_name, buy_price = context.args
        stock_code = self.db_manager.get_stock_code(corp_name)
        if not stock_code:
            tg_msg = '해당 종목 존재 안함!!!'
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return

        data = {
            'stock_code': stock_code[0]['stock_code'],
            'buy_date': get_current_time(),
            'buy_price': buy_price,
            'last_updated_at': get_current_time()
        }
        result = self.db_manager.insert_row('trading', data)
        tg_msg = f'[/buy {corp_name}] DB insert 성공' if result else f'[/buy {corp_name}] DB insert 실패'
        threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
        
    def tg_sell(self, update, context):
        if str(update.message.chat_id) not in ADMIN_IDS:
            return
        
        buy_date, corp_name, sell_price = context.args
        stock_code = self.db_manager.get_stock_code(corp_name)
        if not stock_code:
            tg_msg = '해당 종목 존재 안함!!!'
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return
        stock_code = stock_code[0]['stock_code']
        
        trading_data = self.db_manager.get_trading_data(stock_code, buy_date)
        if not trading_data:
            tg_msg = '해당 매매 존재 안함!!!'
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return
        
        last_price = trading_data[0]['last_price'] if trading_data[0]['last_price'] else trading_data[0]['buy_price']
        profit_ratio = get_profit_ratio(sell_price, last_price, trading_data[0].get('profit_ratio', None))
        
        params = {
            'stock_code': stock_code,
            'buy_date': buy_date,
            'sell_price': sell_price,
            'sell_date': get_current_time(),
            'last_price': last_price,
            'profit_ratio': profit_ratio,
            'last_updated_at': get_current_time()
        }
        result = self.db_manager.update_trading_data(params)
        tg_msg = f'[/sell {corp_name}] DB insert 성공' if result else f'[/sell {corp_name}] DB insert 실패'
        threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()

    def tg_notice(self, update, context):
        if str(update.message.chat_id) not in ADMIN_IDS:
            return

        chat_id = update.effective_chat.id
        log_msg = f'{chat_id}|{context.args}'

        # check input condition
        tg_msg = read_message('n_guide.txt')
        if len(context.args) < 2 or context.args[0] not in ['dev', 'prd']:
            context.dispatcher.run_async(
                self.__log_and_notify,
                'tg_notice',
                log_msg,
                chat_id,
                tg_msg,
                update=update
            )
            return

        send_type = context.args[0]
        content = ' '.join(context.args[1:])
        if len(content.split('n')) > 1:
            content = '\n'.join(content.split('n'))

        if send_type == 'dev':  # admin
            threading.Thread(target=self.tg_manager.send_all_message, args=(ADMIN_IDS, content,)).start()
        else:  # whole member
            targets = self.db_manager.get_targets()
            threading.Thread(target=self.tg_manager.send_all_message, args=(targets, content,)).start()

    # TODO.
    def tg_mail(self, update, context):
        # 회원정보 받고
        # 메일 주소 받고
        # 메일 형식 체크 한번 하고
        # 업데이트 하고
        # 성공등록 완료!
        return
