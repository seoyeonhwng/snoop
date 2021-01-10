from collections import defaultdict

from manager.db_manager import DbManager
from utils.commons import get_current_time, read_message, convert_format, convert_to_str
from utils.config import REVERSE_REASON_CODE, REVERSE_STOCK_TYPE_CODE

class MsgManager:
    def __init__(self):
        self.db_manager = DbManager()
    
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
        
    def __get_signal(self, amount):
        if amount <= 50000000:
            return '❗️'
        if amount <= 100000000:
            return '‼️'
        return '🔥'

    def __get_snoop_header(self, target_date):
        target_date = convert_format(target_date, '%Y%m%d', '%Y-%m-%d').replace("-", "\/")
        greeting = self.__get_greeting()

        message = read_message('s_header.txt').format(greeting=greeting, target_date=target_date)
        return message
        
    def __get_snoop_body(self, data):
        if not data:
            return read_message('no_data.txt')
        
        groupby_rcept = defaultdict(list)
        for d in data:
            groupby_rcept[d['rcept_no']].append(d)
        
        corp_infos, industry_corp_map = {}, defaultdict(set)
        for rcept in groupby_rcept.values():
            total_amount = abs(sum([r['delta_volume'] * r['unit_price'] for r in rcept]))
            if total_amount < 10000000:
                continue

            corp_code, corp_name, industry_name = rcept[0]['corp_code'], rcept[0]['corp_name'], rcept[0]['industry_name']
            industry_corp_map[industry_name].add(corp_code)

            if corp_code not in corp_infos:
                corp_infos[corp_code] = {
                    'corp_name': corp_name.replace('-', '\-').replace('.', '\.'),
                    'count': 1,
                    'max_total_amount': total_amount
                }
            else:
                corp_infos[corp_code]['count'] += 1
                corp_infos[corp_code]['max_total_amount'] = max(corp_infos[corp_code]['max_total_amount'], total_amount)
        
        message = ''
        for industry_name in [d['industry_name'] for d in self.db_manager.get_industry_list()]:
            corporates = industry_corp_map.get(industry_name)
            if not corporates:
                continue
            
            message += f'🐮 *{industry_name}*\n'
            for corp in corporates:
                info = corp_infos.get(corp)
                message += f'• {info["corp_name"]}\({info["count"]}건\) {self.__get_signal(info["max_total_amount"])}\n'
            message += '\n'
        return message
    
    def __get_corp_detail(self, corp_name):
        data = self.db_manager.get_corporate_info(corp_name)[0]
        params = {
            'market': data['market'], 
            'market_rank': data['market_rank'],
            'market_capitalization': int(data['market_capitalization'])
        }

        message = read_message('corp_detail.txt')
        return message.format(**params)

    def __get_message_body(self, data, groupby):
        if not data:
            return read_message('no_data.txt')

        groupby_data = defaultdict(list)
        for d in data:
            groupby_data[d[groupby]].append(d)

        message = ''
        for key, values in groupby_data.items():
            report_url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={values[0]["rcept_no"]}'
            title = key if groupby == 'executive_name' else convert_to_str(values[0]['disclosed_on'], '%Y\/%m\/%d')
            message += f'👉 [{title}]({report_url})\n'

            for v in values:
                traded_on = convert_to_str(v['traded_on'], '%m/%d').replace('/', '\/')
                reason_code = REVERSE_REASON_CODE.get(v['reason_code'])
                stock_type = REVERSE_STOCK_TYPE_CODE.get(v['stock_type'])
                delta = f'▲{v["delta_volume"]:,}' if v["delta_volume"] > 0 else f'▼{-v["delta_volume"]:,}'
                message += f'• {traded_on} \| {reason_code} \| {stock_type} \({delta}주 \/ {int(v["unit_price"]):,}원\)\n'
            message += '\n'
        return message

    def get_snoop_message(self, target_date):
        data = self.db_manager.get_disclosure_data(target_date)

        message = self.__get_snoop_header(target_date)
        message += self.__get_snoop_body(data)
        return message

    def get_detail_message(self, corp_name, target_date):
        data = self.db_manager.get_tg_detail_data(corp_name, target_date)

        target_date = convert_format(target_date, '%Y%m%d', '%Y\/%m\/%d')
        message = f'📈 {target_date} __*{corp_name}*__ 변동 내역\n\n'
        message += self.__get_corp_detail(corp_name)
        message += self.__get_message_body(data, 'executive_name')
        message += '\n\n특정 회사의 상세 스눕이 궁금하면 👉 /d\n특정 회사의 최근 스눕이 궁금하면 👉 /c\n특정 임원의 최근 스눕이 궁금하면 👉 /e'

        return message

    def get_company_message(self, corp_name, count):
        data = self.db_manager.get_tg_company_data(corp_name, count)

        message = f'🏢 __*{corp_name}*__ TOP{count} 변동 내역\n\n'
        message += self.__get_corp_detail(corp_name)
        message += self.__get_message_body(data, 'executive_name')
        message += '\n\n특정 회사의 상세 스눕이 궁금하면 👉 /d\n특정 회사의 최근 스눕이 궁금하면 👉 /c\n특정 임원의 최근 스눕이 궁금하면 👉 /e'

        return message

    def get_executive_message(self, corp_name, executive_name, count):
        data = self.db_manager.get_tg_executive_data(corp_name, executive_name, count)

        message = f'🏢 __*{corp_name}\({executive_name}\)*__ TOP{count} 변동 내역\n\n'
        message += self.__get_corp_detail(corp_name)
        message += self.__get_message_body(data, 'rcept_no')
        message += '\n\n특정 회사의 상세 스눕이 궁금하면 👉 /d\n특정 회사의 최근 스눕이 궁금하면 👉 /c\n특정 임원의 최근 스눕이 궁금하면 👉 /e'
        return message