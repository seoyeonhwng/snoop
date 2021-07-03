import sys
import os
import json

config = {}
with open(os.path.dirname(os.path.realpath(__file__)) + '/../config.json', 'r') as _config_file:
    config = json.load(_config_file)

if not config:
    print('NO config.json')
    sys.exit()

MODE = config.get('mode')
CRTFC_KEY = config.get('crtfc_key')
WATCHDOG_BOT_TOKEN = config.get('watchdog_bot_token')
BOT_TOKEN = config.get('bot_token')
WARNING_BOT_TOKEN = config.get('warning_bot_token')
ADMIN_IDS = config.get('admin_ids')
MAIL_ID = config.get('mail').get('id')
MAIL_PWD = config.get('mail').get('pwd')
MINIMUM_TOTAL_AMOUNT = 10000000
WEAK_TOTAL_AMOUNT = 100000000
STRONG_TOTAL_AMOUNT = 500000000
MAX_BUSINESS_DATE = 900
MINIMUM_PROFIT = 10.0
TG_WORKERS = 32
TG_CONN_POOL = TG_WORKERS + 4

HOST = config.get('mysql').get('host')
USER = config.get('mysql').get('user')
PASSWORD = config.get('mysql').get('password')
DB = config.get('mysql').get('db')
PORT = config.get('mysql').get('port')
CHARSET = config.get('mysql').get('charset')

REASON_CODE = {
    '장내매수': '01',
    '장내매도': '02',
    '장외매도': '03',
    '장외매수': '04',
    '신규상장': '05',
    '신규선임': '06',
    '신규보고': '07',
    '합병': '08',
    '인수': '09',
    '증여': '10',
    '무상신주취득': '11',
    '유상신주취득': '12',
    '주식매수선택권': '13',
    '풋옵션권리행사에따른주식처분': '14',
    'CB인수': '15',
    '시간외매매': '16',
    '전환등': '17',
    '전환사채의권리행사': '18',
    '수증': '19',
    '행사가액조정': '20',
    '공개매수': '21',
    '공개매수청약': '22',
    '교환': '23',
    '임원퇴임': '24',
    '신규주요주주': '25',
    '자사주상여금': '26',
    '계약기간만료': '27',
    '대물변제': '28',
    '대여': '29',
    '상속': '30',
    '수증취소': '31',
    '신고대량매매': '32',
    '신규등록': '33',
    '신주인수권사채의권리행사': '34',
    '실권주인수': '35',
    '실명전환': '36',
    '자본감소': '37',
    '전환권등소멸': '38',
    '제자배정유상증자': '39',
    '주식매수청구권행사': '40',
    '주식병합': '41',
    '주식분할': '42',
    '차입': '43',
    '콜옵션권리행사배정에따른주식처분': '44',
    '콜옵션권리행사에따른주식취득': '45',
    '피상속': '46',
    '회사분할': '47',
    '계약중도해지': '48',
    '계열사편입': '49',
    '교환사채의권리행사': '50',
    '담보주식처분권보유': '51',
    '대물변제수령': '52',
    '매출': '53',
    '상속포기': '54',
    '신주인수권증권의권리행사': '55',
    '우선주무배당': '56',
    '우선주배당': '57',
    '주식배당': '58',
    '주식소각': '59',
    '증여취소': '60',
    '출자전환': '61',
    '특별관계해소': '62',
    '풋옵션권리행사배정에따른주식취득': '63',
    '기타': '99'
}

STOCK_TYPE_CODE = {
    '보통주': '01',
    '우선주': '02',
    '전환사채권': '03',
    '신주인수권이표시된것': '04',
    '신주인수권부사채권': '05',
    '증권예탁증권': '06',
    '교환사채권': '07',
    '기타': '99'
}

REVERSE_REASON_CODE = {v:k for k, v in REASON_CODE.items()}
REVERSE_STOCK_TYPE_CODE = {v:k for k, v in STOCK_TYPE_CODE.items()}
