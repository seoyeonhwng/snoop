import requests
import logging
import time
import re
import json
import threading
from urllib.request import urlopen
from datetime import datetime
from copy import deepcopy

from zipfile import ZipFile
from io import BytesIO
import xml.etree.ElementTree as Et

from bs4 import BeautifulSoup

from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager
from manager.utils import read_config, get_current_time

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

NO_DATA_MSG = "조회된 데이터가 없습니다."
FINISH_MSG = "데이터 로드 성공하였습니다."

API_URL = 'https://opendart.fss.or.kr/api'
MAIN_URL = "https://dart.fss.or.kr"
REPORT = "/dsaf001/main.do?rcpNo={rcept_no}"
SNOOP = "/report/viewer.do?dtd=dart3.xsd&eleId=4&offset=1&length=1&rcpNo={rcept_no}&dcmNo={dcm_no}"

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
    '기타': '99'
}


class Dart:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Dart, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.logger = logger
        self.session = requests.session()
        self.crtfc_key = read_config().get('crtfc_key')
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.api_manager = ApiManager()

    def get_json(self, api, p):
        p['crtfc_key'] = self.crtfc_key
        url_parameter = '&'.join([f'{key}={value}' for key, value in p.items()])
        resp = self.session.get(f'{API_URL}/{api}.json?{url_parameter}')
        return json.loads(resp.text)

    def get_xml(self, api, p):
        p['crtfc_key'] = self.crtfc_key
        url_parameter = '&'.join([f'{key}={value}' for key, value in p.items()])
        return urlopen(f'{API_URL}/{api}.xml?{url_parameter}')

    def convert_valid_format(self, text, text_type):
        if text_type == 'text':
            return re.compile('[^가-힣]+').sub('', text)

        if text_type == 'date':
            text = re.compile('[^0-9]+').sub('', text)
            return datetime.strptime(text, '%Y%m%d')

        if text_type == 'volume':
            text = '0' if text == '-' else text
            text = re.compile('(?!^-)[^-|0-9]+').sub('', text)
            return 0 if text in ['', '-'] else int(text)

        if text_type == 'price':
            text = '0.0' if text == '-' else text
            text = text.replace('.', ',', text.count('.') - 1) if text.count('.') > 1 else text # 오타 방지를 위해 마지막 .만 소수점으로 처리
            text = re.compile('[^0-9.]').sub('', text)
            return 0.0 if text in ['', '-'] else float(text)

    def get_dcm_no(self, _rcept_no):
        r = requests.get(MAIN_URL + REPORT.format(rcept_no=_rcept_no))
        for href in BeautifulSoup(r.text, 'html.parser').find('div', class_='view_search').find_all('li')[:1]:
            return href.find('a')['onclick'].split(' ')[1].replace("'", '').replace(');', '')

    def get_target_table(self, _tables):
        for t in _tables:
            th = t.find(lambda tag: tag.name == 'th')
            if th and th.text == "보고사유":
                return t

    def get_empty_data(self, _rcept_no, _rcept_dt, _stock_code, _executive_name):
        p = {
            'rcept_no': _rcept_no,
            'disclosed_on': datetime.strptime(_rcept_dt, "%Y%m%d"),
            'stock_code': _stock_code,
            'executive_name': _executive_name,
            'reason_code': '',
            'traded_on': '',
            'stock_type': '',
            'before_volume': '',
            'delta_volume': '',
            'after_volume': '',
            'unit_price': '',
            'remark': ''
        }
        return p

    def get_stock_detail(self, _rcept_no, _dcm_no, _rcept_dt, _stock_code, _executive_name):
        stock_detail = []
        r = requests.get(MAIN_URL + SNOOP.format(rcept_no=_rcept_no, dcm_no=_dcm_no))
        time.sleep(1)

        bs = BeautifulSoup(r.text, 'html.parser')
        table = self.get_target_table(bs.findAll(lambda tag: tag.name == 'table'))
        rows = table.findAll(lambda tag: tag.name == 'tr')

        col_names = ['reason_code', 'traded_on', 'stock_type', 'before_volume',
                     'delta_volume', 'after_volume', 'unit_price', 'remark']
        col_types = ['text', 'date', 'text', 'volume', 'volume', 'volume', 'price', 'text']

        for row in rows[2:-1]:
            row_content = [r.text for r in row if r.name == 'td']
            p = self.get_empty_data(_rcept_no, _rcept_dt, _stock_code, _executive_name)

            for text, text_type, c_name in zip(row_content, col_types, col_names):
                print(text, text_type, c_name)
                p[c_name] = self.convert_valid_format(text, text_type)

            p['reason_code'] = REASON_CODE.get(p['reason_code']) if REASON_CODE.get(p['reason_code']) else p[
                'reason_code']
            p['stock_type'] = STOCK_TYPE_CODE.get(p['stock_type']) if STOCK_TYPE_CODE.get(p['stock_type']) else p[
                'stock_type']
            stock_detail.append(p)

        return stock_detail

    def parsing(self, data):
        stock_diff = {}
        for i, d in enumerate(data):
            dcm_no = self.get_dcm_no(d.get('rcept_no'))
            self.logger.info(f"parsing: {d.get('rcept_no')} -> {i + 1} / {len(data)}")
            stock_diff[d.get('rcept_no')] = self.get_stock_detail(d.get('rcept_no'), dcm_no,
                                                                  d.get('rcept_dt'), d.get('stock_code'), d.get('flr_nm'))

        return stock_diff

    def build_corporate_list(self, base_corporate):
        corporates = []
        resp = self.get_xml('corpCode', {})
        with ZipFile(BytesIO(resp.read())) as zf:
            file_list = zf.namelist()
            while len(file_list) > 0:
                file_name = file_list.pop()
                xml = zf.open(file_name).read().decode()
                tree = Et.fromstring(xml)
                stock_list = tree.findall("list")

                stock_code = [x.findtext("stock_code") for x in stock_list]
                corp_code = [x.findtext("corp_code") for x in stock_list]
                corp_name = [x.findtext("corp_name") for x in stock_list]

                for sc, cc, cn in zip(stock_code, corp_code, corp_name):
                    if sc.replace(" ", ""):  # stock_code 있는 경우만
                        base = deepcopy(base_corporate)
                        base['stock_code'] = sc
                        base['corp_code'] = cc
                        base['corp_name'] = cn
                        corporates.append(base)
                return corporates

    def __get_executive_data(self, data):
        """
        임원 관련 보고서와 유가, 코스닥 데이터만 가지고 온다.
        """
        f = lambda x: x.get('report_nm') == '임원ㆍ주요주주특정증권등소유상황보고서' and x.get('corp_cls') in ['Y', 'K']
        return [d for d in data if f(d)]

    def insert_executive(self, _start_date=None, _end_date=None):
        if not _start_date:
            _start_date = get_current_time('%Y%m%d', -1)
        if not _end_date:
            _end_date = _start_date

        params = {
            'bgn_de': _start_date,
            'end_de': _end_date,
            'page_count': 100
        }

        response = self.api_manager.get_json('list', params)
        if response['status'] not in ['000', '013']:
            tg_msg = f"[ERROR] status code - {response['status']}"
            self.logger.info(tg_msg)
            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()
            return

        if response['status'] == '013':
            self.logger.info(NO_DATA_MSG)
            threading.Thread(target=self.tg_manager.send_warning_message, args=(NO_DATA_MSG,)).start()
            return

        data, total_page = response['list'], response['total_page']
        for i in range(2, total_page + 1):
            self.logger.info(f"{_start_date} ~ {_end_date}: current page {i} / {total_page}")
            time.sleep(0.5)  # to prevent IP ban

            params['page_no'] = i
            response = self.api_manager.get_json('list', params)
            if response['status'] != '000':
                continue

            data += response['list']

        executive_data = self.__get_executive_data(data)
        parsed = self.parsing(executive_data)

        for rcept, detail in parsed.items():
            stock_detail = []
            for d in detail:
                d['created_at'] = get_current_time()
                stock_detail.append(tuple(d.values()))
            self.logger.info(f"DB insert on {rcept}")
            self.db_manager.insert_executive(stock_detail)

        threading.Thread(target=self.tg_manager.send_warning_message, args=(FINISH_MSG,)).start()