import requests
import logging
import time
import re
from datetime import datetime

from bs4 import BeautifulSoup

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


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


class ParsingManager:
    def __init__(self):
        self.logger = logger

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
