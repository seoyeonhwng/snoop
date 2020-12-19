import requests
import threading
import time
import re
import logging

from bs4 import BeautifulSoup

from datetime import datetime, timedelta, date

from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager
from manager.utils import get_date, filter_text, REASON_CODE, STOCK_TYPE_CODE

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

NO_DATA_MSG = "조회된 데이터가 없습니다."

MAIN_URL = "https://dart.fss.or.kr"
REPORT = "/dsaf001/main.do?rcpNo={rcept_no}"
SNOOP = "/report/viewer.do?dtd=dart3.xsd&eleId=4&offset=1&length=1&rcpNo={rcept_no}&dcmNo={dcm_no}"


class Snoopy:
    def __init__(self):
        self.logger = logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.api_manager = ApiManager()

    def get_executive_data(self, data):
        """
        임원 관련 보고서와 유가, 코스닥 데이터만 가지고 온다.
        """
        f = lambda x: x['report_nm'] == '임원ㆍ주요주주특정증권등소유상황보고서' and x['corp_cls'] in ['Y', 'K']
        return [d for d in data if f(d)]

    def get_dcm_no(self, _rcept_no):
        r = requests.get(MAIN_URL + REPORT.format(rcept_no=_rcept_no))
        for href in BeautifulSoup(r.text, 'html.parser').find('div', class_='view_search').find_all('li')[:1]:
            return href.find('a')['onclick'].split(' ')[1].replace("'", '').replace(');', '')

    def get_stock_detail(self, _rcept_no, _dcm_no):
        stock_detail = []
        r = requests.get(MAIN_URL + SNOOP.format(rcept_no=_rcept_no, dcm_no=_dcm_no))
        time.sleep(1)

        tbody = BeautifulSoup(r.text, 'html.parser').find_all('table')[-1:][0].find('tbody')  # get most bottom table
        for tr in tbody.find_all('tr')[:-1]:  # remove sum row
            p = {}
            for idx, td in enumerate(tr.find_all('td')):
                value = td.text
                if idx == 0:  # 보고사유
                    value = re.compile(r'[ㄱ-ㅣ가-힣]+').findall(value)[0]
                    value = value if REASON_CODE.get(value) is None else REASON_CODE.get(value)
                    p['reason'] = value
                elif idx == 1:  # 변동일
                    conver_date = re.compile(r'\d+').findall(value)
                    year, month, day = conver_date[0], conver_date[1], conver_date[2]
                    p['date'] = date(int(year), int(month), int(day))
                elif idx == 2:  # 증권종류
                    value = value if STOCK_TYPE_CODE.get(value) is None else STOCK_TYPE_CODE.get(value)
                    p['stock_type'] = value
                elif idx == 3:  # 주식 변동 전
                    p['before'] = int(value.replace(',', '').replace('-', '0'))
                elif idx == 4:  # 주식 증감
                    p['delta'] = int(value.replace(',', ''))  # 증감은 단순 - 가 없음
                elif idx == 5:  # 주식 변동 후
                    p['after'] = int(value.replace(',', '').replace('-', '0'))
                elif idx == 6:  # 취득/처분 단가
                    value = value.replace(',', '').replace('-', '0')
                    if re.compile(r'\d+').findall(value):
                        value = int(re.compile(r'\d+').findall(value)[0])
                    else:
                        value = 0
                    p['price'] = value
                elif idx == 7:  # 비고
                    p['remark'] = value.replace('-', '').strip()
            stock_detail.append(p)

        return stock_detail

    def parsing(self, data):
        stock_diff = {}
        for i, d in enumerate(data):
            dcm_no = self.get_dcm_no(d.get('rcept_no'))
            stock_diff[d.get('rcept_no')] = self.get_stock_detail(d.get('rcept_no'), dcm_no)

            self.logger.info(f"parsed: {d.get('rcept_no')} -> {i+1} / {len(data)}")
        return stock_diff

    def run(self, start_date=None, end_date=None):
        if not start_date:
            start_date = get_date(delta=-1)
        if not end_date:
            end_date = get_date(delta=-1)
        
        params = {
            'bgn_de': start_date,
            'end_de': end_date,
            'page_count': 100
        }

        response = self.api_manager.get_json('list', params)
        if response['status'] not in ['000', '013']:
            tg_msg = f"[ERROR] status code - {response['status']}"
            self.logger.info(tg_msg)
            threading.Thread(target=self.tg_manager.send_msg, args=(tg_msg,)).start()
            return

        if response['status'] == '013':
            tg_msg = NO_DATA_MSG
            self.logger.info(tg_msg)
            threading.Thread(target=self.tg_manager.send_msg, args=(tg_msg,)).start()
            return

        data, total_page = response['list'], response['total_page']
        for i in range(2, total_page + 1):
            self.logger.info(f"current page {i} / {total_page}")
            time.sleep(0.5)  # to prevent IP ban

            params['page_no'] = i
            response = self.api_manager.get_json('list', params)
            if response['status'] != '000':
                continue

            data += response['list']

        executive_data = self.get_executive_data(data)
        parsed = self.parsing(executive_data)
        
        # for p in parsed.values():
        #     # p : 보고서단위... (3갲)
        #     self.db_manager.insert()


if __name__ == "__main__":
    s = Snoopy()
    #s.run()
    
    # r = '20201218000643'
    # d = '7717867'
    # s.get_stock_detail(r, d)

    # d = [{'report_nm': '임원ㆍ주요주주특정증권등소유상황보고서', 'corp_cls': 'Y', 'rcept_no': '20201218000643'}]
    # response = s.process_data(d)
    # print(response)

    # TODO
    # 1. db 설계/구성
    # 2. bulk insert
    # 3. 날짜 input
