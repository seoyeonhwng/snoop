import requests
import threading
import time

from bs4 import BeautifulSoup

from datetime import datetime, timedelta

from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager

MAIN = "https://dart.fss.or.kr"
POPUP_URL = "/dsaf001/main.do?rcpNo={rcept_no}"
SNOOP_URL = "/report/viewer.do?dtd=dart3.xsd&eleId=4&offset=1&length=1&rcpNo={rcept_no}&dcmNo={dcm_no}"


def get_dcm_no(_rcept_no):
    r = requests.get(MAIN + POPUP_URL.format(rcept_no=_rcept_no))
    for href in BeautifulSoup(r.text, 'lxml').find('div', class_='view_search').find_all('li')[:1]:
        return href.find('a')['onclick'].split(' ')[1].replace("'", '').replace(');', '')


def get_stock_diff(_rcept_no, _dcm_no):
    diff = []
    r = requests.get(MAIN + SNOOP_URL.format(rcept_no=_rcept_no, dcm_no=_dcm_no))
    return diff


def buy_or_sell(_rcept_no, _dcm_no):
    pass


class Snoopy:
    def __init__(self):
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.api_manager = ApiManager()

    def process_data(self, data):
        # report name에 임원을 거른다.
        executive_data = []
        for d in data:
            if d['report_nm'] == '임원ㆍ주요주주특정증권등소유상황보고서' and d['corp_cls'] in ['Y', 'K']:
                dcm_no = get_dcm_no(d['rcept_no'])
                print(dcm_no)
                stock_diff = get_stock_diff(d['rcept_no'], dcm_no)
                # signal = buy_or_sell(d['rcept_no'], dcm_no)
                executive_data.append(d)

        return executive_data
        # 순수 매입, 매도만 거르고 싶다...

    def run(self):
        # companies = self.db_manager.select_companies()
        # for c in companies:
        #     print(c)
        # tg_msg = 'Hwangs!!'
        # threading.Thread(target=self.tg_manager.send_message, args=(tg_msg,)).start()
        yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
        
        params = {
            'bgn_de': yesterday,
            'end_de': yesterday,
            'page_count': 100
        }

        response = self.api_manager.get_json('list', params)
        if response['status'] not in ['000', '013']:
            # error message를 나한테만 알려줬으면 좋겠다..
            print('error!!!!!!!')
            pass

        if response['status'] == '013':
            # no message -> 오늘 데이터 없습니다. 
            # 메시지 전송하고 끝!
            print('no message!!!!!')
            pass

        data, total_page = self.process_data(response['list']), response['total_page']
        for i in range(2, total_page + 1):
            time.sleep(0.5)  # to prevent IP ban
            params['page_no'] = i
            response = self.api_manager.get_json('list', params)
            if response['status'] != '000':
                continue

            data += self.process_data(response['list'])

        print(data)


if __name__ == "__main__":
    s = Snoopy()
    # s.run()

    d = [{'report_nm': '임원ㆍ주요주주특정증권등소유상황보고서', 'corp_cls': 'Y', 'rcept_no': '20201218000643'}]
    response = s.process_data(d)
    print(response)
