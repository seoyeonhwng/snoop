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

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

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
        f = lambda x : x['report_nm'] == '임원ㆍ주요주주특정증권등소유상황보고서' and x['corp_cls'] in ['Y', 'K']
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
                    reason_code = {
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
                        '기타': '99'
                    }
                    value = value if reason_code.get(value) is None else reason_code.get(value)
                    p['reason'] = value
                elif idx == 1:  # 변동일
                    conver_date = re.compile(r'\d+').findall(value)
                    year, month, day = conver_date[0], conver_date[1], conver_date[2]
                    p['date'] = date(int(year), int(month), int(day))
                elif idx == 2:  # 증권종류
                    stock_type_code = {
                        '보통주': '01',
                        '우선주': '02',
                        '전환사채권': '03',
                        '신주인수권이표시된것': '04',
                        '신주인수권부사채권': '05'
                    }
                    value = value if stock_type_code.get(value) is None else stock_type_code.get(value)
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

            logger.info(f"parsed: {d.get('rcept_no')} -> {i+1} / {len(data)}")
            # print(stock_diff.get(d.get('rcept_no')))
            # print('--------------------------------------------------------')
        
        return stock_diff

    def run(self):
        # companies = self.db_manager.select_companies()
        # for c in companies:
        #     print(c)
        # tg_msg = 'Hwangs!!'
        # threading.Thread(target=self.tg_manager.send_msg, args=(tg_msg,)).start()
        # threading.Thread(target=self.tg_manager.send_warning_msg, args=(tg_msg,)).start()

        yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
        
        params = {
            'bgn_de': yesterday,
            'end_de': yesterday,
            'page_count': 100
        }

        response = self.api_manager.get_json('list', params)
        if response['status'] not in ['000', '013']:
            # error message를 나한테만 알려줬으면 좋겠다..
            error_msg = f'[ERROR] status code - {response["status"]}'
            self.tg_manager.send_warning_msg(error_msg)
            pass

        if response['status'] == '013':
            # no message -> 오늘 데이터 없습니다. 
            # 메시지 전송하고 끝!
            print('no message!!!!!')
            pass

        data, total_page = response['list'], response['total_page']
        for i in range(2, total_page + 1):
            logger.info(f"current page {i} / {total_page}")
            time.sleep(0.5)  # to prevent IP ban
            params['page_no'] = i
            response = self.api_manager.get_json('list', params)
            if response['status'] != '000':
                continue

            data += response['list']

        executive_data = self.get_executive_data(data)
        print('total data count', len(executive_data))
        parsed = self.parsing(executive_data)

        # for p in parsed:
        #     print('--------------------')
        #     print(p)
        
        # for p in parsed.values():
        #     # p : 보고서단위... (3갲)
        #     self.db_manager.insert()

if __name__ == "__main__":
    s = Snoopy()
    s.run()
    
    # r = '20201218000643'
    # d = '7717867'
    # get_stock_diff(r, d)

    # d = [{'report_nm': '임원ㆍ주요주주특정증권등소유상황보고서', 'corp_cls': 'Y', 'rcept_no': '20201218000643'}]
    # response = s.process_data(d)
    # print(response)

    # TODO
    # 1. db 설계/구성
    # 2. bulk insert
    # 3. 날짜 input
