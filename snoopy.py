import threading
from datetime import datetime, timedelta

from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.api_manager import ApiManager


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
            'bgn_de' : yesterday,
            'end_de' : yesterday,
            'page_count' : 100
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
            params['page_no'] = i
            response = self.api_manager.get_json('list', params)
            if response['status'] != '000':
                continue

            data += self.process_data(response['list'])

        
        print(data)
        


if __name__ == "__main__":
    s = Snoopy()
    s.run()
