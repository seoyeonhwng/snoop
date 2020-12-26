import requests
import re
import json

from manager.db_manager import DbManager

BASE_URL = 'https://m.stock.naver.com/sise'
class Naver:
    def __init__(self):
        self.db_manager = DbManager()

    def __get_industry_list(self):
        resp = requests.get(f'{BASE_URL}/siseList.nhn?menu=upjong')
        if resp.status_code != 200:
            print('[ERROR] status code != 200 in get_industry_list')
            return

        data = re.findall(r'var jsonData = (.+);', resp.text)
        if not data:
            print('[ERROR] no jsonData in get_industry_list')
            return
 
        industry_list = []
        for d in json.loads(data[0]).get('result').get('groupList'):
            industry_list.append({
                'industry_code': d.get('no'),
                'industry_name': d.get('nm')
            })
        return industry_list

    def __get_industry_corporates_list(self, industry_code):
        resp = requests.get(f'{BASE_URL}/siseGroupDetail.nhn?menu=upjong&no={industry_code}')
        if resp.status_code != 200:
            print('[ERROR] status code != 200 in get_industry_corporates_list')
            return

        data = re.findall(r'var jsonData = (.+);', resp.text)
        if not data:
            print('[ERROR] no jsonData in get_industry_corporates_list')
            return

        corporates_list = []
        for d in json.loads(data[0]).get('result').get('itemList'):
            corporates_list.append({
                'stock_code': d.get('cd'),
                'corp_name': d.get('nm')
            })
        return corporates_list

    def update_industry(self):
        # industry 테이블을 업데이트
        industry_list = self.__get_industry_list()
        pass

    def update_industry_code(self):
        # corporates 테이블을 row를 업데이트
        # corporates DB에 industry_code를 채운다.(update)
        # industry별로 업데이트
        
        pass


if __name__ == "__main__":
    n = Naver()
    tmp = n.get_industry_corporates_list(202)
    print(tmp)