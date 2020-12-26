import requests
import re
import json

from manager.db_manager import DbManager
from manager.utils import get_current_time

BASE_URL = 'https://m.stock.naver.com/sise'


class Naver:
    def __init__(self):
        #self.db_manager = DbManager()
        pass

    def get_industry_list(self):
        url = 'https://finance.naver.com/sise/sise_group.nhn?type=upjong'
        resp = requests.get(url)
        if resp.status_code != 200:
            print('[ERROR] status code != 200 in get_industry_list')
            return
        
        exp = r'<a href="/sise/sise_group_detail.nhn\?type=upjong&no=([0-9]+)">(.+)</a>'
        data = re.findall(exp, resp.text)

        industry_list = [d + (get_current_time(), ) for d in data]
        return industry_list
    
    def get_industry_corporates_list(self, industry_code):
        url = f'https://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no={industry_code}'
        resp = requests.get(url)
        if resp.status_code != 200:
            print('[ERROR] status code != 200 in get_industry_corporates_list')
            return
        
        exp = r'<a href="/item/main.nhn\?code=([0-9]+)">(.+)</a>'
        corporates_list = re.findall(exp, resp.text)
        return corporates_list

    def update_industry(self):
        self.db_manager.delete_industry()
        self.db_manager.insert_industry(self.__get_industry_list())

    def update_industry_corporate(self):
        # corporates 테이블을 row를 업데이트
        # corporates DB에 industry_code를 채운다.(update)
        # industry별로 업데이트
        
        pass
