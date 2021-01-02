import requests
import re

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from utils.commons import get_current_time

BASE_URL = 'https://m.stock.naver.com/sise'


class Naver:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()

    def __get_industry_list(self):
        url = 'https://finance.naver.com/sise/sise_group.nhn?type=upjong'
        resp = requests.get(url)
        if resp.status_code != 200:
            self.logger.critical('[ERROR] status code != 200 in get_industry_list')
            return
        
        exp = r'<a href="/sise/sise_group_detail.nhn\?type=upjong&no=([0-9]+)">(.+)</a>'
        data = re.findall(exp, resp.text)

        industry_list = []
        for d in data:
            p = {
                'industry_code': d[0],
                'industry_name': d[1],
                'created_at': get_current_time()
            }
            industry_list.append(p)
        return industry_list
    
    def __get_industry_corporates_list(self, industry_code):
        url = f'https://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no={industry_code}'
        resp = requests.get(url)
        if resp.status_code != 200:
            self.logger.critical('[ERROR] status code != 200 in get_industry_corporates_list')
            return
        
        exp = r'<a href="/item/main.nhn\?code=([0-9]+)">(.+)</a>'
        corporates_list = re.findall(exp, resp.text)
        return corporates_list

    def update_industry(self):
        self.db_manager.delete_table('industry')
        if not self.db_manager.insert_bulk_row('industry', self.__get_industry_list()):
            self.logger.critical('[ERROR] in update_industry')
            return

    def fill_industry_corporate(self, _corporates):
        industry_list = self.db_manager.get_industry_list()
        industry_map = {}
        for ind in industry_list:
            data = self.__get_industry_corporates_list(ind.get('industry_code'))
            tmp = {d[0]: ind.get('industry_code') for d in data}
            industry_map.update(tmp)

        for c in _corporates:
            c['industry_code'] = industry_map.get(c['stock_code'])
        return _corporates
