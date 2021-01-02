import requests
import json
from urllib.request import urlopen

from utils.config import CRTFC_KEY

BASE_URL = 'https://opendart.fss.or.kr/api'


class ApiManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ApiManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.session = requests.session()

    def get_json(self, api, p):
        p['crtfc_key'] = CRTFC_KEY
        url_parameter = '&'.join([f'{key}={value}' for key, value in p.items()])
        resp = self.session.get(f'{BASE_URL}/{api}.json?{url_parameter}')
        return json.loads(resp.text)

    def get_xml(self, api, p):
        p['crtfc_key'] = CRTFC_KEY
        url_parameter = '&'.join([f'{key}={value}' for key, value in p.items()])
        return urlopen(f'{BASE_URL}/{api}.xml?{url_parameter}')
