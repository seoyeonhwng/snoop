import requests
import json
import numpy as np
from pandas import DataFrame

from manager.log_manager import LogManager
from utils.commons import get_current_time

BASE_URL = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'


class Krx:
    def __init__(self):
        self.logger = LogManager().logger

    def get_empty_ticker(self, k, v, _market, _target_date, _market_rank):
        ticker = {
            'stock_code': k,
            'business_date': _target_date,
            'open': v.get('open'),
            'high': v.get('high'),
            'low': v.get('low'),
            'close': v.get('close'),
            'volume': v.get('volume'),
            'quote_volume': v.get('quote_volume'),
            'market_capitalization': v.get('market_capitalization'),
            'market': _market,
            'market_rank': _market_rank,
            'market_ratio': v.get('market_ratio'),  # TODO. None이고 실제로 우리 안 쓰면 컬럼도 뺄까?
            'operating_share': v.get('operating_share'),
            'created_at': get_current_time(),
        }
        return ticker

    def get_ticker_info(self, _target_date, _market='KOSPI'):
        krx_market = {'KOSPI': 'STK', 'KOSDAQ': 'KSQ'}
        p = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'mktId': krx_market.get(_market),
            'trdDd': _target_date
        }
        resp = requests.post(BASE_URL, headers={"User-Agent": "Mozilla/5.0"}, data=p)

        if resp.status_code != 200:
            self.logger.critical(f'[ERROR] status code != 200 in get_ticker_info {_market}')
            return

        df = DataFrame(json.loads(resp.text).get('OutBlock_1'))
        df = df[['MKT_NM', 'ISU_ABBRV', 'ISU_SRT_CD', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'MKTCAP', 'LIST_SHRS']]
        df.columns = ['market', 'corp_name', 'stock_code', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'market_capitalization', 'operating_share']
        if df.iloc[0]['open'] == '-':  # 휴일에는 - 반환하므로 return
            return
        df = df.replace('[^-\w\.]', '', regex=True)
        df = df.replace('\-$', '0', regex=True)
        df = df.replace('', '0')
        df = df.set_index('stock_code')
        df = df.astype({
            'open': np.int64, 'high': np.int64, 'low': np.int64, 'close': np.int64, 'volume': np.int64,
            'quote_volume': np.int64, 'market_capitalization': np.int64, 'operating_share': np.int64})
        tickers = df.sort_values(by='market_capitalization', ascending=False).to_dict('index').items()

        ticker_info = []
        for i, (k, v) in enumerate(tickers):
            ticker_info.append(self.get_empty_ticker(k, v, _market, _target_date, i + 1))
        return ticker_info
