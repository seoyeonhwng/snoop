from zipfile import ZipFile
from io import BytesIO
import xml.etree.ElementTree as Et


from pykrx import stock
from manager.utils import get_current_time
from manager.db_manager import DbManager
from manager.dart import Dart


class DataFactory:
    def __init__(self):
        self.db_manager = DbManager()
        self.dart = Dart()

    def get_empty_corporate(self, _stock_code, _corp_code, _corp_name):
        p = {
            'stock_code': _stock_code,
            'corp_code': _corp_code,
            'corp_name': _corp_name,
            'updated_at': get_current_time()
        }
        return p

    def up_to_date_corporates(self):
        corporates = {d.get('stock_code'): d for d in self.db_manager.get_corporate_infos()}

        resp = self.dart.get_xml('corpCode', {})
        with ZipFile(BytesIO(resp.read())) as zf:
            file_list = zf.namelist()
            while len(file_list) > 0:
                file_name = file_list.pop()
                xml = zf.open(file_name).read().decode()
                tree = Et.fromstring(xml)
                stock_list = tree.findall("list")

                stock_code = [x.findtext("stock_code") for x in stock_list]
                corp_code = [x.findtext("corp_code") for x in stock_list]
                corp_name = [x.findtext("corp_name") for x in stock_list]

                for i, company in enumerate(stock_code):
                    if company.replace(" ", ""):  # stock_code 있는 경우만
                        target = self.get_empty_corporate(stock_code[i], corp_code[i], corp_name[i])
                        if corporates and corporates.get(stock_code[i]):
                            print(f"step1) continue {stock_code[i]} / {corp_name[i]}")
                            continue
                        else:
                            print(f"step1) insert {stock_code[i]} / {corp_name[i]}")
                            self.db_manager.insert_corporate_infos(target)

    def apply_daily_ticker(self, _target_date):
        markets = ["KOSPI", "KOSDAQ"]
        for market in markets:
            ticker_df = stock.get_market_ohlcv_by_ticker(_target_date, market)
            ticker_df = ticker_df.rename({'종목명': 'corp_name', '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close',
                                          '거래량': 'volume', '거래대금': 'quote_volume', '시가총액': 'market_capitalization',
                                          '시총비중': 'market_ratio', '상장주식수': 'operating_share'}, axis='columns')
            # ticker_df = ticker_df.drop('corp_name', axis=1)
            market_rank = 1
            for k, v in ticker_df.sort_values(by='market_capitalization', ascending=False).to_dict('index').items():
                v['stock_code'] = k
                v['business_date'] = _target_date
                v['market'] = market
                v['market_rank'] = market_rank
                v['created_at'] = get_current_time()
                market_rank += 1
                self.db_manager.insert_ticker(v)
                self.db_manager.update_corporate_infos(v)

    def run(self):
        tg_msg = ""
        # up to date corporate list from DART (빈껍데기를 만들고)
        # step1 = "1) up_to_date_corporates"
        # try:
        #     self.up_to_date_corporates()
        # except Exception:
        #     tg_msg += f"{step1} ERROR\n"
        #     # send tg_msg
        # tg_msg += f"{step1} DONE\n"

        # for each market, daily insert ticker / update on company
        # target_date = get_current_time('%Y%m%d', -1)
        target_date = '20201224'
        step2 = "2) apply_daily_ticker"
        try:
            self.apply_daily_ticker(target_date)
        except Exception:
            tg_msg += f"{step2} ERROR\n"
            # send tg_msg
        tg_msg += f"{step2} DONE\n"

        # update industry_code from naver
        step3 = "3) update industry_code from naver"
        try:
            print('update industry_code from naver')
            # self.apply_daily_ticker(target_date)
        except Exception:
            tg_msg += f"{step3} ERROR\n"
            # send tg_msg
        tg_msg += f"{step3} DONE\n"


if __name__ == "__main__":
    d = DataFactory()
    d.run()
