import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from collections import defaultdict
import datetime

from manager.db_manager import DbManager
from utils.commons import convert_to_str
from utils.config import MINIMUM_TOTAL_AMOUNT, STRONG_TOTAL_AMOUNT

MAX_BUSINESS_DATE = 900
MINIMUM_PROFIT = 10.0

EXCEPT_CORP = ['005880', '002070', '000100', '003460', '004150', '123010', '043290', '221610', '073570', '096690', '044380', '214310', '087730', '054780', '101400', '017650', '290650', '042520']


def calculate_term(open_price, close_price):
    return round((float(close_price) - float(open_price))/float(open_price)*100, 3)


def calculate_accum(rate_before, rate_after):
    return round(((1+float(rate_before)/100)*(1+float(rate_after)/100)-1)*100, 3)


class A:
    def __init__(self):
        self.db_manager = DbManager()

    def run(self):
        """
            total_expected = {
                '005380': [{'i': 'i', 'd': 'd', 'o': '1', 'c': '2', 't': '3', 'a': '4'}, ...],
                '096770': [{'i': 'i', 'd': 'd', 'o': '1', 'c': '2', 't': '3', 'a': '4'}, ...]
            }
        :return:
        """
        final_list = []
        total_company = self.db_manager.get_total_company()

        total_expected = defaultdict(list)
        result = defaultdict(int)

        start, end = datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2020, 12, 31, 0, 0)
        target_date_list = []

        for d in range(MAX_BUSINESS_DATE):
            business_date = self.db_manager.get_last_business_date(delta=d+1)
            if business_date < start or business_date > end:
                continue
            target_date_list.append(business_date)
        target_date_list = sorted(set(target_date_list))

        stock_infos = []  # 해당 날짜에 불꽃 BUY라면 대상 회사 리스트에 넣고
        for target_date in target_date_list:
            disclosure_data = self.db_manager.get_disclosure_data(target_date, target_date)

            groupby_rcept = defaultdict(list)
            for d in disclosure_data:
                groupby_rcept[d['rcept_no']].append(d)

            corp_infos = {}
            for rcept in groupby_rcept.values():
                total_amount = sum([r['delta_volume'] * r['unit_price'] for r in rcept])
                if abs(total_amount) < MINIMUM_TOTAL_AMOUNT:
                    continue

                stock_code, corp_name = rcept[0]['stock_code'], rcept[0]['corp_name']
                if stock_code not in corp_infos:
                    corp_infos[stock_code] = {
                        'corp_name': corp_name.replace('-', '\-').replace('.', '\.'),
                        'count': 1,
                        'max_total_amount': total_amount
                    }
                else:
                    corp_infos[stock_code]['count'] += 1
                    if abs(total_amount) >= abs(corp_infos[stock_code]['max_total_amount']):
                        corp_infos[stock_code]['max_total_amount'] = total_amount

            for k, v in corp_infos.items():
                if v.get('max_total_amount') >= STRONG_TOTAL_AMOUNT and k not in EXCEPT_CORP:
                    stock_infos.append({'stock_code': k, 'added_date': target_date})

        # import datetime
        # stock_infos = [{'stock_code': '039490', 'added_date': datetime.datetime(2020, 12, 8, 0, 0)}]
        for value in stock_infos:
            stock_code = value.get('stock_code')
            added_date = value.get('added_date')
            fire = f'{stock_code}_{convert_to_str(added_date, "%Y%md%d")}'

            # 위에서 저장한 발생 시점으로부터 끝까지
            for target in target_date_list:
                if target > added_date:
                    company_name = [c.get('name') for c in total_company if c.get('stock_code') == stock_code][0]
                    print(stock_code, company_name, target)
                    ticker = self.db_manager.get_ticker_info(stock_code, target)
                    o, c = ticker.get('open'), ticker.get('close')
                    if int(o) == 0 or int(c) == 0:
                        continue

                    if total_expected.get(fire):  # 이미 한번 나온 값이면
                        init_i = total_expected.get(fire)[0].get('i')
                        init_o = total_expected.get(fire)[0].get('init_o')
                        latest_c = total_expected.get(fire)[-1].get('latest_c')
                        latest_a = total_expected.get(fire)[-1].get('a')
                        t = calculate_term(latest_c, c)
                        a = calculate_accum(latest_a, t)

                        expected = {'i': init_i, 'd': target, 'init_o': init_o, 'latest_c': c, 't': t, 'a': a}
                        total_expected[fire].append(expected)
                    else:  # 최초 등장하는 회사라면
                        init_target = target
                        t = a = calculate_term(o, c)

                        expected = {'i': target, 'd': target, 'init_o': o, 'latest_c': c, 't': t, 'a': a}
                        total_expected[fire] = [expected]

                    if total_expected.get(fire)[-1].get('a') >= MINIMUM_PROFIT:
                        final_list.append({'stock_code': stock_code, 'company_name': company_name, 'init_target': init_target, 'target': target})
                        print(f" =======> {fire} / {len(total_expected.get(fire))} / {total_expected.get(fire)[-1].get('a')}")
                        result[len(total_expected.get(fire))] += 1
                        break

            print('')

        # print(total_expected)
        print('================================')
        print(final_list)
        print(result)
        print(len(stock_infos))  # 전체 불
        return
        final = {}
        for key, value in total_expected.items():
            final[key] = value[-1]

        win = lose = 0
        for key, value in final.items():
            if float(value.get('a')) > 0.0:
                win += 1
            else:
                lose += 1
        # for _, f in total_expected.items():
        #     if float(f[0].get('a')) > 0.0:
        #         win += 1
        #     else:
        #         lose += 1

        print('')
        print(final)
        print('')
        print(f'MAX_BUSINESS_DATE: {MAX_BUSINESS_DATE} / win: {win} / lose: {lose} / total: {len(final)}')


if __name__ == "__main__":
    a = A()
    a.run()

