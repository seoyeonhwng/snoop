import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import threading

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.krx import Krx

from utils.commons import get_current_time, convert_to_str
from utils.config import MINIMUM_PROFIT


def get_term_ratio(open, close):
    return round((float(close) - float(open)) / float(open) * 100, 3)


def get_profit_ratio(sell_price, last_price, profit_ratio):
    term_ratio = get_term_ratio(last_price, sell_price)
    if not profit_ratio:
        return term_ratio
    else:
        return round(((1 + float(profit_ratio) / 100) * (1 + float(term_ratio) / 100) - 1) * 100, 3)


class Checker:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.krx = Krx()

    def run(self):
        holding = self.db_manager.get_holding_trades()
        if not holding:
            self.logger.info('(checker)current no holding')
        else:
            sell_target = []
            today = get_current_time('%Y%m%d')
            ticker = self.krx.get_ticker_info(today)
            if not ticker:
                self.logger.info(f"(checker)check expected ratio: no ticker")
                return

            total_corporates = self.db_manager.get_total_corporates()
            for h in holding:
                corp_name = [c.get('corp_name') for c in total_corporates if c.get('stock_code') == h.get('stock_code')][0]
                current_price = [t.get('close') for t in ticker if t.get('stock_code') == h.get('stock_code')][0]
                last_price = h.get('buy_price') if not h.get('last_price') else h.get('last_price')  # 오늘 산 경우 고려

                expected = get_profit_ratio(current_price, last_price, h.get('profit_ratio'))

                self.logger.info(f"(checker)check expected ratio {h.get('buy_date')} / {corp_name} / {last_price} / {current_price} / {str(expected)}")

                if expected > MINIMUM_PROFIT:
                    sell_target.append({'buy_date': h.get('buy_date'), 'corp_name': corp_name, 'profit_ratio': str(expected)})

            if sell_target:
                tg_msg = f'[need to SELL]\n'
                for t in sell_target:
                    tg_msg += f"{convert_to_str(t.get('buy_date'), '%Y%m%d')} / {t.get('corp_name')} / {t.get('profit_ratio')}%\n"
                threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()


if __name__ == "__main__":
    c = Checker()
    c.run()
