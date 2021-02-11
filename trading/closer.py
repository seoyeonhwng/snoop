import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import threading

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.krx import Krx
from trading.checker import get_profit_ratio

from utils.commons import get_current_time, convert_to_str


class Closer:
    def __init__(self):
        self.logger = LogManager().logger
        self.db_manager = DbManager()
        self.tg_manager = TgManager()
        self.krx = Krx()

    def run(self):
        # 들고 있는 애 업데이트 쳐주기
        holding = self.db_manager.get_holding_trades()
        if not holding:
            self.logger.info('(closer)current no holding')
        else:
            close_target = []
            today = get_current_time('%Y%m%d', -2)  # TODO. change date
            ticker = self.krx.get_ticker_info(today)
            if not ticker:
                self.logger.info(f"(checker)check expected ratio: no ticker")
                return

            total_corporates = self.db_manager.get_total_corporates()
            tg_msg = f'[CLOSED]\n'
            for h in holding:
                corp_name = [c.get('corp_name') for c in total_corporates if c.get('stock_code') == h.get('stock_code')][0]
                sell_price = [t.get('close') for t in ticker if t.get('stock_code') == h.get('stock_code')][0]
                last_price = h.get('buy_price') if not h.get('last_price') else h.get('last_price')
                profit_ratio = h.get('profit_ratio')
                params = {
                    'stock_code': h.get('stock_code'),
                    'buy_date': convert_to_str(h.get('buy_date'), '%Y%m%d'),
                    'last_price': sell_price,
                    'profit_ratio': get_profit_ratio(sell_price, last_price, profit_ratio),
                    'last_updated_at': get_current_time()
                }
                result = self.db_manager.update_trading_closer(params)
                if not result:
                    self.logger.critical(f'error on closer update')
                    return
                tg_msg += f"{params.get('buy_date')} / {corp_name} / {params.get('profit_ratio')}%\n"

            threading.Thread(target=self.tg_manager.send_warning_message, args=(tg_msg,)).start()


if __name__ == "__main__":
    c = Closer()
    c.run()
