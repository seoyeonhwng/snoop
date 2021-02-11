import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from manager.log_manager import LogManager
from manager.db_manager import DbManager
from manager.tg_manager import TgManager
from manager.krx import Krx


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


if __name__ == "__main__":
    c = Closer()
    c.run()
