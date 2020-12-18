import threading

from manager.db_manager import DbManager
from manager.tg_manager import TgManager


class Snoopy:
    def __init__(self):
        self.db_manager = DbManager()
        self.tg_manager = TgManager()

    def run(self):
        companies = self.db_manager.select_companies()
        for c in companies:
            print(c)
        # tg_msg = 'Hwangs!!'
        # threading.Thread(target=self.tg_manager.send_message, args=(tg_msg,)).start()


if __name__ == "__main__":
    s = Snoopy()
    s.run()
