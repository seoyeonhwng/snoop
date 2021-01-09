from manager.tg_manager import TgManager


class Test:
    def __init__(self):
        self.tg_manager = TgManager()

    def run(self):
        msg = "HJH TEST\n"
        msg += 'http://naver.com'
        self.tg_manager.send_warning_message(msg)


if __name__ == "__main__":
    t = Test()
    t.run()
