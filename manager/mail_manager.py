import smtplib
from email.mime.text import MIMEText

from manager.log_manager import LogManager

from utils.commons import read_message, get_current_time
from utils.config import MAIL_ID, MAIL_PWD


class MailManager:
    def __init__(self):
        self.logger = LogManager().logger
        self.session = smtplib.SMTP('smtp.gmail.com', 587)

    def send_mail(self, target, title, content):
        self.session.starttls()
        self.session.login(MAIL_ID, MAIL_PWD)

        msg = MIMEText(content)
        msg['Subject'] = title

        for t in target:
            # 개인별 제목/내용 개인화할꺼면 msg 구성을 여기서
            self.session.sendmail(MAIL_ID, t.get('mail'), msg.as_string())
            self.logger.info(f'{t.get("name")}|{t.get("mail")} mail sent')


if __name__ == "__main__":
    m = MailManager()

    _target = list()
    _target.append({'name': '황서연', 'mail': 'trytowinme@naver.com'})
    _target.append({'name': '황지환', 'mail': 'neosouler@gmail.com'})

    _title = f'[SNOOP] 제목test'
    _content = f'여러분~~ \n부자되세요!!'

    m.send_mail(_target, _title, _content)
