import time
import pymysql.cursors

from manager.utils import read_config


class DbManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DbManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.mysql = read_config().get("mysql")
        self.db = pymysql.connect(host=self.mysql.get("host"),
                                  user=self.mysql.get("user"),
                                  password=self.mysql.get("password"),
                                  db=self.mysql.get("db"),
                                  port=self.mysql.get("port"),
                                  charset=self.mysql.get("charset"))
        self.cursor = self.db.cursor(pymysql.cursors.DictCursor)

    def commit(self):
        self.db.commit()

    def select_companies(self):
        sql = 'SELECT `stock_code`, `corp_code`, `name` ' \
              'FROM `company` '
        self.cursor.execute(sql, ())
        return self.cursor.fetchall()
