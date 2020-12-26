import pymysql.cursors

from manager.utils import read_config, get_current_time


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

    def insert_executive(self, data):
        sql = "INSERT INTO `executive` " \
              "(`rcept_no`, `disclosed_on`, `stock_code`, `executive_name`, `reason_code`, `traded_on`, `stock_type`, " \
              "`before_volume`, `delta_volume`, `after_volume`, `unit_price`, `remark`, `created_at`) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.executemany(sql, data)
        self.commit()

    def insert_user(self, data):
        sql = "INSERT INTO `user` " \
              "(`chat_id`, `nickname`, `role`, `is_paid`, `is_active`, `created_at`, `expired_at`, `canceled_at`) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

        try:
            self.cursor.execute(sql, tuple(data.values()))
            self.commit()
        except Exception as e:
            print('Error!!! {}'.format(e))
            return False
        return True

    def get_company_infos(self):  # to be deprecated
        sql = 'SELECT `stock_code`, `corp_code`, `name` ' \
              'FROM `company` '
        self.cursor.execute(sql, ())
        return self.cursor.fetchall()

    def get_corporate_infos(self):
        sql = 'SELECT `stock_code`, `corp_code`, `corp_name` ' \
              'FROM `corporate` '
        self.cursor.execute(sql, ())
        return self.cursor.fetchall()

    def insert_corporate_infos(self, data):
        sql = "INSERT INTO `corporate` " \
              "(`stock_code`, `corp_code`, `corp_name`, `updated_at`) " \
              "VALUES (%s, %s, %s, %s)"
        self.cursor.execute(sql, tuple(data.values()))
        self.commit()

    def update_corporate_infos(self, p):
        sql = 'UPDATE `corporate` ' \
              'SET `corp_name` = %s, `last_business_date` = %s, `market` = %s, `market_capitalization` = %s, `market_rank` = %s, `updated_at` = %s ' \
              'WHERE `stock_code` = %s '
        self.cursor.execute(sql, (
            p.get('corp_name'), p.get('business_date'), p.get('market'), p.get('market_capitalization'), p.get('market_rank'), p.get('created_at'),
            p.get('stock_code')))
        self.commit()

    def is_valid_nickname(self, nickname):
        sql = "SELECT * FROM user WHERE nickname = %s"
        self.cursor.execute(sql, (nickname))
        return False if self.cursor.fetchall() else True

    def is_valid_chatid(self, chat_id):
        sql = "SELECT * FROM user WHERE chat_id = %s"
        self.cursor.execute(sql, (chat_id))
        return False if self.cursor.fetchall() else True

    def get_targets(self):
        sql = f"SELECT chat_id FROM user WHERE is_paid = True AND is_active = True AND expired_at > CURDATE()"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_admin(self):
        sql = "SELECT chat_id FROM user WHERE role = '01'"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_disclosure_data(self, date):
        sql = "SELECT * FROM executive WHERE disclosed_on = %s AND reason_code IN ('01', '02') AND stock_type IN ('01', '02')"
        self.cursor.execute(sql, (date))
        return self.cursor.fetchall()

    def insert_ticker(self, p):
        sql = "INSERT INTO `ticker` " \
              "(`stock_code`, `business_date`, `open`, `high`, `low`, `close`, `volume`, " \
              "`quote_volume`, `market_capitalization`, `market_rank`, `market_ratio`, `operating_share`, `created_at`) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.execute(sql, (
            p.get('stock_code'), p.get('business_date'), p.get('open'), p.get('high'), p.get('low'), p.get('close'), p.get('volume'),
            p.get('quote_volume'), p.get('market_capitalization'), p.get('market_rank'), p.get('market_ratio'), p.get('operating_share'), p.get('created_at'))
        )
        self.commit()
