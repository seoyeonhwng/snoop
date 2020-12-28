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

    def delete_table(self, table):
        sql = f"DELETE FROM {table} "
        self.cursor.execute(sql)
        self.commit()

    def insert_bulk_row(self, table, params):
        sql = f'INSERT INTO {table} ({", ".join(params[0].keys())}) VALUES ({", ".join(["%s"] * len(params[0]))})'
        try:
            self.cursor.executemany(sql, [tuple(p.values()) for p in params])
            self.commit()
        except Exception as e:
            print(f"DB INSERT Error {table} => {e}")
            return False
        return True

    def update_or_insert_corporate(self, data):
        sql = "INSERT INTO `corporate` " \
              "(`stock_code`, `corp_code`, `corp_name`, `corp_shorten_name`, `industry_code`, `is_validated`, " \
              "`market`, `market_capitalization`, `market_rank`, `updated_at`) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
              "ON DUPLICATE KEY UPDATE `corp_name` = VALUES(`corp_name`), `industry_code` = VALUES(`industry_code`), `is_validated` = VALUES(`is_validated`), " \
              "`market` = VALUES(`market`), `market_capitalization` = VALUES(`market_capitalization`), `market_rank` = VALUES(`market_rank`), `updated_at` = VALUES(`updated_at`) "
        self.cursor.executemany(sql, data)
        self.commit()

    def get_disclosure_data(self, date):
        sql = "SELECT stock_code, corp_name, market, market_capitalization, market_rank, industry_name, count(*) AS count " \
              "FROM (SELECT e.rcept_no, e.stock_code, c.corp_name, c.market, c.market_capitalization, c.market_rank, i.industry_name " \
              "FROM dtnn.executive AS e LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
              "LEFT JOIN dtnn.industry AS i ON c.industry_code = i.industry_code " \
              "WHERE e.disclosed_on = %s AND e.reason_code IN ('01', '02') " \
              "AND e.stock_type IN ('01', '02') " \
              "GROUP BY e.rcept_no, e.stock_code, c.corp_name, c.market, c.market_capitalization, c.market_rank, i.industry_name) " \
              "AS daily_exe GROUP BY stock_code, corp_name, market, market_capitalization, market_rank, industry_name"
        self.cursor.execute(sql, (date,))
        return self.cursor.fetchall()

    def unvalidate_corporates(self):
        sql = 'UPDATE `corporate` ' \
              'SET `is_validated` = %s'
        self.cursor.execute(sql, (False, ))
        self.commit()

    def select_industry(self):
        sql = "SELECT * FROM `industry` "
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def select_ticker_info(self, date):
        sql = "SELECT * FROM `ticker` WHERE `business_date` = %s "
        self.cursor.execute(sql, (date, ))
        return self.cursor.fetchall()

    def is_valid_nickname(self, nickname):
        sql = "SELECT * FROM user WHERE nickname = %s"
        self.cursor.execute(sql, (nickname, ))
        return False if self.cursor.fetchall() else True

    def is_valid_chatid(self, chat_id):
        sql = "SELECT * FROM user WHERE chat_id = %s"
        self.cursor.execute(sql, (chat_id, ))
        return False if self.cursor.fetchall() else True

    def get_targets(self):
        sql = f"SELECT chat_id FROM user WHERE is_paid = True AND is_active = True AND expired_at > CURDATE()"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_admin(self):
        sql = "SELECT chat_id FROM user WHERE role = '01'"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_corporate_info(self, corp_name):
        sql = "SELECT c.corp_name, c.market, c.market_rank, c.market_capitalization " \
              "FROM dtnn.corporate AS c WHERE c.corp_name = %s"
        self.cursor.execute(sql, (corp_name))
        return self.cursor.fetchone()

    def get_executive_detail(self, corp_name, target_date):
        sql = "SELECT e.* FROM dtnn.executive AS e LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
              "WHERE c.corp_name = %s AND e.disclosed_on = %s"
        self.cursor.execute(sql, (corp_name, target_date))
        return self.cursor.fetchall()
