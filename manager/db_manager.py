import pymysql.cursors

from manager.utils import read_config


class DbManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DbManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.config = read_config().get("mysql")
        self.conn = self.__connect()

    def __connect(self):
        return pymysql.connect(host=self.config.get("host"),
                               user=self.config.get("user"),
                               password=self.config.get("password"),
                               db=self.config.get("db"),
                               port=self.config.get("port"),
                               charset=self.config.get("charset"))

    def __execute(self, query):
        if not self.conn:
            self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)

        try:
            cur.execute(query)
            result = cur.fetchall()
        except Exception as e:
            msg = f'[Error in execute query]\n{e}'
            msg += f'\n\nQuery : {query}'
            print(msg) # TODO log 처리 또는 telegram

            cur.close()
            return None
        
        cur.close()
        return result

    def __execute_values(self, query, values):
        if not self.conn:
            self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)

        try:
            cur.executemany(query, values)
        except Exception as e:
            msg = f'[Error in execute_values query]\n{e}'
            msg += f'\n\nQuery : {query}'
            print(msg) # TODO log 처리 또는 telegram

            cur.close()
            self.conn.rollback()
            return False
        
        cur.close()
        self.conn.commit()
        return True

    def __execute_commit(self, query):
        if not self.conn:
            self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)

        try:
            cur.execute(query)
        except Exception as e:
            msg = f'[Error in execute_commit query]\n{e}'
            msg += f'\n\nQuery : {query}'
            print(msg) # TODO log 처리 또는 telegram

            cur.close()
            self.conn.rollback()
            return False
        
        cur.close()
        self.conn.commit()
        return True

    def delete_table(self, table):
        query = f"DELETE FROM {table} "
        return self.__execute_commit(query)

    def insert_bulk_row(self, table, params):
        query = f'INSERT INTO {table} ({", ".join(params[0].keys())}) VALUES ({", ".join(["%s"] * len(params[0]))})'
        return self.__execute_values(query, [tuple(p.values()) for p in params])

    def update_or_insert_corporate(self, data):
        query = "INSERT INTO `corporate` " \
              "(`stock_code`, `corp_code`, `corp_name`, `corp_shorten_name`, `industry_code`, `is_validated`, " \
              "`market`, `market_capitalization`, `market_rank`, `updated_at`) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
              "ON DUPLICATE KEY UPDATE `corp_name` = VALUES(`corp_name`), `industry_code` = VALUES(`industry_code`), `is_validated` = VALUES(`is_validated`), " \
              "`market` = VALUES(`market`), `market_capitalization` = VALUES(`market_capitalization`), `market_rank` = VALUES(`market_rank`), `updated_at` = VALUES(`updated_at`) "
        return self.__execute_values(query, data)

    def get_disclosure_data(self, date):
        query = "SELECT stock_code, corp_name, market, market_capitalization, market_rank, industry_name, count(*) AS count " \
              "FROM (SELECT e.rcept_no, e.stock_code, c.corp_name, c.market, c.market_capitalization, c.market_rank, i.industry_name " \
              "FROM dtnn.executive AS e LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
              "LEFT JOIN dtnn.industry AS i ON c.industry_code = i.industry_code " \
              "WHERE e.disclosed_on = '{date}' AND e.reason_code IN ('01', '02') " \
              "AND e.stock_type IN ('01', '02') " \
              "GROUP BY e.rcept_no, e.stock_code, c.corp_name, c.market, c.market_capitalization, c.market_rank, i.industry_name) " \
              "AS daily_exe GROUP BY stock_code, corp_name, market, market_capitalization, market_rank, industry_name"
        query = query.format(date=date)
        return self.__execute(query)

    def unvalidate_corporates(self):
        query = 'UPDATE `corporate` ' \
              'SET `is_validated` = False'
        return self.__execute_commit(query)

    def get_industry_list(self):
        query = "SELECT * FROM `industry` "
        return self.__execute(query)

    def select_ticker_info(self, date):
        query = "SELECT * FROM `ticker` WHERE `business_date` = '{date}'"
        query = query.format(date=date)
        return self.__execute(query)

    def is_valid_nickname(self, nickname):
        query = "SELECT * FROM user WHERE nickname = '{nickname}'"
        query = query.format(nickname=nickname)
        return self.__execute(query)

    def is_valid_chatid(self, chat_id):
        query = "SELECT * FROM user WHERE chat_id = '{chat_id}'"
        query = query.format(chat_id=chat_id)
        return self.__execute(query)

    def get_targets(self):
        query = f"SELECT chat_id FROM user WHERE is_paid = True AND is_active = True AND expired_at > CURDATE()"
        return self.__execute(query)

    def get_corporate_info(self, corp_name):
        query = "SELECT c.corp_name, c.market, c.market_rank, c.market_capitalization " \
              "FROM dtnn.corporate AS c WHERE c.corp_name = '{corp_name}'"
        query = query.format(corp_name=corp_name)
        return self.__execute(query)

    def get_executive_detail(self, corp_name, target_date):
        query = "SELECT e.* FROM dtnn.executive AS e LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
              "WHERE c.corp_name = '{corp_name}' AND e.disclosed_on = '{target_date}'"
        query = query.format(corp_name=corp_name, target_date=target_date)
        return self.__execute(query)
