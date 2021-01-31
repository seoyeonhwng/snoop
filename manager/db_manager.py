import pymysql.cursors

from manager.log_manager import LogManager
from utils.config import HOST, USER, PASSWORD, DB, PORT, CHARSET
from utils.commons import get_current_time


class DbManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DbManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.logger = LogManager().logger
        self.conn = None

    def __connect(self):
        return pymysql.connect(host=HOST,
                               user=USER,
                               password=PASSWORD,
                               db=DB,
                               port=PORT,
                               charset=CHARSET)

    def __execute(self, query):
        self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute(query)
            result = cur.fetchall()
        except Exception as e:
            msg = f'[Error in execute query]\n{e}'
            msg += f'\n\nQuery : {query}'
            self.logger.critical(msg)

            cur.close()
            self.conn.close()
            return None
        
        cur.close()
        self.conn.close()
        return result

    def __execute_values(self, query, values):
        self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.executemany(query, values)
        except Exception as e:
            msg = f'[Error in execute_values query]\n{e}'
            msg += f'\n\nQuery : {query}'
            self.logger.critical(msg)

            cur.close()
            self.conn.rollback()
            self.conn.close()
            return False
        
        cur.close()
        self.conn.commit()
        self.conn.close()
        return True

    def __execute_commit(self, query):
        self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute(query)
        except Exception as e:
            msg = f'[Error in execute_commit query]\n{e}'
            msg += f'\n\nQuery : {query}'
            self.logger.critical(msg)

            cur.close()
            self.conn.rollback()
            self.conn.close()
            return False
        
        cur.close()
        self.conn.commit()
        self.conn.close()
        return True

    def delete_table(self, table):
        query = f"DELETE FROM {table} "
        return self.__execute_commit(query)

    def insert_row(self, table, params):
        query = f'INSERT INTO {table} ({", ".join(params.keys())}) VALUES ({", ".join(["%s"] * len(params.values()))})'
        return self.__execute_values(query, [tuple(params.values())])

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

    def get_disclosure_data(self, start_date, end_date):
        query = "SELECT e.disclosed_on, e.rcept_no, e.stock_code, e.delta_volume, e.unit_price, c.corp_code, c.corp_name, c.market, c.market_capitalization, c.market_rank, i.industry_name " \
                "FROM dtnn.executive AS e LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
                "LEFT JOIN dtnn.industry AS i ON c.industry_code = i.industry_code " \
                "WHERE (e.disclosed_on BETWEEN '{start_date}' AND '{end_date}') AND e.reason_code IN ('01', '02') AND e.stock_type IN ('01') " \
                "AND (c.industry_code is not null and c.market_capitalization != '') "
        query = query.format(start_date=start_date, end_date=end_date)
        return self.__execute(query)

    def unvalidate_corporates(self):
        query = 'UPDATE `corporate` ' \
              'SET `is_validated` = False'
        return self.__execute_commit(query)

    def get_industry_list(self):
        query = "SELECT * FROM `industry` ORDER BY order_id ASC"
        return self.__execute(query)

    def select_ticker_info(self, date):
        query = "SELECT * FROM `ticker` WHERE `business_date` = '{date}'"
        query = query.format(date=date)
        return self.__execute(query)

    def is_valid_nickname(self, nickname):
        query = "SELECT * FROM user WHERE nickname = '{nickname}'"
        query = query.format(nickname=nickname)
        return False if self.__execute(query) else True

    def get_user_info(self, chat_id):
        query = "SELECT * FROM user WHERE chat_id = '{chat_id}'"
        query = query.format(chat_id=chat_id)
        return self.__execute(query)

    def get_targets(self):
        query = f"SELECT chat_id FROM user WHERE is_paid = True AND is_active = True AND expired_at > CURDATE()"
        return self.__execute(query)

    def get_corporate_info(self, corp_name):
        query = "SELECT c.corp_name, c.market, c.market_rank, c.market_capitalization " \
              "FROM dtnn.corporate AS c WHERE c.corp_name = '{corp_name}'" \
              "AND (c.industry_code is not null and c.market_capitalization != '')"
        query = query.format(corp_name=corp_name)
        return self.__execute(query)

    def get_tg_detail_data(self, corp_name, target_date):
        query = "SELECT e.* FROM dtnn.executive AS e LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
              "WHERE c.corp_name = '{corp_name}' AND e.disclosed_on = '{target_date}'"
        query = query.format(corp_name=corp_name, target_date=target_date)
        return self.__execute(query)

    def get_tg_company_data(self, corp_name, count):
        query = "SELECT * FROM dtnn.executive as e INNER JOIN " \
                "(SELECT e.rcept_no FROM dtnn.executive AS e " \
		        "LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
	            "WHERE e.reason_code IN ('01', '02') AND e.stock_type IN ('01', '02') " \
                "AND c.corp_name = '{corp_name}' AND (c.industry_code is not null and c.market_capitalization != '') " \
                "GROUP BY e.rcept_no ORDER BY rcept_no DESC LIMIT {count} " \
                ") AS tmp ON e.rcept_no = tmp.rcept_no"
        query = query.format(corp_name=corp_name, count=count)
        return self.__execute(query)

    def get_tg_executive_data(self, corp_name, executive_name, count):
        query = "SELECT * FROM dtnn.executive as e INNER JOIN " \
                "(SELECT e.rcept_no FROM dtnn.executive AS e " \
		        "LEFT JOIN dtnn.corporate AS c ON e.stock_code = c.stock_code " \
	            "WHERE e.reason_code IN ('01', '02') AND e.stock_type IN ('01', '02') " \
                "AND c.corp_name = '{corp_name}' " \
                "AND (c.industry_code is not null and c.market_capitalization != '') " \
                "AND e.executive_name = '{executive_name}' " \
                "GROUP BY e.rcept_no ORDER BY rcept_no DESC LIMIT {count} " \
                ") AS tmp ON e.rcept_no = tmp.rcept_no"
        query = query.format(corp_name=corp_name, executive_name=executive_name, count=count)
        return self.__execute(query)

    def get_last_business_date(self, end_date=get_current_time('%Y%m%d'), delta=1):
        query = "SELECT DISTINCT business_date FROM ticker " \
                "WHERE business_date <= '{end_date}' " \
                "ORDER BY business_date DESC " \
                "LIMIT {delta}"
        query = query.format(end_date=end_date, delta=delta)
        return self.__execute(query)[-1].get('business_date')

    def get_highest_price(self, last_business_date):
        query = "SELECT MAX((high * 1)) AS highest_price FROM ticker " \
                "WHERE business_date = '{last_business_date}'"
        query = query.format(last_business_date=last_business_date)
        return self.__execute(query)[0].get('highest_price')

    def get_frequency_info(self, period, target_date):
        query = "SELECT * FROM frequency WHERE period = '{period}' AND business_date = '{target_date}'"
        query = query.format(period=period, target_date=target_date)
        return self.__execute(query)

    def get_ticker_info(self, stock_code, target_date):
        query = "SELECT * FROM ticker WHERE stock_code = '{stock_code}' AND business_date = '{target_date}'"
        query = query.format(stock_code=stock_code, target_date=target_date)
        return self.__execute(query)[0]
