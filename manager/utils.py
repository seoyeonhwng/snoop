import os
import json
from datetime import datetime, timedelta


def read_config():
    with open(os.path.dirname(os.path.realpath(__file__)) + '/../config.json', 'r') as _config_file:
        return json.load(_config_file)


def get_date(delta):
    """
    오늘부터 delta만큼의 날짜를 반환
    """
    date = (datetime.today() + timedelta(days=delta)).strftime('%Y%m%d')
    return date
