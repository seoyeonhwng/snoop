import logging
import os
from manager.utils import get_current_time


class LogManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(LogManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        log_directory = os.path.dirname(os.path.realpath(__file__)) + '/../logs/'
        if not os.path.exists(log_directory):
            os.mkdir(log_directory)

        logging.basicConfig(
            format="{asctime} {levelname:8} {filename:<10} {funcName:<20} {message}",
            style="{",
            level=logging.INFO,
            handlers=[
                logging.FileHandler(log_directory + get_current_time('%Y%m%d') + '.log', mode='a'),
                logging.StreamHandler(),
            ]
        )
        self.logger = logging.getLogger()
