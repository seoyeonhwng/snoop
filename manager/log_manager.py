import os
import logging
from logging.handlers import TimedRotatingFileHandler

from utils.commons import get_current_time


class LogManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(LogManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        log_directory = os.path.dirname(os.path.realpath(__file__)) + '/../logs/'
        if not os.path.exists(log_directory):
            os.mkdir(log_directory)

        logging.basicConfig(level=logging.INFO,
                            format="{asctime} {levelname:8} {filename:<15} {message}",
                            style="{",
                            handlers=[
                                TimedRotatingFileHandler(log_directory + get_current_time('%Y%m%d') + '.log',
                                                         when="midnight",
                                                         interval=1),
                                logging.StreamHandler(),
                            ])
        self.logger = logging.getLogger()
