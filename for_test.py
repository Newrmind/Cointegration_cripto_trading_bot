import logging
import time
import threading
import pandas as pd
import datetime
from Database import postgres_sql
from params import postgres_connection_server
from time_functions import time_now
from connection import client

import logger
import requests

class TelegramHandler:
    """Handler to send message telegram"""

    def __init__(self):
        self.token = "6472814197:AAFVYgWZSJVy4srRCQmmiq92yo6SZFXAqfw"
        self.chat_id = 503034116

    def send_message(self, message: str):
        """Send message to tg."""
        url = f'https://api.telegram.org/bot{self.token}/sendMessage'
        data = {'chat_id': self.chat_id, 'text': message}
        requests.post(url=url, json=data)


def test():
    try:
        x = 1 / 0

    except Exception as ex:
        tg = TelegramHandler()
        message_text = f'[ERROR] {ex}'
        tg.send_message(message=message_text)

test()

# def create_tg_logger(name: str) -> logging.Logger:
#     logger = logging.getLogger(name)
#     logger.setLevel(level=logging.DEBUG)
#     handler =



