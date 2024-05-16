from binance import Client
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
import os

load_dotenv()

client = Client(os.getenv('api_key'), os.getenv('api_secret'))

bot = Bot(os.getenv('BOT_TOKEN'), parse_mode=ParseMode.HTML)
dp = Dispatcher(bot=bot, parse_mode=ParseMode.HTML)


