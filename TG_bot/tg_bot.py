import asyncio
import threading

import params
from Database import postgres_sql
from connection import dp, bot

from aiogram import types
from aiogram.filters import Command
from aiogram.types import Message


db = postgres_sql.Database()
df_account_info = db.get_table_from_db('SELECT * FROM account_info')
tg_id = db.get_table_from_db("SELECT value FROM account_info WHERE indicator_name = 'tg_id'")['value'].iloc[0]

HELP_COMMAND = """
<b>/start</> - <em>показать список команд;</em>
<b>/balance</> - <em>получить баланс;</em>
<b>/open_pos</> - <em>получить данные по открытым позициям;</em>
<b>/closed_pos</> - <em>получить статистику по закрытым позициям;</em>
<b>/check_threads</> - <em>получить активные потоки;</em>
<b>/check_trading_allowed</> - <em>получить значение параметра trading_allowed;</em>
<b>/change_trading_allowed</> - <em>изменить значение параметра trading_allowed;</em>


"""

@dp.message(Command('start'))
async def command_start_handler(message: Message) -> None:
    try:
        if message.from_user.id == tg_id:
            await bot.send_message(chat_id=message.from_user.id,
                                   text=f'Welcome to main menu.\n{HELP_COMMAND}')
    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('balance'))
async def command_get_balance(message: Message) -> None:
    try:
        if message.from_user.id == tg_id:
            df_account_info = db.get_table_from_db("SELECT * FROM account_info")
            total_wallet_balance = df_account_info.loc[df_account_info['indicator_name'] == 'total_wallet_balance', 'value'].iloc[0]
            available_balance = df_account_info.loc[df_account_info['indicator_name'] == 'available_balance', 'value'].iloc[0]
            unrealized_profit = df_account_info.loc[df_account_info['indicator_name'] == 'unrealized_profit', 'value'].iloc[0]
            position_initial_margin = df_account_info.loc[df_account_info['indicator_name'] == 'position_initial_margin', 'value'].iloc[0]
            total_margin_balance = df_account_info.loc[df_account_info['indicator_name'] == 'total_margin_balance', 'value'].iloc[0]
            account_info_dict = {'Баланс': total_wallet_balance, 'Доступный баланс': available_balance, 'Нереализованный PnL': unrealized_profit,
                                 'Маржа в позициях': position_initial_margin, 'Баланс с учётом PnL': total_margin_balance}
            account_info_str = []

            for key, value in account_info_dict.items():
                account_info_str.append(f"{key}: {round(value, 2)}")

            account_info_str = '\n'.join(account_info_str)
            await bot.send_message(chat_id=message.from_user.id, text=account_info_str)
    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('open_pos'))
async def command_get_open_pos(message: Message) -> None:
    try:
        if message.from_user.id == tg_id:
            open_positions = db.get_table_from_db("""SELECT spread, direction, full_volume AS vol, curr_volume AS cur_vol, curr_result_perc AS cur_perc, 
                                                    curr_result_usd AS cur_usd FROM open_positions""")
            open_positions_str = open_positions.to_string(index=False)
            await bot.send_message(chat_id=message.from_user.id, text=open_positions_str)
    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('closed_pos'))
async def command_get_closed_pos(message: Message) -> None:
    try:
        if message.from_user.id == tg_id:
            closed_positions = db.get_table_from_db(
                """SELECT
                        COUNT(*) AS count_trades,
                        SUM(result_usd) AS total_result_usd,
                        AVG(result_usd) AS avg_result_usd,
                        AVG(result_perc) AS avg_result_perc,
                        COUNT(CASE WHEN close_reason = 'sl' THEN 1 ELSE NULL END) AS count_sl,
                        COUNT(CASE WHEN close_reason = 'tp' THEN 1 ELSE NULL END) AS count_tp
                    FROM
                        closed_positions;""")

            closed_positions_info_str = []
            for column in closed_positions.columns:
                closed_positions_info_str.append(column + ': ' + str(round(closed_positions[column].iloc[0], 2)))

            closed_positions_info = '\n'.join(closed_positions_info_str)
            await bot.send_message(chat_id=message.from_user.id, text=closed_positions_info)

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('check_threads'))
async def command_check_threads(message: Message) -> None:
    """Отправляет список активных потоков"""
    try:
        if message.from_user.id == tg_id:
            active_threads = threading.enumerate()
            active_threads_names = ['\nСписок активных потоков:']
            for thread in active_threads:
                active_threads_names.append(thread.name)
            active_threads_str = '\n'.join(active_threads_names)
            await bot.send_message(chat_id=message.from_user.id, text=active_threads_str)

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('check_trading_allowed'))
async def command_check_threads(message: Message) -> None:
    """Отправляет значение параметра trading_allowed"""
    try:
        if message.from_user.id == tg_id:
            trading_allowed = db.get_table_from_db("SELECT * FROM account_info WHERE indicator_name = 'trading_allowed'")
            trading_allowed_value = trading_allowed.loc[trading_allowed['indicator_name'] == 'trading_allowed', 'value'].iloc[0]
            message_text = f'Параметр trading_allowed_bd = {trading_allowed_value}.\nПараметр trading_allowed_const = {params.trading_allowed}.'
            await bot.send_message(chat_id=message.from_user.id, text=message_text)

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('change_trading_allowed'))
async def command_check_threads(message: Message) -> None:
    """Изменяет значение параметра trading_allowed"""
    try:
        if message.from_user.id == tg_id:
            trading_allowed = db.get_table_from_db("SELECT * FROM account_info")
            trading_allowed_value = trading_allowed.loc[trading_allowed['indicator_name'] == 'trading_allowed', 'value'].iloc[0]

            if trading_allowed_value:
                trading_allowed.loc[trading_allowed['indicator_name'] == 'trading_allowed', 'value'] = 0
            else:
                trading_allowed.loc[trading_allowed['indicator_name'] == 'trading_allowed', 'value'] = 1

            trading_allowed_new = trading_allowed.loc[trading_allowed['indicator_name'] == 'trading_allowed', 'value'].iloc[0]
            db.add_table_to_db(trading_allowed, 'account_info', 'replace')
            message_text = f'Параметр trading_allowed_bd, равный {trading_allowed_value}, изменён на {trading_allowed_new}.'
            await bot.send_message(chat_id=message.from_user.id, text=message_text)

    except TypeError:
        await message.answer("Nice try!")

@dp.message()
async def echo_handler(message: types.Message) -> None:
    try:
        if message.from_user.id == tg_id:
            await bot.send_message(chat_id=message.chat.id, text=HELP_COMMAND)

    except TypeError:
        await message.answer("Nice try!")

async def main() -> None:
    print('\n[INFO] TG bot starting.')
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

def start_bot():
    asyncio.run(main())
