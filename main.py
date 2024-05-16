from Data_request import fut_candles_request, futures_metadata_update
from Database.postgres_sql import Database
from time_functions import *
from Trading import trade_logic
from TG_bot import tg_bot
import time
from logger import clear_log_file, log_function_info
import threading
import params
import sys


def check_time_analyze(db, trading):
    """Проверяет время последнего анализа данных"""

    while True:
        last_data_analyze = int(db.get_table_from_db("SELECT timestamp_utc_milisec FROM requests_time \
                                 WHERE request = 'data_preprocessing'").loc[0, 'timestamp_utc_milisec'])
        need_analize = has_passed_any_hours(value=last_data_analyze, hours=params.analyze_period)
        print(f'\nNeed_analize = {need_analize}\n')

        if need_analize:
            log_function_info('Запуск анализа данных.')

            futures_metadata_update.futures_metadata_update()  # Обновляем таблицу метаданных
            fut_candles_request.fut_data_request()  # Обновляем таблицы свечных данных

            trading.data_preprocessing()
            trading.data_analyze()
            trading.bollinger_bands_preprocessing()
            trading.bollinger_bands_calcualate()

            clear_log_file()  # Очистить файл логов

        time.sleep(30)

def main(trading):

    cycle = 0
    while True:
        print(f'Цикл {cycle} \n')
        trading.trading()

        cycle += 1
        time.sleep(1)


if __name__ == '__main__':
    db = Database()
    trading = trade_logic.Trading()

    # Анализ данных
    thr_check_time_analyze = threading.Thread(target=check_time_analyze, args=(db, trading,))
    thr_check_time_analyze.start()

    # Трейдинг
    thr_main = threading.Thread(target=main, args=(trading,))
    thr_main.start()

    # tg_bot
    thr_bot = threading.Thread(target=tg_bot.start_bot())
    thr_bot.start()

    # Список потоков
    threads = [thr_check_time_analyze, thr_main, thr_bot]

    # Проверка запущенных потоков
    while True:
        # Получение списка активных потоков
        active_threads = threading.enumerate()
        print('\nСписок активных потоков:')
        for thread in active_threads:
            print(thread.name)

        # Проверка состояния потоков и обработка ошибок
        for thread in threads:
            if not thread.is_alive():
                print(f'Поток {thread.name} завершился с ошибкой или был прерван.')

                # Выход с ошибкой (например, код 1)
                sys.exit(1)

        time.sleep(15)
        print()


