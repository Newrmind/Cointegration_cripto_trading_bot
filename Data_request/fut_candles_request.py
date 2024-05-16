import time_functions
from connection import client
from Database.postgres_sql import Database
import pandas as pd
from time_functions import request_time_change, calculate_execution_time, convert_timestamp, round_time, round_up_time, add_human_readable_time
from params import *
import time
from logger import log_function_info, error_inf



@calculate_execution_time
def fut_data_request():
    log_function_info("Старт функции fut_data_request()")

    db = Database()
    tickers = db.get_futures_metadata()
    len_tickers = len(tickers['symbol'])

    futures_last_klines = db.get_table_from_db("SELECT * FROM klines_minmax")  # получаем датафрейм с временем первой и последней свечи для каждого тикера

    for interval in intervals:
        ticker_count = 1
        end_time = round_time(interval) # время последней закрытой свечи
        for ticker in tickers['symbol']:
            retries = 5  # Максимальное количество попыток
            while retries > 0:
                try:

                    max_timestamp = futures_last_klines.loc[(futures_last_klines['symbol'] == ticker) & (
                                    futures_last_klines['timeframe'] == interval), 'max_timestamp'].tolist()

                    # Запрашиваем свечи с последней свечи в БД, если свечей нет, то запрашиваем с 1683390600000
                    start_time = [max_timestamp[0]] if len(max_timestamp) != 0 else [start_time_default]
                    start_time = start_time[0]
                    # Округляем время в большую сторону, чтобы избежать дублирования свечей
                    start_time = round_up_time(start_time, interval)
                    count = 1
                    while int(start_time) < int(end_time):
                        print(f'Запрос данных №{count} {ticker} тф {interval}. '
                              f'Номер тикера: {ticker_count} из {len_tickers}')
                        count += 1
                        klines = client.futures_klines(symbol=ticker, interval=interval, startTime=start_time, endTime=end_time, limit=999)
                        df_fut_klines = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'Close Time',
                                                                'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume',
                                                                'Taker Buy Quote Asset Volume', 'Ignore'])

                        # Добавление колонки в начало с именем тикера
                        column_name = 'symbol'
                        df_fut_klines.insert(0, column_name, ticker)
                        df_fut_klines = df_fut_klines.rename(columns={'time': 'timestamp'})

                        df_fut_klines = add_human_readable_time(df_fut_klines, 'timestamp', 'time')
                        df_fut_klines = df_fut_klines.drop(columns=['Ignore', 'Close Time', 'Quote Asset Volume',
                                                                    'Number of Trades', 'Taker Buy Base Asset Volume',
                                                                    'Taker Buy Quote Asset Volume'])

                        # Получаем время первой и последней свечи
                        first_kline = df_fut_klines['timestamp'].min()
                        last_kline = df_fut_klines['timestamp'].max()

                        print(f'Время первой свечи {convert_timestamp(first_kline)}')
                        print(f'Время последней свечи {convert_timestamp(last_kline)}')
                        print(f'Время последней свечи timestamp {last_kline}')
                        start_time = int(last_kline) # устанавливаем новое время старта

                        # добавить свечи в БД
                        db.add_table_to_db(df=df_fut_klines, table_name=f'futures_klines_{interval}', if_exists='append')
                        print('***********************\n')

                        # Обновить время первой и последней свечи в БД
                        db.update_futures_last_klines(intervals=intervals)
                        
                    break

                except Exception as ex:
                    print(f"Произошла ошибка: {ex}")
                    error_inf(ex)
                    time.sleep(10)
                    retries -= 1  # Уменьшение количества попыток

            ticker_count += 1

        request_time_change(db, f'futures_klines_{interval}')  # обновить время последнего запроса в БД


    def check_old_klines():
        """Проверяет, есть ли в базе данных свечи старше полугода, и удаляет их раз в 30 дней."""

        last_del = db.get_table_from_db("SELECT * FROM requests_time WHERE request = 'delete_old_klines'")
        selected_number = last_del.iloc[0, last_del.columns.get_loc('timestamp_utc_milisec')]

        if time_functions.time_now()[1] > (selected_number + 2500000000):
            for interval in intervals:
                print(f'Удаление старых свеч из базы данных для таймфрейма {interval}.\n')
                db.delete_old_klines(interval)

            request_time_change(db, f'delete_old_klines')

    check_old_klines()


if __name__ == '__main__':
    fut_data_request()


