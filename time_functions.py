import datetime
import time
import pandas as pd



def time_now():
    # Получение текущего времени в формате UTC
    current_time = datetime.datetime.utcnow()
    formatted_datetime = current_time.strftime("%d.%m.%Y %H:%M:%S")

    # Преобразование времени в значение timestamp с миллисекундами
    timestamp = int(current_time.timestamp() * 1000)
    return formatted_datetime, timestamp

def request_time_change(db, request: str):
    """Обновляет время запросов в таблице requests_time"""
    requests_time = db.get_table_from_db('SELECT * FROM requests_time')
    time = time_now()
    requests_time.loc[requests_time['request'] == f'{request}', 'time_utc'] = time[0]
    requests_time.loc[requests_time['request'] == f'{request}', 'timestamp_utc_milisec'] = time[1]
    db.add_table_to_db(requests_time, table_name='requests_time', if_exists='replace')

def calculate_execution_time(func):
    """Функция выполняет измерение времени перед выполнением другой функции и выводит результат"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения функции {func.__name__}: {execution_time:.6f} секунд")
        return result
    return wrapper

def convert_timestamp(timestamp_ms):
    """конвертация timestamp_ms в unix"""
    # Преобразование миллисекунд в объект datetime
    dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0)
    return dt

# округление текущего времени в меньшую сторону для каждого таймфрейма
def round_time(interval: str) -> int:
    timestamp = time.time()
    rounded_timestamp = timestamp

    if interval == '5m':
        interval_5min = 5 * 60  # 5 minutes in seconds
        # Округляем в меньшую сторону до 5 минут
        rounded_timestamp -= timestamp % interval_5min

    elif interval == '15m':
        interval_15min = 15 * 60  # 15 minutes in seconds
        # Округляем в меньшую сторону до 15 минут
        rounded_timestamp -= timestamp % interval_15min

    elif interval == '1h':
        interval_1hr = 60 * 60  # 1 час в секундах
        # Округляем в меньшую сторону до 1 часа
        rounded_timestamp -= timestamp % interval_1hr

    else:
        print('Error!')

    return int(rounded_timestamp * 1000)  # конвертируем в милисекунды

# округление времени первой свечи в большую сторону для каждого таймфрейма
def round_up_time(timestamp: int, interval: str) -> int:
    # Преобразование timestamp в формат datetime
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)

    # Округление времени до ближайшего момента времени, кратного 5 минутам в большую сторону
    if interval == '5m':
        rounded_time = (dt + datetime.timedelta(minutes=5)).replace(second=0, microsecond=0)
        rounded_time = rounded_time + datetime.timedelta(minutes=-(rounded_time.minute % 5))
        # Преобразование округленных временных меток в формат timestamp в миллисекундах
        rounded_time = int(rounded_time.timestamp() * 1000)

    # Округление времени до ближайшего момента времени, кратного 15 минутам в большую сторону
    elif interval == '15m':
        rounded_time = (dt + datetime.timedelta(minutes=15)).replace(second=0, microsecond=0)
        rounded_time = rounded_time + datetime.timedelta(minutes=-(rounded_time.minute % 15))
        # Преобразование округленных временных меток в формат timestamp в миллисекундах
        rounded_time = int(rounded_time.timestamp() * 1000)

    # Округление времени до ближайшего момента времени, кратного 1 часу в большую сторону
    elif interval == '1h':
        rounded_time = (dt + datetime.timedelta(hours=1)).replace(second=0, microsecond=0)
        rounded_time = rounded_time + datetime.timedelta(minutes=-rounded_time.minute,
                                                                     seconds=-rounded_time.second)
        # Преобразование округленных временных меток в формат timestamp в миллисекундах
        rounded_time = int(rounded_time.timestamp() * 1000)
    else:
        rounded_time = timestamp
        print('Error!')

    return rounded_time


def has_passed_any_hours(value: int, hours: float) -> bool:
    """
    Определяет, прошло ли более указанного количества часов с момента заданного времени.

    Параметры:
    - value (int): Время в формате timestamp в миллисекундах.
    - hours (int): Количество часов для проверки.

    Возвращает:
    - bool: True, если прошло более указанного количества часов, False в противном случае.
    """

    current_time_utc = datetime.datetime.utcnow()  # текущее время UTC
    current_time = int(current_time_utc.timestamp() * 1000)

    # Разница между текущим временем и заданным временем в часах
    hours_difference = (current_time - value) / (1000 * 60 * 60)

    return hours_difference > hours


def get_start_time_for_concat(amount_klines: dict, current_time: int = None) -> dict:
    """
        Вычитает из текущего времени указанное в значениях словаря кол-во секунд.

        Параметры:
        - amount_klines (dict): Словарь, с количеством секунд для анализа в значениях.
        - current_time (int): Время, от которого будет производиться отсчёт

        Возвращает:
        - dict: Словарь разностей текущего времени со значениями изначального словаря.
        """

    if current_time is None:
        current_time = datetime.datetime.now()
    else:
        current_time = datetime.datetime.fromtimestamp(current_time / 1000.0)

    rounded_timestamps = {}
    for key, seconds in amount_klines.items():
        rounded_time = current_time - datetime.timedelta(seconds=current_time.second, microseconds=current_time.microsecond)
        rounded_time -= datetime.timedelta(seconds=seconds)
        rounded_timestamps[f'start_time_{key}'] = int(rounded_time.timestamp() * 1000)  # Преобразование в миллисекунды
        rounded_timestamps[f'end_time_{key}'] = int(current_time.timestamp()) * 1000 # Добавляем end_time

    return rounded_timestamps


def add_human_readable_time(df, timestamp_column, new_column):
    # Функция конвертирует timestamp время в человеческое и добавляет его в начало ДФ
    df[new_column] = pd.to_datetime(df[timestamp_column] // 1000, unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    df.insert(0, new_column + '_temp', df[new_column])
    df.drop(columns=[new_column], inplace=True)
    df.rename(columns={new_column + '_temp': new_column}, inplace=True)
    return df

