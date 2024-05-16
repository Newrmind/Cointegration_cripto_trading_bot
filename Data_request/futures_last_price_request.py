from connection import client
import time
from logger import log_function_info, error_inf

def last_price_request(symbols):
    log_function_info("Старт функции last_price_request()")
    print(f'Запрос цен для тикеров: {symbols}')

    last_trades = {}

    retries = 10  # Максимальное количество попыток
    while retries > 0:
        try:
            last_prices = client.futures_symbol_ticker()

            if last_prices:
                for element in last_prices:
                    if element['symbol'] in symbols:
                        last_trade_price = float(element['price'])
                        last_trades[element['symbol']] = last_trade_price
                break  # Выход из цикла при успешном запросе

        except Exception as ex:
            error_inf(ex)
            if "Read timed out" in str(ex):
                print("Произошла ошибка: Read timed out (время ожидания истекло)")
            else:
                print(f"Произошла ошибка: {ex}")
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток
    else:
        print(f"Не удалось получить данные для {symbols} после нескольких попыток")

    return last_trades




