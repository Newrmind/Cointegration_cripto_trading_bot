from connection import client
import time
from logger import log_function_info, error_inf
from Database import postgres_sql
import pandas as pd
import time_functions

def create_future_order(symbol, side, order_type, quantity, price=None, stop_price=None):
    """Размещает ордер в стакане"""

    log_function_info("Старт функции create_future_order()")
    db = postgres_sql.Database()

    # Определите параметры ордера
    order_params = {
        'symbol': symbol,  # Например, 'BTCUSDT' для пары BTC/USDT
        'side': side,  # 'BUY' для покупки или 'SELL' для продажи
        'type': order_type,  # 'LIMIT' для лимитного ордера или 'MARKET' для рыночного
        'quantity': quantity  # Количество фьючерсов, которое вы хотите купить или продать
    }

    # Добавьте цену и/или стоп-цену, если это лимитный ордер или условный ордер
    if order_type == 'LIMIT':
        order_params['price'] = price
        order_params['timeInForce'] = 'GTC'  # Пример: 'GTC' - ордер будет действовать до отмены
    elif order_type == 'STOP_MARKET':
        order_params['stopPrice'] = stop_price

    retries = 5  # Максимальное количество попыток
    while retries > 0:
        # Выставить ордер и получить ответ
        try:
            order = client.futures_create_order(**order_params)

            if order:
                print(f'Ордер {order} успешно создан!')
                order_df = pd.DataFrame()
                order_df = pd.concat([order_df, pd.DataFrame.from_records([order])])
                order_df = time_functions.add_human_readable_time(order_df, 'updateTime', 'time')

                db.add_table_to_db(order_df, 'orders', 'append')
                break
        except Exception as e:
            error_inf(e)
            retries -= 1  # Уменьшение количества попыток
            print(f"Ошибка при создании ордера: {e}")
            time.sleep(10)

    # Если все попытки завершились неудачей, вернуть None
    return None

