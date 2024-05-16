from connection import client
from logger import log_function_info, error_inf
import time


def execute_order(symbol, side, oder_quantity):
    """Оценивает ликвидность в стакане."""

    log_function_info("Старт функции execute_order()")
    retries = 5  # Максимальное количество попыток
    while retries > 0:
        try:

            if side not in ['buy', 'sell']:
                raise ValueError("Параметр 'side' должен быть 'buy' или 'sell'.")

            # Получение стакана
            depth = client.futures_order_book(symbol=symbol, limit=1000)

            total_price = 0.0
            total_quantity = 0.0

            if side == 'buy':
                best_prise = float(depth['bids'][0][0])
                data = depth['asks']

            elif side == 'sell':
                best_prise = float(depth['asks'][0][0])
                data = depth['bids']

            for price, quantity in data:
                price = float(price)
                quantity = float(quantity)
                if total_quantity + quantity <= oder_quantity:
                    total_price += price * quantity
                    total_quantity += quantity
                else:
                    # Если сумма количества сделок достаточна для покупки oder_quantity
                    # только частично, то вычисляем средневзвешенную цену для этой части
                    remaining_quantity = oder_quantity - total_quantity
                    total_price += price * remaining_quantity
                    total_quantity += remaining_quantity
                    break

            if total_quantity > 0:
                weighted_avg_price = total_price / total_quantity
                if total_quantity < oder_quantity:
                    print(f"\nНевозможно совершить сделку на {oder_quantity} {symbol}. ")
                    print(f"Доступно для {side}: {total_quantity:.3f} {symbol} по средней цене {weighted_avg_price:.2f}.")
                    print(f"Средняя цена отличается от лучшей цены на {(weighted_avg_price / best_prise * 100 - 100):.2f}%\n")
                    return None
                else:
                    print(f"Средневзвешенная цена {oder_quantity:.3f} {symbol}: {weighted_avg_price:.5f}")
                    print(f"Средняя цена отличается от лучшей цены на {(weighted_avg_price / best_prise * 100 - 100):.4f}%\n")

                    # Округляем цену
                    decimal_places = str(best_prise)[::-1].find('.')
                    if decimal_places > 0:
                        weighted_avg_price = round(weighted_avg_price, decimal_places)
                    else:
                        weighted_avg_price = int(weighted_avg_price)

                    return weighted_avg_price
            else:
                print(f"Недостаточно данных для выполнения покупки актива {symbol}.")
                print(f"total_quantity {symbol} = {total_quantity}.")
                return None

        except Exception as e:
            print(f"Ошибка при запросе стакана: {e}")
            error_inf(e)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток
            return None
