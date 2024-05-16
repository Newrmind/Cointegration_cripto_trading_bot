from Trading import execute_order, place_fut_orer
from connection import client
import time
from logger import log_function_info, error_inf
from decimal import Decimal
from params import trading_params


def calculate_trade_quantity(symbol, exchange_info, symbol_prise, volume):
    """Рассчитывает объём заявки исходя из минимального шага цены"""

    log_function_info("Старт функции calculate_trade_quantity()")
    retries = 5  # Максимальное количество попыток
    while retries > 0:
        try:
            # Найдем настройки для данной торговой пары
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == symbol:
                    for filter_info in symbol_info['filters']:
                        if filter_info['filterType'] == 'PRICE_FILTER':
                            price_precision = float(filter_info['tickSize'])  # Минимальный шаг цены
                        elif filter_info['filterType'] == 'LOT_SIZE':
                            lot_size = float(filter_info['stepSize'])  # Размер лота

            if symbol_prise > lot_size:
                remainder = symbol_prise % price_precision
                result = symbol_prise - remainder
            else:
                result = symbol_prise

            decimal_places = str(lot_size)[::-1].find('.')
            trade_quantity = volume / result

            if decimal_places > 0 and lot_size < 1:
                trade_quantity = round(trade_quantity, decimal_places)
            else:
                trade_quantity = int(trade_quantity)

            print(f'[INFO] calculate_trade_quantity symbol {symbol}, trade_quantity = {trade_quantity}')
            return trade_quantity

        except Exception as ex:
            print(f"Произошла ошибка в функции calculate_trade_quantity: {ex}")
            error_inf(ex)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток

def evaluate_liquidity_trade(direction, symbol_1, symbol_2, last_price_symbol_1, last_price_symbol_2, spread_price,
                             volume_for_trade, take_profit_percent):
    """Оценивает целесообразность совершения сделки исходя из ликвидности в стакане"""

    log_function_info("Старт функции evaluate_liquidity_trade()")

    print(f'\nОценка ликвидности спреда {symbol_1}/{symbol_2}')

    # Получить объём сделки для каждого инструмента
    exchange_info = client.futures_exchange_info()
    volume_for_calc_quant = volume_for_trade / 3

    symbol_1_third_part = calculate_trade_quantity(symbol_1, exchange_info, last_price_symbol_1, volume_for_calc_quant)
    symbol_2_third_part = calculate_trade_quantity(symbol_2, exchange_info, last_price_symbol_2, volume_for_calc_quant)

    if symbol_1_third_part == 0 or symbol_2_third_part == 0:
        return None, None, None, None, None, None, None

    symbol_1_quantity = symbol_1_third_part * 3
    symbol_2_quantity = symbol_2_third_part * 3

    decimal_places_symbol_1 = str(symbol_1_third_part)[::-1].find('.')
    if decimal_places_symbol_1 > 0:
        symbol_1_quantity = round(symbol_1_quantity, decimal_places_symbol_1)

    decimal_places_symbol_2 = str(symbol_2_third_part)[::-1].find('.')
    if decimal_places_symbol_2 > 0:
        symbol_2_quantity = round(symbol_2_quantity, decimal_places_symbol_2)

        # Получить средневзвешенную цену для каждого инструмента
    if direction == 'buy':
        symbol_1_execute = execute_order.execute_order(symbol_1, 'buy', symbol_1_quantity)
        symbol_2_execute = execute_order.execute_order(symbol_2, 'sell', symbol_2_quantity)

        if symbol_1_execute and symbol_2_execute:
            # Рассчитать проскальзывание и целесообразность совершения сделки
            execute_spread_prise = symbol_1_execute / symbol_2_execute
            spread_slippage_percent = abs((spread_price / execute_spread_prise - 1) * 100)
            impact_on_outcome = spread_slippage_percent / take_profit_percent * 100
            if impact_on_outcome < 10:
                print(f'Влияние проскальзывания на результат = {impact_on_outcome}%, сделка целесообразна')
                return execute_spread_prise, symbol_1_execute, symbol_2_execute, symbol_1_quantity, \
                    symbol_2_quantity, symbol_1_third_part, symbol_2_third_part
            else:
                print(f'Влияние проскальзывания на результат = {impact_on_outcome}%, сделка нецелесообразна')
                return None, symbol_1_execute, symbol_2_execute, symbol_1_quantity, symbol_2_quantity, \
                    symbol_1_third_part, symbol_2_third_part
        else:
            print('[ERROR] symbol_1_execute or symbol_2_execute is None!')
            print(f'[ERROR] symbol_1_execute = {symbol_1_execute}, symbol_2_execute = {symbol_2_execute}')

    elif direction == 'sell':
        symbol_1_execute = execute_order.execute_order(symbol_1, 'sell', symbol_1_quantity)
        symbol_2_execute = execute_order.execute_order(symbol_2, 'buy', symbol_2_quantity)

        if symbol_1_execute and symbol_2_execute:
            # Рассчитать проскальзывание и целесообразность совершения сделки
            execute_spread_prise = symbol_1_execute / symbol_2_execute
            spread_slippage_percent = abs((spread_price / execute_spread_prise - 1) * 100)
            impact_on_outcome = spread_slippage_percent / take_profit_percent * 100
            if impact_on_outcome < 10:
                print(f'Влияние проскальзывания на результат = {impact_on_outcome}%, сделка целесообразна')
                return execute_spread_prise, symbol_1_execute, symbol_2_execute, symbol_1_quantity, symbol_2_quantity, \
                    symbol_1_third_part, symbol_2_third_part
            else:
                print(f'Влияние проскальзывания на результат = {impact_on_outcome}%, сделка нецелесообразна')
                return None, symbol_1_execute, symbol_2_execute, symbol_1_quantity, symbol_2_quantity, \
                    symbol_1_third_part, symbol_2_third_part
        else:
            print('[ERROR] symbol_1_execute or symbol_2_execute is None!')
            print(f'[ERROR] symbol_1_execute = {symbol_1_execute}, symbol_2_execute = {symbol_2_execute}')


def price_rounding(symbol, prise):
    """Округляет цену входа исходя из минимального шага цены"""
    log_function_info("Старт функции price_rounding()")
    retries = 5  # Максимальное количество попыток
    while retries > 0:

        try:
            exchange_info = client.futures_exchange_info()
            # Найдем настройки для данной торговой пары
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == symbol:
                    for filter_info in symbol_info['filters']:
                        if filter_info['filterType'] == 'PRICE_FILTER':
                            price_precision = float(filter_info['tickSize'])  # Минимальный шаг цены

                            # Округляем цену до ближайшего шага цены
                            rounded_price = round(prise / price_precision) * price_precision

                            price_precision_decimal = Decimal(str(price_precision))
                            decimal_places = abs(price_precision_decimal.as_tuple().exponent)

                            if decimal_places > 0:
                                rounded_price = round(rounded_price, decimal_places)

                            return rounded_price

        except Exception as ex:
            print(f"Произошла ошибка  в функции price_rounding: {ex}")
            error_inf(ex)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток


def get_execute_prices(direction, symbol_1, symbol_2, symbol_1_quantity, symbol_2_quantity):
    """Возвращает средневзвешенную цену закрытия спреда и его элементов"""

    log_function_info("Старт функции get_execute_prices()")
    retries = 10  # Максимальное количество попыток
    while retries > 0:
        try:

            # Получить средневзвешенную цену для каждого инструмента
            if direction == 'buy':
                symbol_1_execute = execute_order.execute_order(symbol_1, 'buy', symbol_1_quantity)
                symbol_2_execute = execute_order.execute_order(symbol_2, 'sell', symbol_2_quantity)
                # Рассчитать проскальзывание и целесообразность совершения сделки
                execute_spread_prise = symbol_1_execute / symbol_2_execute

                return symbol_1_execute, symbol_2_execute, execute_spread_prise

            elif direction == 'sell':
                symbol_1_execute = execute_order.execute_order(symbol_1, 'sell', symbol_1_quantity)
                symbol_2_execute = execute_order.execute_order(symbol_2, 'buy', symbol_2_quantity)

                # Рассчитать проскальзывание и целесообразность совершения сделки
                execute_spread_prise = symbol_1_execute / symbol_2_execute

                return symbol_1_execute, symbol_2_execute, execute_spread_prise

        except Exception as ex:
            print(f"Произошла ошибка  в функции get_execute_prices: {ex}")
            error_inf(ex)
            time.sleep(5)
            retries -= 1  # Уменьшение количества попыток


def get_sl_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction):

    volume = df_open_positions.loc[index_to_update, 'curr_volume']
    symbol_1_quantity = float(df_open_positions.loc[index_to_update, 'symbol_1_curr_quantity'])
    symbol_2_quantity = float(df_open_positions.loc[index_to_update, 'symbol_2_curr_quantity'])

    # получить текущую цену элементов спреда
    symbol_1_execute, symbol_2_execute, execute_spread_prise = get_execute_prices(
        direction, symbol_1, symbol_2, symbol_1_quantity, symbol_2_quantity)

    factor = trading_params['factor_for_sl']

    if direction == 'buy':
        symbol_1_limit_price = symbol_1_execute - symbol_1_execute * factor  # sell limit price
        symbol_2_limit_price = symbol_2_execute + symbol_2_execute * factor  # buy limit price

    elif direction == 'sell':
        symbol_1_limit_price = symbol_1_execute + symbol_1_execute * factor  # buy limit price
        symbol_2_limit_price = symbol_2_execute - symbol_2_execute * factor  # sell limit price

    else:
        print('[ERROR] Произошла ошибка в функции get_position_params')
        symbol_1_limit_price, symbol_2_limit_price = None, None

    rounded_limit_price_1 = price_rounding(symbol_1, symbol_1_limit_price)
    rounded_limit_price_2 = price_rounding(symbol_2, symbol_2_limit_price)

    return volume, symbol_1_quantity, symbol_2_quantity, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise

def get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction, tp_type):

    volume = df_open_positions.loc[index_to_update, f'vol_{tp_type}']

    symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
    symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])

    # получить текущую цену элементов спреда
    symbol_1_execute, symbol_2_execute, execute_spread_prise = get_execute_prices(
        direction, symbol_1, symbol_2, symbol_1_quantity, symbol_2_quantity)

    factor = trading_params['factor_for_tp']

    if direction == 'buy':
        symbol_1_limit_price = symbol_1_execute - symbol_1_execute * factor  # sell limit price
        symbol_2_limit_price = symbol_2_execute + symbol_2_execute * factor  # buy limit price

    elif direction == 'sell':
        symbol_1_limit_price = symbol_1_execute + symbol_1_execute * factor  # buy limit price
        symbol_2_limit_price = symbol_2_execute - symbol_2_execute * factor  # sell limit price

    else:
        print('[ERROR] Произошла ошибка в функции get_position_params')
        symbol_1_limit_price, symbol_2_limit_price = None, None

    rounded_limit_price_1 = price_rounding(symbol_1, symbol_1_limit_price)
    rounded_limit_price_2 = price_rounding(symbol_2, symbol_2_limit_price)

    return volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise


def do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
             rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity):
    if direction_value == 'buy':
        """Закрытие лонговой позиции, т.е. шорт спреда."""
        place_fut_orer.create_future_order(symbol=symbol_1, side='SELL', order_type='LIMIT',
                                           quantity=symbol_1_quantity, price=rounded_limit_price_1)
        place_fut_orer.create_future_order(symbol=symbol_2, side='BUY', order_type='LIMIT',
                                           quantity=symbol_2_quantity, price=rounded_limit_price_2)
    elif direction_value == 'sell':
        """Закрытие шортовой позиции, т.е. лонг спреда."""
        place_fut_orer.create_future_order(symbol=symbol_1, side='BUY', order_type='LIMIT',
                                           quantity=symbol_1_quantity, price=rounded_limit_price_1)
        place_fut_orer.create_future_order(symbol=symbol_2, side='SELL', order_type='LIMIT',
                                           quantity=symbol_2_quantity, price=rounded_limit_price_2)