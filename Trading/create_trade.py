import pandas as pd
import time_functions
from Data_request.futures_last_price_request import last_price_request
from params import trading_params
from Trading import secondary_functions, place_fut_orer
from Data_request import get_future_account_balance


def create_trade(db, last_bollinger_bands: pd.DataFrame, df_open_positions: pd.DataFrame):
    """Ищет точки входа"""

    blacklist = db.get_table_from_db('SELECT * from blacklist')
    blacklist_history = pd.DataFrame()

    pairs_list = last_bollinger_bands['pair'].tolist()
    pairs_list_uniq = [item for pair in pairs_list for item in pair.split('/')]
    symbols_list_uniq = list(set(pairs_list_uniq))
    last_prices = last_price_request(symbols_list_uniq)

    # Получить размер депозита и рассчитать торговый объём
    balance, available_balance, total_unrealized_profit = get_future_account_balance.get_future_account_balance()
    print(f'Общий баланс: {balance}. Свободных средств: {available_balance}. Нереализованный PnL: {total_unrealized_profit}\n')
    volume_base = balance * trading_params['percent_for_trade']

    def spread_to_bl(blacklist, blacklist_history, reason):
        # Добавить спред в чёрный список
        new_row_blacklist = {'spread': row.pair, 'time': time_now, 'timestamp': timestamp_now,
                             'reason': reason}
        blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])
        blacklist.reset_index(drop=True)
        db.add_table_to_db(blacklist, 'blacklist', 'replace')
        blacklist_history = pd.concat(
            [blacklist_history, pd.DataFrame.from_records([new_row_blacklist])])
        db.add_table_to_db(blacklist_history, 'blacklist_history', 'append')

    # Проверить объём использования маржи
    if available_balance >= balance * trading_params['min_totalPositionInitialMargin']:

        for index, row in last_bollinger_bands.iterrows():
            time_now, timestamp_now = time_functions.time_now()

            if row.pair not in df_open_positions['spread'].tolist()  \
                    and (df_open_positions.shape[0] < trading_params['quantity_positions']) \
                    and row.pair not in blacklist['spread'].tolist() and \
                    (trading_params['dif_perc_min'] < row.dif_percentage < trading_params['dif_perc_max']):

                symbol_1, symbol_2 = row.pair.split('/')
                last_price_symbol_1 = last_prices[symbol_1]
                last_price_symbol_2 = last_prices[symbol_2]
                spread_price = last_price_symbol_1 / last_price_symbol_2

                def calculate_volume(entry_price, stop_loss_price):
                    price_diff_percent = abs(((stop_loss_price - entry_price) / entry_price) * 100)

                    if 0 <= price_diff_percent < 2.5:
                        volume_percent = 100
                    elif price_diff_percent > 0:
                        volume_percent = 100 / (price_diff_percent / 1.7)
                    else:
                        volume_percent = 0

                    return volume_percent / 100

                if spread_price <= row.lb:
                    """signal = 'Buy' """

                    direction = 'buy'
                    stop_loss = spread_price - row.sd * trading_params['sl_std']
                    full_move = row.sma200 - row.lb
                    tp_80 = row.sma200 - full_move * trading_params['tp_80']
                    tp_50 = row.sma200 - full_move * trading_params['tp_50']
                    tp_30 = row.sma200 - full_move * trading_params['tp_30']
                    take_profit_percent = abs((spread_price / tp_80 - 1) * 100)

                    # Рассчитать торговый объём на сделку с поправкой на волатильность
                    volume_percent = calculate_volume(spread_price, stop_loss)
                    volume_for_trade = round((volume_base * volume_percent), 2)

                    if volume_for_trade < 36:
                        volume_for_trade = 36

                    volume_third_part = round((volume_for_trade / 3), 2)

                    # Оценить целесообразность совершения сделки исходя из ликвидности
                    execute_spread_prise, symbol_1_price, symbol_2_price, symbol_1_quantity, symbol_2_quantity, \
                        symbol_1_third_part, symbol_2_third_part = secondary_functions.evaluate_liquidity_trade(
                        direction, symbol_1, symbol_2, last_price_symbol_1, last_price_symbol_2, spread_price,
                        volume_for_trade, take_profit_percent)

                    if execute_spread_prise is None or symbol_1_price is None or symbol_2_price is None or symbol_1_quantity is None or symbol_2_quantity is None:
                        print(f'[ERROR] Ошибка! Функция evaluate_liquidity_trade вернула None!')
                        spread_to_bl(blacklist, blacklist_history, 'evaluate_liquidity_trade_error')
                        return None

                    if execute_spread_prise:

                        factor = 0.005
                        symbol_1_limit_price = symbol_1_price + symbol_1_price * factor  # buy limit price
                        symbol_2_limit_price = symbol_2_price - symbol_2_price * factor  # sell limit price
                        rounded_limit_price_1 = secondary_functions.price_rounding(symbol_1, symbol_1_limit_price)
                        rounded_limit_price_2 = secondary_functions.price_rounding(symbol_2, symbol_2_limit_price)

                        place_fut_orer.create_future_order(symbol=symbol_1, side='BUY', order_type='LIMIT',
                                                           quantity=symbol_1_quantity, price=rounded_limit_price_1)
                        place_fut_orer.create_future_order(symbol=symbol_2, side='SELL', order_type='LIMIT',
                                                           quantity=symbol_2_quantity, price=rounded_limit_price_2)

                        new_position = {'open_time': time_now, 'open_timestamp': timestamp_now, 'spread': row.pair,
                                        'direction': direction, 'open_prise': execute_spread_prise, 'sl': stop_loss,
                                        'tp_30': tp_30, 'tp_50': tp_50, 'tp_80': tp_80, 'full_volume': volume_for_trade,
                                        'curr_volume': volume_for_trade, 'vol_30': volume_third_part,
                                        'vol_50': volume_third_part, 'vol_80': volume_third_part,

                                        'symbol_1_full_quantity': symbol_1_quantity, 'symbol_1_curr_quantity': symbol_1_quantity,
                                        'symbol_1_price': symbol_1_price, 'symbol_1_30_quantity': symbol_1_third_part,
                                        'symbol_1_50_quantity': symbol_1_third_part, 'symbol_1_80_quantity': symbol_1_third_part,

                                        'symbol_2_full_quantity': symbol_2_quantity, 'symbol_2_curr_quantity': symbol_2_quantity,
                                        'symbol_2_price': symbol_2_price, 'symbol_2_30_quantity': symbol_2_third_part,
                                        'symbol_2_50_quantity': symbol_2_third_part, 'symbol_2_80_quantity': symbol_2_third_part,

                                        'result_perc': 0, 'result_usd': 0, 'close_time': None, 'close_reason': None}

                        print(f'Открыта позиция {row.pair} {direction} по цене {execute_spread_prise}')
                        df_open_positions = pd.concat([df_open_positions, pd.DataFrame.from_records([new_position])])

                    else:
                        # Добавить спред в чёрный список
                        new_row_blacklist = {'spread': row.pair, 'time': time_now, 'timestamp': timestamp_now,
                                             'reason': 'low_liquidity'}
                        blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])
                        blacklist.reset_index(drop=True)
                        db.add_table_to_db(blacklist, 'blacklist', 'replace')
                        blacklist_history = pd.concat(
                            [blacklist_history, pd.DataFrame.from_records([new_row_blacklist])])
                        db.add_table_to_db(blacklist_history, 'blacklist_history', 'append')

                elif spread_price >= row.ub:
                    """signal = 'Sell' """

                    direction = 'sell'
                    stop_loss = spread_price + row.sd * trading_params['sl_std']
                    full_move = row.sma200 - row.ub
                    tp_80 = row.sma200 - full_move * trading_params['tp_80']
                    tp_50 = row.sma200 - full_move * trading_params['tp_50']
                    tp_30 = row.sma200 - full_move * trading_params['tp_30']
                    take_profit_percent = abs((spread_price / tp_80 - 1) * 100)

                    # Рассчитать торговый объём на сделку с поправкой на волатильность
                    volume_percent = calculate_volume(spread_price, stop_loss)
                    volume_for_trade = round((volume_base * volume_percent), 2)

                    if volume_for_trade < 36:
                        volume_for_trade = 36

                    volume_third_part = round((volume_for_trade / 3), 2)

                    # Оценить целесообразность совершения сделки исходя из ликвидности
                    execute_spread_prise, symbol_1_price, symbol_2_price, symbol_1_quantity, symbol_2_quantity, \
                        symbol_1_third_part, symbol_2_third_part = secondary_functions.evaluate_liquidity_trade(
                        direction, symbol_1, symbol_2, last_price_symbol_1, last_price_symbol_2, spread_price,
                        volume_for_trade, take_profit_percent)

                    if execute_spread_prise is None or symbol_1_price is None or symbol_2_price is None or symbol_1_quantity is None or symbol_2_quantity is None:
                        print(f'[ERROR] Ошибка! Функция evaluate_liquidity_trade вернула None!')
                        spread_to_bl(blacklist, blacklist_history, 'evaluate_liquidity_trade_error')
                        return None

                    if execute_spread_prise:

                        factor = 0.005
                        symbol_1_limit_price = symbol_1_price - symbol_1_price * factor  # buy limit price
                        symbol_2_limit_price = symbol_2_price + symbol_2_price * factor  # sell limit price
                        rounded_limit_price_1 = secondary_functions.price_rounding(symbol_1, symbol_1_limit_price)
                        rounded_limit_price_2 = secondary_functions.price_rounding(symbol_2, symbol_2_limit_price)

                        place_fut_orer.create_future_order(symbol=symbol_1, side='SELL', order_type='LIMIT',
                                                           quantity=symbol_1_quantity, price=rounded_limit_price_1)
                        place_fut_orer.create_future_order(symbol=symbol_2, side='BUY', order_type='LIMIT',
                                                           quantity=symbol_2_quantity, price=rounded_limit_price_2)

                        new_position = {'open_time': time_now, 'open_timestamp': timestamp_now, 'spread': row.pair,
                                        'direction': direction, 'open_prise': execute_spread_prise, 'sl': stop_loss,
                                        'tp_30': tp_30, 'tp_50': tp_50, 'tp_80': tp_80, 'full_volume': volume_for_trade,
                                        'curr_volume': volume_for_trade, 'vol_30': volume_third_part,
                                        'vol_50': volume_third_part, 'vol_80': volume_third_part,

                                        'symbol_1_full_quantity': symbol_1_quantity,
                                        'symbol_1_curr_quantity': symbol_1_quantity,
                                        'symbol_1_price': symbol_1_price, 'symbol_1_30_quantity': symbol_1_third_part,
                                        'symbol_1_50_quantity': symbol_1_third_part,
                                        'symbol_1_80_quantity': symbol_1_third_part,

                                        'symbol_2_full_quantity': symbol_2_quantity,
                                        'symbol_2_curr_quantity': symbol_2_quantity,
                                        'symbol_2_price': symbol_2_price, 'symbol_2_30_quantity': symbol_2_third_part,
                                        'symbol_2_50_quantity': symbol_2_third_part,
                                        'symbol_2_80_quantity': symbol_2_third_part,

                                        'result_perc': 0, 'result_usd': 0, 'close_time': None,
                                        'close_reason': None}

                        print(f'Открыта позиция {row.pair} {direction} по цене {execute_spread_prise}')
                        df_open_positions = pd.concat([df_open_positions, pd.DataFrame.from_records([new_position])])

                    else:
                        spread_to_bl(blacklist, blacklist_history, 'low_liquidity')

                df_open_positions = df_open_positions.reset_index(drop=True)
                db.add_table_to_db(df_open_positions, 'open_positions', 'replace')

            elif not (-trading_params['dif_perc_max'] < row.dif_percentage < trading_params['dif_perc_min']):
                # Добавить спред в чёрный список
                if row.pair not in blacklist['spread'].tolist():
                    print(f'\n[INFO] Спред {row.pair} отправлен в чёрный список, т.к. dif_percentage = {row.dif_percentage}.\n')
                    new_row_blacklist = {'spread': row.pair, 'time': time_now, 'timestamp': timestamp_now,
                                         'reason': f'dif_perc'}
                    blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])
                    blacklist.reset_index(drop=True)
                    db.add_table_to_db(blacklist, 'blacklist', 'replace')
                    blacklist_history = pd.concat(
                        [blacklist_history, pd.DataFrame.from_records([new_row_blacklist])])
                    db.add_table_to_db(blacklist_history, 'blacklist_history', 'append')

            elif row.pair in blacklist['spread'].tolist():
                print(f'\n[INFO] Спред {row.pair} в чёрном списке, сделка с ним не будет совершена!\n')

            elif not (df_open_positions.shape[0] < trading_params['quantity_positions']):
                print(f'\n[INFO] Сделка не была совершена по спреду {row.pair} т.к. превышено максимальное кол-во открытых позиций!'
                      f'Позиций открыто: {df_open_positions.shape[0]}\n')




