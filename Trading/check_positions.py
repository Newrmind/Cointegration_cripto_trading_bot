import pandas as pd
from Data_request.futures_last_price_request import last_price_request
from params import trading_params
import time_functions
from Trading import secondary_functions


def check_positions(database, df_open_positions, df_closed_positions, bollinger_bands):

    pairs_list = df_open_positions['spread'].tolist()
    pairs_list_uniq = [item for pair in pairs_list for item in pair.split('/')]
    symbols_list_uniq = list(set(pairs_list_uniq))
    last_prices = last_price_request(symbols_list_uniq)

    blacklist = database.get_table_from_db('SELECT * from blacklist')
    blacklist_history = pd.DataFrame()

    def change_df_open_positions(df_open_positions, result_percent, result_USD,
                                 close_reason, current_time, current_timestamp):
        df_open_positions.loc[index_to_update, 'result_perc'] = result_percent
        df_open_positions.loc[index_to_update, 'result_usd'] += result_USD
        df_open_positions.loc[index_to_update, 'close_reason'] = close_reason
        df_open_positions.loc[index_to_update, 'close_time'] = current_time
        df_open_positions.loc[index_to_update, 'close_timestamp'] = current_timestamp
        return df_open_positions

    def change_tp(direction_value, df_open_positions, index_to_update, spread):
        pos_open_timestamp = df_open_positions.loc[index_to_update, 'open_timestamp']

        if (pos_open_timestamp + trading_params['transposition_tp'] - current_timestamp) < 0:
            max_timestamp_rows = bollinger_bands.loc[bollinger_bands.groupby('pair')['timestamp'].idxmax()]
            max_timestamp_row = max_timestamp_rows[max_timestamp_rows['pair'] == spread]
            last_bollinger_sma = max_timestamp_row['sma200'].iloc[0]
            last_bollinger_ub = max_timestamp_row['ub'].iloc[0]
            last_bollinger_lb = max_timestamp_row['lb'].iloc[0]

            if direction_value == 'buy':
                full_move = last_bollinger_sma - last_bollinger_lb
                new_tp_30 = last_bollinger_sma - full_move * trading_params['tp_30']
                new_tp_50 = last_bollinger_sma - full_move * trading_params['tp_50']
                new_tp_80 = last_bollinger_sma - full_move * trading_params['tp_80']

                df_open_positions.loc[index_to_update, 'tp_30'] = new_tp_30
                df_open_positions.loc[index_to_update, 'tp_50'] = new_tp_50
                df_open_positions.loc[index_to_update, 'tp_80'] = new_tp_80

            elif direction_value == 'sell':
                full_move = last_bollinger_sma - last_bollinger_ub
                new_tp_30 = last_bollinger_sma - full_move * trading_params['tp_30']
                new_tp_50 = last_bollinger_sma - full_move * trading_params['tp_50']
                new_tp_80 = last_bollinger_sma - full_move * trading_params['tp_80']

                df_open_positions.loc[index_to_update, 'tp_30'] = new_tp_30
                df_open_positions.loc[index_to_update, 'tp_50'] = new_tp_50
                df_open_positions.loc[index_to_update, 'tp_80'] = new_tp_80

        return df_open_positions

    def round_curr_quantity(reference_quantity, curr_quantity):
        decimal_places = str(reference_quantity)[::-1].find('.')
        if decimal_places > 0:
            curr_quantity = round(curr_quantity, decimal_places)
        return curr_quantity

    for index, row in df_open_positions.iterrows():

        symbol_1, symbol_2 = row.spread.split('/')
        last_price_symbol_1 = last_prices[symbol_1]
        last_price_symbol_2 = last_prices[symbol_2]
        spread_price = last_price_symbol_1 / last_price_symbol_2

        spread = row.spread

        if spread in df_open_positions['spread'].tolist():

            # Получить индекс строки, где 'spread' == 'text'
            index_to_update = df_open_positions[df_open_positions['spread'] == spread].index[0]

            current_timestamp = time_functions.time_now()[1]
            current_time = time_functions.time_now()[0]

            direction_value = df_open_positions.loc[index_to_update, 'direction']

            result_30 = df_open_positions.loc[index_to_update, 'result_30']
            result_50 = df_open_positions.loc[index_to_update, 'result_50']
            result_80 = df_open_positions.loc[index_to_update, 'result_80']

            vol_30 = df_open_positions.loc[index_to_update, 'vol_30']
            vol_50 = df_open_positions.loc[index_to_update, 'vol_50']
            vol_80 = df_open_positions.loc[index_to_update, 'vol_80']

            if direction_value == 'buy':

                current_result_percent = round((spread_price / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                df_open_positions.loc[index_to_update, 'curr_result_perc'] = current_result_percent
                current_result_usd = round(df_open_positions.loc[index_to_update, 'curr_volume'] * (current_result_percent / 100), 4)

                if result_30 is not None and not pd.isna(result_30):
                    current_result_usd += result_30
                if result_50 is not None and not pd.isna(result_50):
                    current_result_usd += result_50
                if result_80 is not None and not pd.isna(result_80):
                    current_result_usd += result_80

                df_open_positions.loc[index_to_update, 'curr_result_usd'] = round(current_result_usd, 4)

                # Обновить tp
                df_open_positions = change_tp(direction_value, df_open_positions, index_to_update, spread)

                if spread_price <= df_open_positions.loc[index_to_update, 'sl']:
                    """Stop Loss"""

                    volume, symbol_1_quantity, symbol_2_quantity, rounded_limit_price_1, \
                        rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                        get_sl_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value)

                    # Совершить сделку
                    secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                 rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                    result_percent = round((execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)

                    if result_30 is None or pd.isna(result_30) or result_30 == 0:
                        result_30 = round((vol_30 * (result_percent / 100)), 2)
                        df_open_positions.loc[index_to_update, 'result_30'] = result_30

                    if result_50 is None or pd.isna(result_50) or result_50 == 0:
                        result_50 = round((vol_50 * (result_percent / 100)), 2)
                        df_open_positions.loc[index_to_update, 'result_50'] = result_50

                    if result_80 is None or pd.isna(result_80) or result_80 == 0:
                        result_80 = round((vol_80 * (result_percent / 100)), 2)
                        df_open_positions.loc[index_to_update, 'result_80'] = result_80

                    result_USD = df_open_positions.loc[index_to_update, 'result_30'] + \
                                 df_open_positions.loc[index_to_update, 'result_50'] + \
                                 df_open_positions.loc[index_to_update, 'result_80']

                    # Внести изменения в датафрейм открытых позиций
                    df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "sl",
                                                                 current_time, current_timestamp)

                    # Отправляем закрытую позицию в БД и удаляем из датафрейма
                    row_to_move = df_open_positions.loc[index_to_update].to_dict()
                    df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                    print(f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по стопу")
                    df_open_positions = df_open_positions.drop(index_to_update)

                    # Добавить спред в чёрный список
                    new_row_blacklist = {'spread': spread, 'time': row.open_time,
                                         'timestamp': time_functions.time_now()[1],
                                         'reason': 'sl'}
                    blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])
                    blacklist_history = pd.concat([blacklist_history, pd.DataFrame.from_records([new_row_blacklist])])

                elif spread_price >= df_open_positions.loc[index_to_update, 'tp_30'] or \
                        spread_price >= df_open_positions.loc[index_to_update, 'tp_50'] or \
                        spread_price >= df_open_positions.loc[index_to_update, 'tp_80']:

                    """Take Profit"""

                    if spread_price >= df_open_positions.loc[index_to_update, 'tp_80']:
                        tp_type = '80'
                        symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
                        symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])
                        if symbol_1_quantity is not None and symbol_2_quantity is not None and symbol_1_quantity != 0 and symbol_2_quantity != 0:
                            volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                                get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value, tp_type)

                            # Совершить сделку
                            secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                         rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                            df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'] -= symbol_2_quantity
                            df_open_positions.loc[index_to_update, f'curr_volume'] -= volume
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] -= symbol_2_quantity

                            # округлить текущее количество
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] = round_curr_quantity(symbol_1_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_1_curr_quantity'])
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] = round_curr_quantity(symbol_2_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_2_curr_quantity'])

                            result_percent = round((execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                            result_USD = df_open_positions.loc[index_to_update, 'result_usd'] + round((volume * (result_percent / 100)), 2)

                            result_80 = round((vol_80 * (result_percent / 100)), 2)
                            df_open_positions.loc[index_to_update, 'result_80'] = result_80

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "tp",
                                                                         current_time, current_timestamp)

                    if spread_price >= df_open_positions.loc[index_to_update, 'tp_50']:
                        tp_type = '50'
                        symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
                        symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])
                        if symbol_1_quantity is not None and symbol_2_quantity is not None and symbol_1_quantity != 0 and symbol_2_quantity != 0:
                            volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                                get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value, tp_type)

                            # Совершить сделку
                            secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                         rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                            df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'] -= symbol_2_quantity
                            df_open_positions.loc[index_to_update, f'curr_volume'] -= volume
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] -= symbol_2_quantity

                            # округлить текущее количество
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] = round_curr_quantity(symbol_1_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_1_curr_quantity'])
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] = round_curr_quantity(symbol_2_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_2_curr_quantity'])

                            result_percent = round((execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                            result_USD = df_open_positions.loc[index_to_update, 'result_usd'] + round((volume * (result_percent / 100)), 2)

                            result_50 = round((vol_50 * (result_percent / 100)), 2)
                            df_open_positions.loc[index_to_update, 'result_50'] = result_50

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "tp",
                                                                         current_time, current_timestamp)

                    if spread_price >= df_open_positions.loc[index_to_update, 'tp_30']:
                        tp_type = '30'
                        symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
                        symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])
                        if symbol_1_quantity is not None and symbol_2_quantity is not None and symbol_1_quantity != 0 and symbol_2_quantity != 0:
                            volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                                get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value, tp_type)

                            # Совершить сделку
                            secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                         rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                            df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'] -= symbol_2_quantity
                            df_open_positions.loc[index_to_update, f'curr_volume'] -= volume
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] -= symbol_2_quantity

                            # округлить текущее количество
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] = round_curr_quantity(symbol_1_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_1_curr_quantity'])
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] = round_curr_quantity(symbol_2_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_2_curr_quantity'])

                            result_percent = round((execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                            result_USD = df_open_positions.loc[index_to_update, 'result_usd'] + round((volume * (result_percent / 100)), 2)

                            result_30 = round((vol_30 * (result_percent / 100)), 2)
                            df_open_positions.loc[index_to_update, 'result_30'] = result_30

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "tp",
                                                                         current_time, current_timestamp)

                    if df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] == 0 and \
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] == 0:
                        # Отправляем закрытую позицию в БД и удаляем из датафрейма
                        row_to_move = df_open_positions.loc[index_to_update].to_dict()
                        df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                        print(f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по тейку")
                        df_open_positions = df_open_positions.drop(index_to_update)

            if direction_value == 'sell':

                current_result_percent = round(-(spread_price / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                df_open_positions.loc[index_to_update, 'curr_result_perc'] = current_result_percent
                current_result_usd = round(df_open_positions.loc[index_to_update, 'curr_volume'] * (current_result_percent / 100), 4)

                if result_30 is not None and not pd.isna(result_30):
                    current_result_usd += result_30
                if result_50 is not None and not pd.isna(result_50):
                    current_result_usd += result_50
                if result_80 is not None and not pd.isna(result_80):
                    current_result_usd += result_80

                df_open_positions.loc[index_to_update, 'curr_result_usd'] = round(current_result_usd, 4)

                # Обновить tp
                df_open_positions = change_tp(direction_value, df_open_positions, index_to_update, spread)

                if spread_price >= df_open_positions.loc[index_to_update, 'sl']:
                    """Stop Loss"""

                    volume, symbol_1_quantity, symbol_2_quantity, rounded_limit_price_1, \
                        rounded_limit_price_2, execute_spread_prise = secondary_functions.\
                        get_sl_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value)

                    # Совершить сделку
                    secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                 rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                    result_percent = round(-(execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)

                    if result_30 is None or pd.isna(result_30) or result_30 == 0:
                        result_30 = round((vol_30 * (result_percent / 100)), 2)
                        df_open_positions.loc[index_to_update, 'result_30'] = result_30

                    if result_50 is None or pd.isna(result_50) or result_50 == 0:
                        result_50 = round((vol_50 * (result_percent / 100)), 2)
                        df_open_positions.loc[index_to_update, 'result_50'] = result_50

                    if result_80 is None or pd.isna(result_80) or result_80 == 0:
                        result_80 = round((vol_80 * (result_percent / 100)), 2)
                        df_open_positions.loc[index_to_update, 'result_80'] = result_80

                    result_USD = df_open_positions.loc[index_to_update, 'result_30'] + \
                                 df_open_positions.loc[index_to_update, 'result_50'] + \
                                 df_open_positions.loc[index_to_update, 'result_80']

                    # Внести изменения в датафрейм открытых позиций
                    df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "sl",
                                                                 current_time, current_timestamp)

                    # Отправляем закрытую позицию в БД и удаляем из датафрейма
                    row_to_move = df_open_positions.loc[index_to_update].to_dict()
                    df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                    print(f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по стопу")
                    df_open_positions = df_open_positions.drop(index_to_update)

                    # Добавить спред в чёрный список
                    new_row_blacklist = {'spread': spread, 'time': row.open_time,
                                         'timestamp': time_functions.time_now()[1],
                                         'reason': 'sl'}
                    blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])
                    blacklist_history = pd.concat([blacklist_history, pd.DataFrame.from_records([new_row_blacklist])])

                elif spread_price <= df_open_positions.loc[index_to_update, 'tp_30'] or spread_price <= df_open_positions.loc[index_to_update, 'tp_50'] or \
                        spread_price <= df_open_positions.loc[index_to_update, 'tp_80']:

                    """Take Profit"""

                    if spread_price <= df_open_positions.loc[index_to_update, 'tp_80']:
                        tp_type = '80'
                        symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
                        symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])
                        if symbol_1_quantity is not None and symbol_2_quantity is not None and symbol_1_quantity != 0 and symbol_2_quantity != 0:
                            volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                                get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value, tp_type)

                            # Совершить сделку
                            secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                         rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                            df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'] -= symbol_2_quantity
                            df_open_positions.loc[index_to_update, f'curr_volume'] -= volume
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] -= symbol_2_quantity

                            # округлить текущее количество
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] = round_curr_quantity(symbol_1_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_1_curr_quantity'])
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] = round_curr_quantity(symbol_2_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_2_curr_quantity'])

                            result_percent = round(-(execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                            result_USD = df_open_positions.loc[index_to_update, 'result_usd'] + round((volume * (result_percent / 100)), 2)

                            result_80 = round((vol_80 * (result_percent / 100)), 2)
                            df_open_positions.loc[index_to_update, 'result_80'] = result_80

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "tp",
                                                                         current_time, current_timestamp)

                    if spread_price <= df_open_positions.loc[index_to_update, 'tp_50']:
                        tp_type = '50'
                        symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
                        symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])
                        if symbol_1_quantity is not None and symbol_2_quantity is not None and symbol_1_quantity != 0 and symbol_2_quantity != 0:
                            volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                                get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value, tp_type)

                            # Совершить сделку
                            secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                         rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                            df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'] -= symbol_2_quantity
                            df_open_positions.loc[index_to_update, f'curr_volume'] -= volume
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] -= symbol_2_quantity

                            # округлить текущее количество
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] = round_curr_quantity(symbol_1_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_1_curr_quantity'])
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] = round_curr_quantity(symbol_2_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_2_curr_quantity'])

                            result_percent = round(-(execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                            result_USD = df_open_positions.loc[index_to_update, 'result_usd'] + round((volume * (result_percent / 100)), 2)

                            result_50 = round((vol_50 * (result_percent / 100)), 2)
                            df_open_positions.loc[index_to_update, 'result_50'] = result_50

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "tp",
                                                                         current_time, current_timestamp)

                    if spread_price <= df_open_positions.loc[index_to_update, 'tp_30']:
                        tp_type = '30'
                        symbol_1_quantity = float(df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'])
                        symbol_2_quantity = float(df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'])
                        if symbol_1_quantity is not None and symbol_2_quantity is not None and symbol_1_quantity != 0 and symbol_2_quantity != 0:
                            volume, rounded_limit_price_1, rounded_limit_price_2, execute_spread_prise = secondary_functions. \
                                get_tp_params(df_open_positions, index_to_update, symbol_1, symbol_2, direction_value, tp_type)

                            # Совершить сделку
                            secondary_functions.do_trade(symbol_1, symbol_2, direction_value, rounded_limit_price_1,
                                                         rounded_limit_price_2, symbol_1_quantity, symbol_2_quantity)

                            df_open_positions.loc[index_to_update, f'symbol_1_{tp_type}_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_{tp_type}_quantity'] -= symbol_2_quantity
                            df_open_positions.loc[index_to_update, f'curr_volume'] -= volume
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] -= symbol_1_quantity
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] -= symbol_2_quantity

                            # округлить текущее количество
                            df_open_positions.loc[index_to_update, f'symbol_1_curr_quantity'] = round_curr_quantity(symbol_1_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_1_curr_quantity'])
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] = round_curr_quantity(symbol_2_quantity, df_open_positions.loc[
                                index_to_update, f'symbol_2_curr_quantity'])

                            result_percent = round(-(execute_spread_prise / df_open_positions.loc[index_to_update, 'open_prise'] - 1) * 100, 2)
                            result_USD = df_open_positions.loc[index_to_update, 'result_usd'] + round((volume * (result_percent / 100)), 2)

                            result_30 = round((vol_30 * (result_percent / 100)), 2)
                            df_open_positions.loc[index_to_update, 'result_30'] = result_30

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions = change_df_open_positions(df_open_positions, result_percent, result_USD, "tp",
                                                                         current_time, current_timestamp)

                    if df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] == 0 and \
                            df_open_positions.loc[index_to_update, f'symbol_2_curr_quantity'] == 0:
                        # Отправляем закрытую позицию в БД и удаляем из датафрейма
                        row_to_move = df_open_positions.loc[index_to_update].to_dict()
                        df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                        print(f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по тейку")
                        df_open_positions = df_open_positions.drop(index_to_update)

        df_open_positions = df_open_positions.reset_index(drop=True)
        df_closed_positions = df_closed_positions.reset_index(drop=True)

        blacklist = blacklist.reset_index(drop=True)
        database.add_table_to_db(blacklist, 'blacklist', 'replace')
        database.add_table_to_db(blacklist_history, 'blacklist_history', 'append')

    return df_open_positions, df_closed_positions