from Database.postgres_sql import Database
import pandas as pd
from Data_analysis.data_analysis import DataAnalysis
import params
import time_functions
from time import sleep
from Trading import check_positions, create_trade


class Trading:
    def __init__(self, amount_klines: dict = params.amount_klines,
                 trading_params: dict = params.trading_params,
                 trading_allowed: bool = params.trading_allowed):

        self.main_database = Database()
        self.time_window_concat = time_functions.get_start_time_for_concat(amount_klines)
        self.pairs_list = None
        # создать экземпляр класса DataAnalysis
        self.data_analysis = DataAnalysis()
        self.trading_params = trading_params
        self.blacklist_time = params.blacklist_time
        self.trading_allowed = trading_allowed

    def data_preprocessing(self):
        self.data_analysis.concat(self.time_window_concat)

        # записать в БД время последнего вызова функции
        df_requests_time = self.main_database.get_table_from_db('SELECT * FROM requests_time')
        time_now, timestamp_now = time_functions.time_now()
        index_to_update = df_requests_time[df_requests_time['request'] == 'data_preprocessing'].index[0]
        new_values = {'request': 'data_preprocessing', 'time_utc': time_now, 'timestamp_utc_milisec': timestamp_now}
        df_requests_time.loc[index_to_update] = new_values
        self.main_database.add_table_to_db(df_requests_time, table_name='requests_time', if_exists='replace')

    def data_analyze(self) -> None:
        """
        Функция отбирает коинтегрированные активы для заданного промежутка времени.
        """

        tf_5m_start = time_functions.convert_timestamp(self.time_window_concat["start_time_5m"])
        tf_5m_end = time_functions.convert_timestamp(self.time_window_concat["end_time_5m"])
        tf_15m_start = time_functions.convert_timestamp(self.time_window_concat["start_time_15m"])
        tf_15m_end = time_functions.convert_timestamp(self.time_window_concat["end_time_15m"])

        print(f'''\nТекущее временное окно для анализа: 
                    tf_5m_start: {tf_5m_start} tf_5m_end: {tf_5m_end} , 
                    tf_15m_start: {tf_15m_start} tf_15m_end: {tf_15m_end} \n''')
        sleep(1)

        # Отбор активов для торговли
        self.data_analysis.correlation()

        # Удалить из выборки спреды в чёрном списке
        self.main_database.delete_matching_pairs()
        self.data_analysis.cointegration_test()

    def get_pairs_list(self):
        # Получить 100 наиболее коинтегрированных активов
        query = f"""    SELECT pair
                        FROM result_coint_rating
                        WHERE final_score >= 10
                        ORDER BY final_score DESC
                        LIMIT 100; """

        query_open_pos = """SELECT spread FROM open_positions"""
        query_hand_selected_pairs = """SELECT pair FROM hand_selected_spreads"""

        pairs_list = self.main_database.get_table_from_db(query)['pair'].tolist()
        pairs_list_open = (self.main_database.get_table_from_db(query_open_pos)['spread'].tolist())
        hand_selected_pairs_list = (self.main_database.get_table_from_db(query_hand_selected_pairs)['pair'].tolist())

        for pair in pairs_list_open:
            if pair not in pairs_list:
                pairs_list.append(pair)

        for pair in hand_selected_pairs_list:
            if pair not in pairs_list:
                pairs_list.append(pair)

        pairs_list = list(set(pairs_list))
        self.pairs_list = pairs_list

    def bollinger_bands_preprocessing(self):
        """Строит полосы Боллинджера. """
        self.get_pairs_list()
        pairs_list = self.pairs_list

        # Получить список уникальных символов
        pairs_list_uniq = [item for pair in pairs_list for item in pair.split('/')]
        symbols_list_uniq = list(set(pairs_list_uniq))

        # Преобразование списка в строку для SQL запроса
        columns_str = ', '.join(f'"{col}"' for col in symbols_list_uniq)

        df_instrument_for_spread = self.main_database.get_table_from_db(f"""SELECT time, timestamp, {columns_str} 
                                       FROM concat_futures_15m""")

        # Удаление кавычек из имен столбцов
        df_instrument_for_spread.rename(columns=lambda x: x.strip('"'), inplace=True)

        """Построить спред путём деления одного актива на другой"""
        spreads_df = pd.DataFrame()
        spreads_df['time'] = df_instrument_for_spread['time']
        spreads_df['timestamp'] = df_instrument_for_spread['timestamp']

        for pair in pairs_list:
            column1 = pair.split('/')[0]
            column2 = pair.split('/')[1]
            spreads_df[pair] = df_instrument_for_spread[column1] / df_instrument_for_spread[column2]

        # сохранить датафрейм спредов в базу данных
        self.main_database.add_table_to_db(spreads_df, 'spreads', 'replace')

    def bollinger_bands_calcualate(self):
        """Построить полосы Боллинджера"""
        self.get_pairs_list()
        pairs_list = self.pairs_list
        spreads_df = self.main_database.get_table_from_db('SELECT * FROM spreads')

        bollinger_bands_concat = pd.DataFrame()

        for pair in pairs_list:
            bollinger_bands = pd.DataFrame()
            # Расчёт SMA
            bollinger_bands['time'] = spreads_df['time']
            bollinger_bands['timestamp'] = spreads_df['timestamp']
            bollinger_bands['pair'] = pair
            bollinger_bands['close'] = spreads_df[pair]
            bollinger_bands['sma200'] = spreads_df[pair].rolling(200).mean()

            # Расчёт стандартного отклонения (standard deviation)
            bollinger_bands['sd'] = spreads_df[pair].rolling(200).std()

            # Расчёт нижнего BB
            bollinger_bands['lb'] = bollinger_bands['sma200'] - 2.9 * bollinger_bands['sd']

            # Расчёт верхнего BB
            bollinger_bands['ub'] = bollinger_bands['sma200'] + 2.9 * bollinger_bands['sd']

            bollinger_bands.dropna(subset=['sma200', 'lb', 'ub'], inplace=True)  # Удалить строки с NaN в указанных столбцах
            bollinger_bands.dropna(inplace=True)

            # Расчёт среднего значения стандартного отклонения и отклонения от среднего std
            bollinger_bands['sd_mean'] = bollinger_bands['sd'].mean()
            bollinger_bands['dif_percentage'] = (bollinger_bands['sd'] / bollinger_bands['sd_mean'] * 100) - 100

            bollinger_bands_concat = pd.concat([bollinger_bands_concat, bollinger_bands], ignore_index=True)

        # сохранить датафрейм полосы Боллинджера в базу данных
        self.main_database.add_table_to_db(df=bollinger_bands_concat, table_name='bollinger_bands', if_exists='replace')

        # записать в БД время последнего вызова функции
        df_requests_time = self.main_database.get_table_from_db('SELECT * FROM requests_time')
        time_now, timestamp_now = time_functions.time_now()
        index_to_update = df_requests_time[df_requests_time['request'] == 'bollinger_bands_calculate'].index[0]
        new_values = {'request': 'bollinger_bands_calculate', 'time_utc': time_now, 'timestamp_utc_milisec': timestamp_now}
        df_requests_time.loc[index_to_update] = new_values
        self.main_database.add_table_to_db(df_requests_time, table_name='requests_time', if_exists='replace')

        print('[INFO] Расчёт bollinger_bands завершён.')

    def trading(self):

        blacklist = self.main_database.get_table_from_db('SELECT * from blacklist')
        if not blacklist.empty:
            for index, row in blacklist.iterrows():
                blacklist_row_timestamp = row.timestamp
                del_from_bl_sl, del_from_bl_dif_perc, del_from_bl_liquidity_err = False, False, False
                if row.reason == 'sl':
                    del_from_bl_sl = time_functions.has_passed_any_hours(blacklist_row_timestamp, self.blacklist_time)
                elif row.reason == 'dif_perc':
                    del_from_bl_dif_perc = time_functions.has_passed_any_hours(blacklist_row_timestamp, 1)
                elif row.reason == 'evaluate_liquidity_trade_error':
                    del_from_bl_dif_perc = time_functions.has_passed_any_hours(blacklist_row_timestamp, 6)

                if del_from_bl_sl or del_from_bl_dif_perc or del_from_bl_dif_perc:
                    blacklist = blacklist.drop(index)
                    blacklist.reset_index(drop=True)
                    self.main_database.add_table_to_db(blacklist, 'blacklist', 'replace')

        df_open_positions = self.main_database.get_table_from_db('SELECT * FROM open_positions')
        df_open_positions = df_open_positions.apply(pd.to_numeric, errors='ignore')
        df_closed_positions = pd.DataFrame()
        bollinger_bands = self.main_database.get_table_from_db('SELECT * FROM bollinger_bands')

        last_bollinger_bands = bollinger_bands.loc[bollinger_bands.groupby('pair')['timestamp'].idxmax()]

        if not df_open_positions.empty:
            df_open_positions, df_closed_positions = check_positions.check_positions(self.main_database, df_open_positions, df_closed_positions, bollinger_bands)
            self.main_database.add_table_to_db(df_open_positions, 'open_positions', 'replace')

            if not df_closed_positions.empty:
                if 'curr_result_perc' in df_closed_positions.columns:
                    df_closed_positions = df_closed_positions.drop(
                        columns=['curr_result_perc', 'curr_result_usd', 'curr_volume', 'symbol_1_curr_quantity', 'symbol_2_curr_quantity', ], axis=1)
                self.main_database.add_table_to_db(df_closed_positions, 'closed_positions', 'append')

        def check_last_analyze():
            """Проверяет, когда был последний анализ."""
            last_time_analyze = int(self.main_database.get_table_from_db("SELECT timestamp_utc_milisec FROM requests_time \
                                             WHERE request = 'bollinger_bands_calculate'").loc[0, 'timestamp_utc_milisec'])
            need_analyze = time_functions.has_passed_any_hours(value=last_time_analyze, hours=params.check_analyze_period)
            if need_analyze:
                print(f'\n[INFO] Требуется анализ. Открытие новых позиций заблокировано.\n')
            return need_analyze

        # Условия, разрешающие торговлю
        is_analyze = not check_last_analyze()
        trading_allowed_df = self.main_database.get_table_from_db("SELECT * FROM account_info WHERE indicator_name = 'trading_allowed'")
        trading_allowed_value_db = trading_allowed_df.loc[trading_allowed_df['indicator_name'] == 'trading_allowed', 'value'].iloc[0]


        # найти сделки
        if self.trading_allowed and trading_allowed_value_db and is_analyze:
            create_trade.create_trade(self.main_database, last_bollinger_bands, df_open_positions)
        elif not self.trading_allowed:
            print(f'\n[INFO] Параметр trading_allowed = {self.trading_allowed}. Открытие новых позиций заблокировано.\n')
        elif not trading_allowed_value_db:
            print(f'\n[INFO] Параметр trading_allowed_value_db = {trading_allowed_value_db}. Открытие новых позиций заблокировано.\n')



if __name__ == '__main__':
    t = Trading()
    # t.data_preprocessing()
    # t.data_analyze()
    t.bollinger_bands_preprocessing()
    t.bollinger_bands_calcualate()