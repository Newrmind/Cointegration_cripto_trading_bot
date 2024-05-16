import pandas as pd
import numpy as np
import time
from statsmodels.tsa.stattools import coint
import params
import time_functions
from Database.postgres_sql import Database


class DataAnalysis:
    def __init__(self, db_to_load: Database = Database(), db_to_save: Database = Database(),
                 amount_klines: dict = params.amount_klines):
        """
        :param db_to_load: экземпляр класса базы данных, из которого загружаются данные для анализа
        :param db_to_save: экземпляр класса базы данных, в который сохраняются результаты анализа
        :param time_window_concat: словарь, содержащий start & end time для выборки данных

        Model name:     "Citadel"
        Version:        2.2
        Last_update:    11.09.2023
        """
        self.db_to_load = db_to_load
        self.db_to_save = db_to_save
        self.time_window_concat = time_functions.get_start_time_for_concat(amount_klines)

    def concat(self, window_concat: dict = None) -> None:
        '''
        Функция конкатенации для последующего анализа данных.
        '''

        function_start_time = time.time()

        # Соединяемся с базой данных, получаем список тикеров
        symbols = self.db_to_load.get_futures_metadata()
        symbols = symbols['symbol'].tolist()

        time_window_concat = self.time_window_concat
        if window_concat is not None:
            time_window_concat = window_concat

        for timeframe in params.intervals:
            start_time = time_window_concat[f'start_time_{timeframe}']
            end_time = time_window_concat[f'end_time_{timeframe}']
            print(f'Конкатенация для таймфрейма {timeframe}.')
            print(f'Период: {time_functions.convert_timestamp(start_time)} UTC - {time_functions.convert_timestamp(end_time)} UTC\n')

            # Формируем строку с частями "MAX(CASE ...)" для каждого символа
            symbol_columns = ', '.join(
                [f'''MAX(CASE WHEN symbol = '{symbol}' THEN close END) AS "{symbol}"''' for symbol in symbols])

            # Формируем строку с условиями для фильтрации данных
            conditions = f"""symbol IN ({", ".join([f"'{symbol}'" for symbol in symbols])}) AND (timestamp > {start_time}) 
                            AND (timestamp < {end_time})"""

            # Формируем основную часть запроса
            query = f"SELECT time, timestamp, {symbol_columns} FROM futures_klines_{timeframe} WHERE {conditions} GROUP BY time, timestamp"

            # Выполняем запрос и читаем результат в DataFrame
            result_df = self.db_to_load.get_table_from_db(query)

            # проверяем последнюю строку на наличие пустых ячеек
            if not result_df.empty:
                last_row_has_nan = result_df.iloc[-1].isna().any()
                # Удаляем последнюю строку при наличии пустых ячеек
                if last_row_has_nan:
                    result_df = result_df.drop(result_df.index[-1])
            elif result_df.empty:
                print('Внимание! Получен пустой датафрейм!')

            # Проверяем наличие пустых ячеек в каждой колонке
            columns_with_empty_cells = result_df.columns[result_df.isnull().any()].tolist()
            # Удаляем колонки, в которых есть пустые ячейки
            df_cleaned = result_df.drop(columns=columns_with_empty_cells)

            # Далее преобразуем все колонки, кроме колонок времени, в числовые значения
            first_two_columns = df_cleaned.iloc[:, :2].copy()
            numeric_columns = df_cleaned.iloc[:, 2:].apply(pd.to_numeric, errors='coerce')
            # Объединяем первые два столбца с преобразованными числовыми столбцами
            df_cleaned = pd.concat([first_two_columns, numeric_columns], axis=1)

            # Записываем результат в БД
            self.db_to_save.add_table_to_db(df=df_cleaned, table_name=f'concat_futures_{timeframe}', if_exists='replace')

            if df_cleaned.empty:
                print('Внимание! Получен пустой датафрейм!')

        function_end_time = time.time()
        execution_time = function_end_time - function_start_time
        print(f"\nФункция выполнялась {execution_time:.3f} секунд")
        print('Конкатенация завершена. Данные записаны в базу данных!')

    def correlation(self, analyze_window: dict = None) -> None:
        """
        Функция производит корреляционный анализ.

        :param analyze_window: Словарь, содержащий start_time и end_time для каждого тф (необязательный).
        """
        time_window_for_analyze = self.time_window_concat
        if analyze_window is not None:
            time_window_for_analyze = analyze_window

        timeframe = '15m'

        print(f'\nКорреляционный анализ для таймрейма {timeframe}')

        # Запрашиваем из БД таблицу для анализа
        if time_window_for_analyze is not None:
            start_time = time_window_for_analyze[f'start_time_{timeframe}']
            end_time = time_window_for_analyze[f'end_time_{timeframe}']

            print(f'Период анализа: {time_functions.convert_timestamp(start_time)} UTC - {time_functions.convert_timestamp(end_time)} UTC\n')
            querry = f'SELECT * FROM concat_futures_15m ' \
                     f'WHERE Timestamp >= {start_time} AND Timestamp <= {end_time}'
        else:
            querry = f'SELECT * FROM concat_futures_{timeframe}'
        df = self.db_to_save.get_table_from_db(querry=querry)

        # Удаляем несколько столбцов
        columns_to_drop = ['time', 'timestamp']
        df_to_corr = df.drop(columns_to_drop, axis=1)
        # Преобразуем данные в числовые
        df_to_corr = df_to_corr.apply(pd.to_numeric, errors='coerce')

        # Строим корреляционную матрицу
        correlation_matrix = df_to_corr.corr()

        # Удаляем дублирующиеся данные
        correlation_matrix = correlation_matrix.where(np.triu(np.ones(correlation_matrix.shape), k=1).astype(bool))

        # округляем значения до 3 цифр после запятой
        correlation_matrix = correlation_matrix.round(3)

        # Преобразуем корреляционную матрицу в одномерный Series
        corr_series = correlation_matrix.stack()

        # Фильтруем значения корреляции больше 0.8
        high_corr_values = corr_series[(abs(corr_series) >= 0.8) & (corr_series != 1)]

        # Преобразуем индекс Series в нужный формат
        high_corr_values.index = high_corr_values.index.map(lambda x: f"{x[0]}/{x[1]}")

        # Создаем новый DataFrame для вывода результатов
        high_corr_values = pd.DataFrame({'pair': high_corr_values.index, 'corr': high_corr_values.values})
        high_corr_values = high_corr_values.round(3)

        # Записываем результат в БД
        self.db_to_save.add_table_to_db(df=high_corr_values, table_name=f'correlation_{timeframe}', if_exists='replace')


    def cointegration_test(self, analyze_window: dict = None) -> None:
        """
        Функция анализа временных рядов с целью поиска коинтегрированных активов.
        """

        function_start_time = time.time()

        df_corr = self.db_to_save.get_table_from_db(querry='SELECT * '
                                                           'FROM correlation_15m '
                                                           'ORDER BY corr DESC '
                                                           'LIMIT 500;')

        if df_corr.empty:
            print("Датафрейм correlation_15m пустой. Функция завершается.")
            return

        # Получаем список пар активов, скоррелированных 15 мин. тф
        symbols_pairs = df_corr['pair'].tolist()
        # Разбиваем пары на отдельные значения
        symbols_list = [item for pair in symbols_pairs for item in pair.split('/')]
        # Оставляем только уникальные значения
        unique_symbols_list = list(set(symbols_list))
        # Преобразование списка в строку для SQL запроса
        columns_str = ', '.join(f'"{col}"' for col in unique_symbols_list)

        time_window_for_analyze = self.time_window_concat
        if analyze_window is not None:
            time_window_for_analyze = analyze_window

        for tf in ['15m']:
            # Получаем таблицу с конкатенированными данными
            print(f"Коинтеграционный анализ для тф {tf}")
            if time_window_for_analyze is not None:
                start_time = time_window_for_analyze[f'start_time_{tf}']
                end_time = time_window_for_analyze[f'end_time_{tf}']

                querry_concat = f"""
                        SELECT {columns_str} 
                        FROM concat_futures_{tf} 
                        WHERE 
                            timestamp >= {start_time} AND timestamp <= {end_time};
                         """
                print(f'Период анализа: {time_functions.convert_timestamp(start_time)} UTC - {time_functions.convert_timestamp(end_time)} UTC')
            else:
                querry_concat = f'SELECT {columns_str} FROM concat_futures_{tf}'
            df = self.db_to_save.get_table_from_db(querry=querry_concat)

            symbols_pairs_len = len(symbols_pairs)
            print(f'Количество пар для коинтеграционного анализа: {symbols_pairs_len}')

            # Список для сбора показателей теста на коинтеграцию
            coint_scores = []

            count = 1

            for pair_symbols in symbols_pairs:

                if count % 10 == 0:
                    print(f'Анализ пары №{count} из {symbols_pairs_len} tf-{tf} ---- {pair_symbols}')

                count += 1
                pair_symbols_list = pair_symbols.split('/')

                symbol_series_1 = df[pair_symbols_list[0]]
                symbol_series_2 = df[pair_symbols_list[1]]

                # Выполняем тест на коинтеграцию
                coint_result = coint(symbol_series_1, symbol_series_2)

                # Извлекаем показатели из результата теста
                t_statistic = coint_result[0]
                p_value = coint_result[1]
                critical_values = coint_result[2]

                """ 
                Проводим сравнение t_statistic с критическими значениями. Если t-статистика превышает критические 
                значения, то вы можно отвергнуть нулевую гипотезу о отсутствии коинтеграции. Это означает, что существует
                статистически значимая коинтеграция между временными рядами. 
                """
                comparison_t_val_and_crit = True
                for value in critical_values:
                    if t_statistic > value:
                        comparison_t_val_and_crit = False
                        break

                # Добавляем данные в список
                coint_scores.append({'pair': pair_symbols, 't_statistic': t_statistic, 'p_value': p_value,
                                   'critical_value_1%': critical_values[0],
                                   'critical_value_5%': critical_values[1],
                                   'critical_value_10%': critical_values[2],
                                   'differences_t_val': comparison_t_val_and_crit})

            # Создаем датафрейм из списка данных
            adf_scores_df = pd.DataFrame(coint_scores)

            # Сохраняем данные в БД
            self.db_to_save.add_table_to_db(adf_scores_df, f'coint_scores_{tf}', 'replace')

        # Формируем результирующую таблицу со всеми показателями
        querry_result = '''
                    SELECT
                        correlation_15m.pair,
                        correlation_15m.corr,
                        
                        coint_scores_15m.t_statistic AS t_statistic_15m,
                        coint_scores_15m.p_value AS p_value_15m,
                        coint_scores_15m.differences_t_val AS differences_t_val_15m
                    FROM
                        correlation_15m
                    INNER JOIN
                        coint_scores_15m ON correlation_15m.pair = coint_scores_15m.pair
                    '''
        df_result = self.db_to_save.get_table_from_db(querry=querry_result)
        # Добавляем колону итоговой оценки всех показателей
        df_result['final_score'] = 0

        def coint_result_analysis(df: pd.DataFrame) -> pd.DataFrame:
            """
            Оценивает показатели ADF теста и составляет общий рейтинг.

            :param df: датафрейм для анализа
            :return: pd.DataFrame
            """

            def compare_with_mapping(value, thresholds, values_to_assign):
                """
                Сравнивает значение с порогами из первого списка и
                возвращает соответствующее значение из второго списка.

                :param value: значение для сравнения
                :param thresholds: список порогов
                :param values_to_assign: список соответствующих значений
                :return: соответствующее значение или None
                """
                for i, threshold in enumerate(thresholds[::-1]):
                    if value >= threshold:
                        return values_to_assign[len(thresholds) - i - 1]
                return None

            def compare_adf_results(df: pd.DataFrame, column_names: list, thresholds: list, values_to_assign: list):
                for col_name in column_names:
                    comparison_results = []
                    for value in df[col_name]:
                        abs_value = abs(value)  # берём число по модулю
                        comparison_results.append(compare_with_mapping(abs_value, thresholds, values_to_assign))

                    df['final_score'] += comparison_results

                return df

            # Оцениваем показатели корреляции
            columns_corr = ['corr']
            thresholds_corr = [-1, 0.8, 0.85, 0.9, 0.95]  # Значения, с которыми сравниваются показатели корреляции
            values_to_assign_corr = [-4, -2, -1, 0, 2]  # Баллы, соответствующие значениям
            df = compare_adf_results(df, columns_corr, thresholds_corr, values_to_assign_corr)

            # Оцениваем итоги сравнения t-статистики с критическими значениями
            columns_adf = ['differences_t_val_15m']
            thresholds_adf = [0, 1]  # Значения, с которыми сравниваются показатели
            values_to_assign_adf = [-6, 6]  # Баллы, соответствующие значениям
            df = compare_adf_results(df, columns_adf, thresholds_adf, values_to_assign_adf)

            # Оцениваем показатель p_value
            columns_p_value = ['p_value_15m']
            thresholds_p_value = [0, 0.0001,  0.001, 0.01, 0.05, 0.1, 0.15]  # Значения, с которыми сравниваются показатели
            values_to_assign_p_value = [10, 8, 6, 4, 2, 0, -2]  # Баллы, соответствующие значениям
            df = compare_adf_results(df, columns_p_value, thresholds_p_value, values_to_assign_p_value)

            # Оцениваем показатель t-статистики
            columns_t_stat = ['t_statistic_15m']
            thresholds_t_stat = [0, 1, 2, 3, 3.8, 4, 4.2, 4.4, 4.6, 4.8, 5, 5.2]  # Значения, с которыми сравниваются показатели
            values_to_assign_t_stat = [-8, -6, -4, 0, 1, 2, 3, 4, 5, 6, 7, 8]  # Баллы, соответствующие значениям
            df = compare_adf_results(df, columns_t_stat, thresholds_t_stat, values_to_assign_t_stat)

            # Переставим колонку final_score в начало датафрейма
            temp_column = df['final_score']
            df = df.drop(columns=['final_score'])
            df.insert(1, 'final_score', temp_column)

            # Сортируем датафрейм по колонке final_score
            sorted_df = df.sort_values(by='final_score', ascending=False)

            return sorted_df

        # Вызываем функцию, оценивающую полученный результат и составляющую рейтинг
        final_df = coint_result_analysis(df_result)
        self.db_to_save.add_table_to_db(df=final_df, table_name='result_coint_rating', if_exists='replace')

        # Разбить колонку спреда на тикеры
        final_df_to_del = self.db_to_save.split_spread_column()

        def create_count_dict(open_positions_df):
            """Создаёт словарь, в котором записано кол-во, повторений тикера в таблице открытых позиций"""
            symbols_pairs = open_positions_df['spread'].tolist()
            symbols_list = [item for pair in symbols_pairs for item in pair.split('/')]

            count_dict = {}
            for symbol in symbols_list:
                if symbol in count_dict:
                    count_dict[symbol] += 1
                else:
                    count_dict[symbol] = 1
            return count_dict

        open_positions_df = self.db_to_save.get_table_from_db("SELECT spread FROM open_positions")
        count_dict = create_count_dict(open_positions_df)

        # Удалить спреды с тикерами, повторяющимися более 2 раз
        def del_duplicates(result_stat, count_dict, column):

            new_df = pd.DataFrame()
            for index, row in result_stat.iterrows():

                row_to_move = row.to_dict()
                item = row_to_move[column]

                if item in count_dict:
                    count_dict[item] += 1
                    if count_dict[item] <= 2:
                        new_df = pd.concat([new_df, pd.DataFrame.from_records([row_to_move])])

                else:
                    new_df = pd.concat([new_df, pd.DataFrame.from_records([row_to_move])])
                    count_dict[item] = 1

            new_df.reset_index(drop=True, inplace=True)
            return new_df

        clear_result_stat = del_duplicates(final_df_to_del, count_dict, 'symbol_1')
        clear_result_stat = del_duplicates(clear_result_stat, count_dict, 'symbol_2')

        self.db_to_save.add_table_to_db(df=clear_result_stat, table_name='result_coint_rating', if_exists='replace')

        # записать в БД время последнего анализа
        df_requests_time = self.db_to_save.get_table_from_db('SELECT * FROM requests_time')
        time_now, timestamp_now = time_functions.time_now()
        index_to_update = df_requests_time[df_requests_time['request'] == 'last_coint_analysis'].index[0]
        new_values = {'request': 'last_coint_analysis', 'time_utc': time_now, 'timestamp_utc_milisec': timestamp_now}
        df_requests_time.loc[index_to_update] = new_values
        self.db_to_save.add_table_to_db(df_requests_time, table_name='requests_time', if_exists='replace')

        function_end_time = time.time()
        execution_time = function_end_time - function_start_time
        print(f"\nФункция выполнялась {execution_time:.3f} секунд\n")


if __name__ == '__main__':
    analyze = DataAnalysis()
    # analyze.concat()
    analyze.correlation()
    analyze.cointegration_test()


