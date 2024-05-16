import sqlite3
import pandas as pd
import params


def copy_positions_tables():
    # Подключение к первой базе данных
    source_conn = sqlite3.connect(params.main_db_path)

    # Подключение ко второй базе данных
    destination_conn = sqlite3.connect(params.copy_result_db)

    # Список таблиц, которые вы хотите скопировать
    tables_to_copy = ['open_positions', 'closed_positions', 'blacklist', 'result_coint_rating', 'requests_time']

    for table_name in tables_to_copy:
        # Чтение данных из исходной таблицы в DataFrame
        df = pd.read_sql(f"SELECT * FROM {table_name}", source_conn)

        # Запись данных DataFrame в целевую таблицу
        df.to_sql(table_name, destination_conn, if_exists='replace', index=False)

    # Закрытие подключений
    source_conn.close()
    destination_conn.close()

