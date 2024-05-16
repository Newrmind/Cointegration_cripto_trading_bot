import pandas as pd
from typing import Literal
import sqlalchemy as sa
import params
import time_functions

class Database:
    def __init__(self, db_info: dict = params.postgres_connection_info):
        self.db_info = db_info
        self.engine = sa.create_engine(
            f"postgresql://{self.db_info['user']}:{self.db_info['password']}@{self.db_info['host']}:"
            f"{self.db_info['port']}/{self.db_info['dbname']}"
        )

    # Функция получения таблицы из БД
    def get_table_from_db(self, querry: str):
        connection = self.engine.connect()
        if connection:
            try:
                df = pd.read_sql(querry, connection)
                return df
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                connection.close()
        return None

    # Функция записи таблицы в БД
    def add_table_to_db(self, df: pd.DataFrame, table_name: str, if_exists: Literal['append', 'replace']) -> None:
        connection = self.engine.connect()
        if connection:
            try:
                df.to_sql(name=table_name, con=connection, if_exists=if_exists, index=False)
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                connection.close()

    def change_table(self, query: str) -> None:
        connection = self.engine.raw_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                connection.close()

    def update_futures_metadata(self, df: pd.DataFrame, action: str) -> None:
        raw_connection = self.engine.raw_connection()
        connection = self.engine.connect()
        if raw_connection:
            try:
                cursor = raw_connection.cursor()
                if action == 'append':
                    df.to_sql(name='futures_metadata', con=connection, if_exists='append', index=False)
                elif action == 'delete':
                    for index, row in df.iterrows():
                        symbol = row['symbol']
                        query = "DELETE FROM futures_metadata WHERE symbol = %s"
                        cursor.execute(query, (symbol,))
                    raw_connection.commit()
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                raw_connection.close()

    # Получение таблицы фьючерсов с метаданными Binance
    def get_futures_metadata(self) -> pd.DataFrame:
        querry = "SELECT * FROM futures_metadata ORDER BY symbol ASC"
        return self.get_table_from_db(querry)

    def update_futures_last_klines(self, intervals: list) -> None:

        get_futures_last_klines = pd.DataFrame()

        with self.engine.connect() as connection:
            for interval in intervals:
                print(f'Запрос времени первой и последней свечи для тф {interval}')
                query = f"SELECT symbol, MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp " \
                        f"FROM futures_klines_{interval} GROUP BY symbol"

                klilines_minmax = pd.read_sql_query(query, connection)
                klilines_minmax['timeframe'] = f'{interval}'
                get_futures_last_klines = pd.concat([get_futures_last_klines, klilines_minmax], ignore_index=True)

            get_futures_last_klines['min_time'] = pd.to_datetime(get_futures_last_klines['min_timestamp'], unit='ms')
            get_futures_last_klines['max_time'] = pd.to_datetime(get_futures_last_klines['max_timestamp'], unit='ms')

            get_futures_last_klines.to_sql(name='klines_minmax', con=connection, if_exists='replace', index=False)
            connection.commit()

    def delete_old_klines(self, timeframe) -> None:
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            timestamp_now = time_functions.time_now()[1]
            half_year_ago_timestamp = timestamp_now - 15552000000
            query = f"DELETE FROM futures_klines_{timeframe} WHERE timestamp < {half_year_ago_timestamp}"
            cursor.execute(query)
            connection.commit()
        finally:
            connection.close()

    def delete_matching_pairs(self):
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            query = '''DELETE FROM correlation_15m
                        WHERE pair IN (SELECT spread FROM blacklist);'''
            cursor.execute(query)
            connection.commit()
        finally:
            connection.close()

    def split_spread_column(self):
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.execute('''ALTER TABLE result_coint_rating
                            ADD COLUMN symbol_1 TEXT;''')
            cursor.execute('''ALTER TABLE result_coint_rating
                            ADD COLUMN symbol_2 TEXT;''')
            cursor.execute('''UPDATE result_coint_rating
                            SET symbol_1 = SUBSTR(pair, 1, POSITION('/' IN pair) - 1),
                            symbol_2 = SUBSTR(pair, POSITION('/' IN pair) + 1);''')

            connection.commit()
            query = "SELECT * FROM result_coint_rating"
            return self.get_table_from_db(query)

        finally:
            connection.close()

    # Создание триггера каскадного удаления
    def cascade_del(self) -> None:
        connection = self.engine.connect()
        if connection:
            try:
                connection = self.engine.connect()
                transaction = connection.begin()

                query = '''
                    CREATE OR REPLACE FUNCTION delete_ticker()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        DELETE FROM futures_klines_5m WHERE Symbol = OLD.symbol;
                        DELETE FROM futures_klines_15m WHERE Symbol = OLD.symbol;
                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql;
    
                    CREATE TRIGGER delete_ticker
                    AFTER DELETE ON futures_metadata
                    FOR EACH ROW
                    EXECUTE FUNCTION delete_ticker();
                '''

                connection.execute(sa.text(query))  # Используем sa.text для передачи многострочного SQL-запроса
                print("[INFO] Триггер каскадного удаления создан.")

                transaction.commit()
            except Exception as e:
                if 'connection' in locals() and connection is not None:
                    connection.close()
                raise e
            finally:
                if 'connection' in locals() and connection is not None:
                    connection.close()

if __name__ == '__main__':
    db = Database()
    # db.update_futures_last_klines(['15m'])
    # db.split_spread_column()
    # db.delete_old_klines('15m')
