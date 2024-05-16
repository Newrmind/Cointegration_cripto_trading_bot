from connection import client
import time_functions
from logger import log_function_info, error_inf
from Database import postgres_sql
import time

def get_future_account_balance():

    log_function_info("Старт функции get_future_account_balance()")

    db = postgres_sql.Database()
    retries = 10  # Максимальное количество попыток
    while retries > 0:
        try:
            # Получите информацию о счете фьючерсов
            futures_account_info = client.futures_account(recvWindow=15000)
            del futures_account_info['assets'], futures_account_info['positions']

            # Извлеките балансы и маржи из ответа
            totalWalletBalance = float(futures_account_info['totalWalletBalance'])  # Всего средств
            availableBalance = float(futures_account_info['availableBalance'])  # Свободные средства
            totalOpenOrderInitialMargin = float(futures_account_info['totalOpenOrderInitialMargin'])  # Занятые средства (маржа)
            totalUnrealizedProfit = float(futures_account_info['totalUnrealizedProfit'])  # Нереализованный PnL
            totalPositionInitialMargin = float(futures_account_info['totalPositionInitialMargin'])  # Маржа по открытым позициям
            totalMarginBalance = float(futures_account_info['totalMarginBalance'])  # Маржа учётом PnL

            df_account_info = db.get_table_from_db("SELECT * FROM account_info")
            time_now = time_functions.time_now()
            indicator_names = {'total_wallet_balance': totalWalletBalance, 'available_balance': availableBalance,
                               'initial_margin': totalOpenOrderInitialMargin, 'unrealized_profit': totalUnrealizedProfit,
                               'position_initial_margin': totalPositionInitialMargin, 'total_margin_balance': totalMarginBalance}
            for key, value in indicator_names.items():
                df_account_info.loc[df_account_info['indicator_name'] == f'{key}', 'time'] = time_now[0]
                df_account_info.loc[df_account_info['indicator_name'] == f'{key}', 'timestamp'] = time_now[1]
                df_account_info.loc[df_account_info['indicator_name'] == f'{key}', 'value'] = value

            db.add_table_to_db(df_account_info, 'account_info', 'replace')

            return totalWalletBalance, availableBalance, totalUnrealizedProfit

        except Exception as e:
            print(f"Ошибка при получении информации о счете фьючерсов: {e}")
            error_inf(e)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток

            if retries <= 1:
                print(f'Не удалось получить баланс счёта. Будут использованы данные из базы.')
                df_account_info = db.get_table_from_db("SELECT * FROM account_info")
                totalWalletBalance = df_account_info.loc[df_account_info['indicator_name'] == 'total_wallet_balance', 'value'].iloc[0]
                availableBalance = df_account_info.loc[df_account_info['indicator_name'] == 'available_balance', 'value'].iloc[0]
                totalUnrealizedProfit = df_account_info.loc[df_account_info['indicator_name'] == 'unrealized_profit', 'value'].iloc[0]

                return totalWalletBalance, availableBalance, totalUnrealizedProfit
            return None
