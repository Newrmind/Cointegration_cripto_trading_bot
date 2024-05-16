import params
from connection import client
import pandas as pd
import time
from Database import postgres_sql


# записываем таблицу в базу данных
def futures_database_entry() -> None:
    retries = 5  # Максимальное количество попыток
    db = postgres_sql.Database(db_info=params.postgres_connection_info)

    while retries > 0:
        try:
            futures_info = client.futures_exchange_info()
            symbols = futures_info['symbols']

            df_futures_symbols = pd.DataFrame(symbols)
            df_futures_symbols = df_futures_symbols.drop(columns=['filters', 'orderTypes', 'timeInForce', 'underlyingSubType']) \
                .loc[df_futures_symbols['quoteAsset'] == 'USDT']

            df_futures_symbols = df_futures_symbols.loc[df_futures_symbols['status'] == 'TRADING']
            df_futures_symbols = df_futures_symbols.loc[df_futures_symbols['contractType'] == 'PERPETUAL']

            df_futures_symbols = df_futures_symbols.rename(columns={
                'contractType': 'contract_type', 'deliveryDate': 'delivery_date', 'onboardDate': 'onboard_date',
                'maintMarginPercent': 'maint_margin_percent', 'requiredMarginPercent': 'required_margin_percent',
                'baseAsset': 'base_asset', 'quoteAsset': 'quote_asset', 'marginAsset': 'margin_asset',
                'pricePrecision': 'price_precision', 'quantityPrecision': 'quantity_precision',
                'baseAssetPrecision': 'base_asset_precision', 'quotePrecision': 'quote_precision',
                'underlyingType': 'underlying_type', 'settlePlan': 'settle_plan', 'triggerProtect': 'trigger_protect',
                'liquidationFee': 'liquidation_fee', 'marketTakeBound': 'market_take_bound',
                'maxMoveOrderLimit': 'max_move_order_limit',
            })

            df_futures_symbols.sort_values('symbol', ascending=True, inplace=True)  # сортируем по алфавиту
            db.add_table_to_db(df_futures_symbols, 'futures_metadata', 'replace')  # запись в БД

            # Запускаем функцию, добавляющую каскадное удаление
            db.cascade_del()
            break

        except Exception as e:
            print(f"Ошибка при получении таблицы метаданных: {e}")
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток
            return None

