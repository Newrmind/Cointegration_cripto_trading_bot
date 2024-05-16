from connection import client
from Database import postgres_sql
import pandas as pd
from time_functions import request_time_change
import time
from logger import log_function_info, error_inf

# записываем таблицу в базу данных
def futures_metadata_update() -> None:
    log_function_info("Старт функции futures_metadata_update()")
    retries = 5  # Максимальное количество попыток

    db = postgres_sql.Database()

    while retries > 0:
        try:
            futures_info = client.futures_exchange_info()
            symbols = futures_info['symbols']

            symbols_metadata_actual = pd.DataFrame(symbols)
            symbols_metadata_actual = symbols_metadata_actual.drop(columns=['filters', 'orderTypes', 'timeInForce', 'underlyingSubType']) \
                .loc[symbols_metadata_actual['quoteAsset'] == 'USDT']

            symbols_metadata_actual = symbols_metadata_actual.loc[symbols_metadata_actual['status'] == 'TRADING']
            symbols_metadata_actual = symbols_metadata_actual.loc[symbols_metadata_actual['contractType'] == 'PERPETUAL']
            symbols_metadata_actual = symbols_metadata_actual.loc[symbols_metadata_actual['underlyingType'] == 'COIN']

            symbols_metadata_actual = symbols_metadata_actual.rename(columns={
                'contractType': 'contract_type', 'deliveryDate': 'delivery_date', 'onboardDate': 'onboard_date',
                'maintMarginPercent': 'maint_margin_percent', 'requiredMarginPercent': 'required_margin_percent',
                'baseAsset': 'base_asset', 'quoteAsset': 'quote_asset', 'marginAsset': 'margin_asset',
                'pricePrecision': 'price_precision', 'quantityPrecision': 'quantity_precision',
                'baseAssetPrecision': 'base_asset_precision', 'quotePrecision': 'quote_precision',
                'underlyingType': 'underlying_type', 'settlePlan': 'settle_plan', 'triggerProtect': 'trigger_protect',
                'liquidationFee': 'liquidation_fee', 'marketTakeBound': 'market_take_bound',
                'maxMoveOrderLimit': 'max_move_order_limit',
                })

            symbols_metadata_actual.sort_values('symbol', ascending=True, inplace=True)  # сортируем по алфавиту

            # получаем из БД таблицу с метаданными
            symbols_metadata_db = db.get_futures_metadata()

            # Выполнение сравнения и обновление данных
            # Добавление отсутствующих данных из датафрейма в sqlite таблицу
            new_data = symbols_metadata_actual[~symbols_metadata_actual['symbol'].isin(symbols_metadata_db['symbol'])]
            if not new_data.empty:
                print('Новые тикеры, которых нет в БД: ', new_data['symbol'])
                db.update_futures_metadata(df=new_data, action='append')

            # Удаление данных из sqlite таблицы, которых нет в датафрейме
            data_to_delete = symbols_metadata_db[~symbols_metadata_db['symbol'].isin(symbols_metadata_actual['symbol'])]
            if not data_to_delete.empty:
                print('Тикеры, которые будут удалены из БД: ', data_to_delete)
                db.update_futures_metadata(df=data_to_delete, action='delete')

            request_time_change(db=db, request='futures_metadata')  # записываем время последнего запроса метаданных

            break

        except Exception as e:
            print(f"Ошибка при получении таблицы метаданных: {e}")
            error_inf(e)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток
            return None

