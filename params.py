from connection import postgres_user, postgres_password, postgres_dbname, postgres_server_ip

postgres_connection_info = {
    'host': '127.0.0.1',
    'dbname': postgres_dbname,
    'user': postgres_user,
    'password': postgres_password,
    'port': '5432'
}

postgres_connection_server = {
    'host': postgres_server_ip,
    'dbname': postgres_dbname,
    'user': postgres_user,
    'password': postgres_password,
    'port': '5432'
}

# Список таймфреймов, с которыми будет работать программа
intervals = ['15m']

# Время, с которого начинается загрузка свечных данных
start_time_default = 1688158800000  # 01.07.2023
end_time_default = 9999999990000

# Количество секунд для выборки данных для анализа
amount_klines = {'5m': 900000, '15m': 3600000}

analyze_period = 0.25  # интервал анализа данных в часах
check_analyze_period = 0.35  # интервал анализа данных в часах (для проверки в модуле trading)

# trading_params
trading_params = {
    'quantity_positions': 999,  # максимум одновременно открытых позиций
    'percent_for_trade': 0.3,  # процент от депозита на 1 сделку
    'transposition_tp': 3600000,  # время, через которое будет изменён тейк
    'min_totalPositionInitialMargin': 0.3,  # минимальный процент депозита, ниже которого прекращается открытие новых позиций
    'factor': 0.005,  # процент влияния проскальзывания на результат, выше которого сделки не совершаются
    'factor_for_sl': 0.03,  # процент влияния проскальзывания на результат, выше которого стоп не сработает
    'factor_for_tp': 0.03,  # процент влияния проскальзывания на результат, выше которого тейк не сработает
    'sl_std': 3,  # стоп-лос (кол-во стандартных отклонений)
    'tp_30': 0.5,  # множитель для расчёта tp_30
    'tp_50': 0.4,  # множитель для расчёта tp_50
    'tp_80': 0.2,  # множитель для расчёта tp_80
    'dif_perc_max': 50,  # допустимый процент отклонения от среднего стандартного отклонения в большую сторону
    'dif_perc_min': -60  # допустимый процент отклонения от среднего стандартного отклонения в меньшую сторону
                    }

blacklist_time = 12  # кол-во часов, на которое спред помещается в чёрный список после стопа (float)

trading_allowed = False  # параметр, разрешающий торговлю