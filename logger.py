import logging
import sys
import inspect

# Создаем форматтер для логов с указанием имени функции
log_format = '%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(filename='app.log', level=logging.INFO, format=log_format)

logger = logging.getLogger(__name__)


def log_function_info(message):
    current_function = inspect.currentframe().f_back.f_code.co_name
    logger.info(f"[{current_function}] - {message}")

def error_inf(exception_message):
    """Записывает ошибку в логи"""
    current_function = inspect.currentframe().f_back.f_code.co_name
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno
    error_message = f"Error in module '{file_name}', function: [{current_function}] line {line_number}: {exception_message}"

    # Логируем сообщение с информацией о модуле и номере строки
    logger.error(error_message)


def clear_log_file():
    log_file = 'app.log'
    try:
        # Чтение содержимого файла логов
        with open(log_file, 'r') as file:
            lines = file.readlines()

        # Если в файле больше 10000 строк, оставляем только последние 3000 строк
        if len(lines) > 10000:
            lines = lines[-3000:]

            # Запись обновленных строк в файл
            with open(log_file, 'w') as file:
                file.writelines(lines)

            logger.info("Log file was cleared!")

    except Exception as e:
        logger.error(f"Error clearing log file: {e}")

