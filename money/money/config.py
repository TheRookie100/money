"""Configurações centralizadas para o projeto"""

import os
import logging
import sys
import io
from datetime import date

# Configuração de diretórios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "assets", "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "assets", "output")
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "assets", "screenshots")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
# Configurações de implementação
IMPLEMENTATION = "selenium"  # "selenium" ou "scrapy"

# Configurações do Selenium
HEADLESS = True  # Se True, o navegador roda em modo headless
TAKE_SCREENSHOTS = True  # Se True, screenshots são salvas durante a execução

# Configuração de parâmetros de consulta
DEFAULT_CURRENCY_FILE = os.path.join(INPUT_DIR, "currencies.xlsx")
DEFAULT_OUTPUT_FORMAT = "xlsx"  # Formato de saída padrão (xlsx, csv, json)

# Configurações de timeout e retry
REQUEST_TIMEOUT = 30  # Timeout para requisições em segundos
MAX_RETRIES = 3  # Número máximo de tentativas em caso de falha

# Garante que os diretórios existem
# os.makedirs(INPUT_DIR, exist_ok=True)
# os.makedirs(OUTPUT_DIR, exist_ok=True)
# os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
# os.makedirs(LOGS_DIR, exist_ok=True)

def setup_logging():
    """Configura o logging para o projeto"""
    # Configuração para lidar com caracteres especiais nos logs
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    # Nome do arquivo de log
    log_file = os.path.join(
        LOGS_DIR, f"currency_automation_{date.today().strftime('%Y%m%d')}.log")

    # Formato simplificado para os logs principais
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Logger principal
    logger = logging.getLogger("currency_automation")

    # Logger separado para logs detalhados/técnicos
    debug_logger = logging.getLogger("debug_logger")
    debug_log_file = os.path.join(
        LOGS_DIR, f"debug_{date.today().strftime('%Y%m%d')}.log")
    debug_handler = logging.FileHandler(debug_log_file, encoding='utf-8')
    debug_handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S"
    ))
    debug_logger.addHandler(debug_handler)
    debug_logger.setLevel(logging.DEBUG)
    
    return logger, debug_logger