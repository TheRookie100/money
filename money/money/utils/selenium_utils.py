# Este arquivo contém a implementação da automação de cotações do Banco Central
import os
import time
import logging
import pandas as pd
import sys
import io
import random
import uuid
import re
import traceback
import hashlib
from datetime import date, datetime
from typing import List, Dict, Any, Tuple, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from dataclasses import dataclass

# Configuração para lidar com caracteres especiais nos logs
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Configuração de logging formatada
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(
    log_dir, f"currency_automation_{date.today().strftime('%Y%m%d')}.log")

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
    log_dir, f"debug_{date.today().strftime('%Y%m%d')}.log")
debug_handler = logging.FileHandler(debug_log_file, encoding='utf-8')
debug_handler.setFormatter(logging.Formatter(
    "[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S"
))
debug_logger.addHandler(debug_handler)
debug_logger.setLevel(logging.DEBUG)

# Classes para representação dos dados
@dataclass
class CurrencyPair:
    """Representa um par de moedas para conversão"""
    from_currency: str
    to_currency: str

    def __str__(self) -> str:
        return f"{self.from_currency} para {self.to_currency}"


@dataclass
class ExchangeRate:
    """Representa uma cotação de moeda"""
    from_currency: str
    to_currency: str
    rate_value: float
    rate_date: date
    status: str
    execution_time: Optional[float] = None

    def to_dict(self) -> dict:
        """Converte para dicionário (para exportação em Excel)"""
        return {
            "Moeda entrada": self.from_currency,
            "Taxa": 1,
            "Moeda saída": self.to_currency,
            "Valor cotação": round(self.rate_value, 3) if self.rate_value else 0,
            "Data": self.rate_date.strftime("%d/%m/%Y") if isinstance(self.rate_date, date) else self.rate_date,
            "Status": self.status
        }


# Funções para logs específicos e formatados
def log_process_start():
    """Registra o início do processamento de cotações"""
    logger.info("INÍCIO - Processamento de cotações")


def log_process_end(total_count: int, success_count: int, warning_count: int, error_count: int):
    """Registra o fim do processamento com estatísticas"""
    logger.info(
        f"FIM - Processamento de cotações - Total: {total_count} consultas, {success_count} ok, {warning_count} com avisos, {error_count} com erros")


def log_query_start(currency_pair: CurrencyPair):
    """Registra o início de uma consulta de cotação"""
    logger.info(f"CONSULTA - {currency_pair} - INÍCIO")


def log_query_end(currency_pair: CurrencyPair, execution_time: float, rate_value: Optional[float] = None):
    """Registra o fim bem-sucedido de uma consulta de cotação"""
    value_info = f" - Valor: {rate_value:.3f}" if rate_value else ""
    logger.info(
        f"CONSULTA - {currency_pair} - CONCLUÍDO - Tempo: {execution_time:.1f}s{value_info}")


def log_query_warning(currency_pair: CurrencyPair, message: str, execution_time: float):
    """Registra um aviso na consulta de cotação"""
    logger.warning(
        f"CONSULTA - {currency_pair} - AVISO - {message} - Tempo: {execution_time:.1f}s")


def log_query_error(currency_pair: CurrencyPair, error: str, execution_time: Optional[float] = None):
    """Registra um erro na consulta de cotação"""
    time_info = f" - Tempo: {execution_time:.1f}s" if execution_time else ""
    logger.error(f"CONSULTA - {currency_pair} - ERRO - {error}{time_info}")


# Função para logs técnicos detalhados
def debug_log(level, message, exc_info=None):
    """Registra mensagens técnicas detalhadas no log de debug"""
    if level == "INFO":
        debug_logger.info(message)
    elif level == "DEBUG":
        debug_logger.debug(message)
    elif level == "WARNING":
        debug_logger.warning(message)
    elif level == "ERROR":
        debug_logger.error(message, exc_info=exc_info)
    elif level == "CRITICAL":
        debug_logger.critical(message, exc_info=exc_info)


class BCBAutomation:
    """Classe principal para automação de cotações do Banco Central"""

    def __init__(self, headless: bool = True, debug_screenshots: bool = False):
        self.url = "https://www.bcb.gov.br/conversao"
        self.headless = headless
        self.debug_screenshots = debug_screenshots
        self.driver = None

        # Diretórios de entrada e saída - adaptados para estrutura Dagster
        self.input_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))), "assets", "input")
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))), "assets", "output")
        self.screenshots_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))), "assets", "screenshots")

        # Garante que os diretórios existem
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        if self.debug_screenshots:
            os.makedirs(self.screenshots_dir, exist_ok=True)

    def setup_driver(self):
        """Configura o driver do Selenium com opções otimizadas"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")

        # Desabilitar mensagens de console desnecessárias
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # Adicionar user-agent para parecer mais com um navegador real
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")

        # Adicionar preferências para melhorar a estabilidade
        prefs = {
            "profile.default_content_setting_values.notifications": 2,  # Bloquear notificações
            "profile.default_content_settings.popups": 0,  # Bloquear popups
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)

        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )

            # Define um timeout de página padrão mais alto
            self.driver.set_page_load_timeout(60)

            # Maximiza a janela para maior garantia que elementos serão visíveis
            self.driver.maximize_window()

            debug_log("INFO", "Driver do Selenium configurado com sucesso")
        except Exception as e:
            error_msg = f"Erro ao configurar driver: {str(e)}"
            debug_log("ERROR", error_msg, exc_info=True)
            raise

    def close_driver(self):
        """Fecha o driver do Selenium"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                debug_log("INFO", "Driver do Selenium fechado")
            except Exception as e:
                debug_log("ERROR", f"Erro ao fechar o driver: {str(e)}")

    def take_screenshot(self, name: str):
        """Captura screenshot para debug"""
        if self.debug_screenshots and self.driver:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshots_dir}/{name}_{timestamp}.png"
            try:
                self.driver.save_screenshot(filename)
            except Exception:
                pass

    def _close_banners(self):
        """Tenta fechar banners/avisos que possam estar atrapalhando"""
        try:
            # Aguarda um pouco para garantir que os banners são carregados
            time.sleep(3)

            # Lista de seletores específicos para o banner de cookies do BCB
            cookie_selectors = [
                # Seletor por classe específica
                (By.CSS_SELECTOR, "button.btn-primary.btn-accept"),
                # XPath exato fornecido
                (By.XPATH,
                 "/html/body/app-root/bcb-cookies/div/div/div/div/button[2]"),
                # Seletor por texto
                (By.XPATH, "//button[contains(text(), 'Prosseguir')]"),
                # Seletor combinado
                (By.XPATH,
                 "//div[contains(@class, 'text-center')]/button[contains(@class, 'btn-accept')]"),
                # Botão por tipo
                (By.XPATH,
                 "//button[@type='button' and contains(@class, 'btn-primary')]")
            ]

            # Flag para controlar se o banner foi fechado
            cookie_banner_closed = False

            # Tenta cada seletor até encontrar e clicar no botão
            for by, selector in cookie_selectors:
                if cookie_banner_closed:
                    break

                try:
                    elements = self.driver.find_elements(by, selector)

                    for element in elements:
                        try:
                            if element.is_displayed():
                                # Scroll para o elemento
                                self.driver.execute_script(
                                    "arguments[0].scrollIntoView({block: 'center'});", element)
                                time.sleep(0.5)

                                # Tenta clicar de várias maneiras
                                try:
                                    # Tenta clique direto
                                    element.click()
                                except:
                                    try:
                                        # Tenta com JavaScript
                                        self.driver.execute_script(
                                            "arguments[0].click();", element)
                                    except:
                                        # Última tentativa com Actions
                                        from selenium.webdriver.common.action_chains import ActionChains
                                        ActionChains(self.driver).move_to_element(
                                            element).click().perform()

                                cookie_banner_closed = True
                                time.sleep(1)  # Espera para processamento
                                break
                        except:
                            continue
                except:
                    continue

            # Verifica outras possíveis distrações
            generic_selectors = [
                "//button[contains(@class, 'close')]",
                "//button[contains(@class, 'btn-close')]",
                "//button[contains(text(), 'Fechar')]",
                "//a[contains(@class, 'close')]",
                "//div[contains(@class, 'modal-header')]//button"
            ]

            for selector in generic_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            self.driver.execute_script(
                                "arguments[0].click();", element)
                            time.sleep(0.5)
                except:
                    continue

        except:
            # Não falha a execução por erro nesta etapa
            pass

    def read_currency_pairs(self):
        """Lê os pares de moedas do arquivo Excel"""
        filepath = os.path.join(self.input_dir, "currencies.xlsx")

        try:
            # Verifica se a dependência openpyxl está disponível
            try:
                import openpyxl
            except ImportError:
                logger.error(
                    "Pacote 'openpyxl' não encontrado. Este pacote é necessário para ler arquivos Excel.")
                logger.error(
                    "Por favor, instale-o usando: pip install openpyxl")
                raise ValueError(
                    "Dependência necessária não encontrada: openpyxl. Use 'pip install openpyxl' para instalar.")

            # Verifica se o arquivo existe no diretório raiz primeiro
            if os.path.exists("currencies.xlsx"):
                filepath = "currencies.xlsx"
                debug_log(
                    "INFO", f"Usando arquivo de moedas do diretório raiz: {filepath}")
            elif not os.path.exists(filepath):
                debug_log(
                    "WARNING", f"Arquivo {filepath} não encontrado. Criando arquivo de exemplo.")
                filepath = self.create_sample_input_file()

            df = pd.read_excel(filepath)

            # Primeiro, tenta com os nomes de coluna esperados
            try:
                if "Moeda Origem" in df.columns and "Moeda Destino" in df.columns:
                    # Formato português
                    column_from = "Moeda Origem"
                    column_to = "Moeda Destino"
                elif "from" in df.columns and "to" in df.columns:
                    # Formato inglês
                    column_from = "from"
                    column_to = "to"
                else:
                    # Tenta usar as duas primeiras colunas, independente dos nomes
                    if len(df.columns) >= 2:
                        column_from = df.columns[0]
                        column_to = df.columns[1]
                    else:
                        raise ValueError(
                            "Arquivo precisa ter pelo menos duas colunas")
            except Exception as e:
                debug_log("ERROR", f"Erro ao detectar colunas: {str(e)}")
                raise ValueError(f"Erro no formato do arquivo: {str(e)}")

            # Converte para lista de pares de moedas
            currency_pairs = [
                CurrencyPair(
                    from_currency=str(row[column_from]),
                    to_currency=str(row[column_to])
                )
                for _, row in df.iterrows()
            ]

            debug_log("INFO", f"Lidos {len(currency_pairs)} pares de moedas")
            return currency_pairs

        except Exception as e:
            error_msg = f"Erro ao ler arquivo {filepath}: {str(e)}"
            debug_log("ERROR", error_msg, exc_info=True)

            # Se o erro for relacionado a openpyxl, forneça uma mensagem mais clara
            if "openpyxl" in str(e):
                raise ValueError(
                    "Erro ao ler arquivo Excel: pacote 'openpyxl' não instalado. "
                    "Execute 'pip install openpyxl' no terminal para resolver este problema."
                )

            raise ValueError("Erro ao ler pares de moedas: " + str(e))

    def create_sample_input_file(self, force=False):
        """Cria um arquivo de exemplo com pares de moedas."""
        file_path = os.path.join(self.input_dir, "currencies.xlsx")

        if os.path.exists(file_path) and not force:
            debug_log("INFO", f"Arquivo {file_path} já existe.")
            return file_path

        debug_log("INFO", "Criando arquivo de exemplo com pares de moedas")

        # Cria DataFrame com pares de exemplo
        data = {
            'Moeda Origem': ['USD', 'EUR', 'GBP', 'JPY', 'BRL'],
            'Moeda Destino': ['BRL', 'BRL', 'BRL', 'BRL', 'USD']
        }

        # Garante que o diretório existe
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Salva o DataFrame como Excel
        df = pd.DataFrame(data)
        df.to_excel(file_path, index=False)

        debug_log("INFO", f"Arquivo de exemplo criado em {file_path}")

        return file_path

    def _select_currency(self, button_id: str, currency_name: str):
        """Seleciona uma moeda no dropdown - Versão melhorada"""
        try:
            # Localiza o botão do dropdown
            button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, button_id))
            )

            # Scroll até o botão para garantir visibilidade
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)

            # Clica no botão dropdown usando JavaScript
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(1.5)

            # Localiza os itens do dropdown usando seletores específicos
            item_selectors = [
                f"//ul[@id='{button_id.replace('button-', '')}']//a[contains(text(), '{currency_name}')]",
                f"//div[contains(@class, 'dropdown-menu')]//a[contains(text(), '{currency_name}')]",
                f"//a[contains(@class, 'dropdown-item')][contains(text(), '{currency_name}')]"
            ]

            for selector in item_selectors:
                try:
                    items = self.driver.find_elements(By.XPATH, selector)

                    if items:
                        for item in items:
                            try:
                                # Verifica se está visível
                                if item.is_displayed():
                                    # Scroll e clique
                                    self.driver.execute_script(
                                        "arguments[0].scrollIntoView({block: 'center'});",
                                        item
                                    )
                                    time.sleep(0.5)

                                    # Clica usando JavaScript
                                    self.driver.execute_script(
                                        "arguments[0].click();", item)
                                    return True
                            except:
                                continue
                except:
                    continue

            # Se não conseguiu selecionar, tenta abordagem alternativa
            try:
                # Clica novamente no dropdown para garantir que está aberto
                self.driver.execute_script("arguments[0].click();", button)
                time.sleep(1)

                # Tenta usar índice de elementos
                items = self.driver.find_elements(
                    By.XPATH, "//a[contains(@class, 'dropdown-item')]")

                # Procura por texto contendo a moeda
                for item in items:
                    try:
                        item_text = item.text.strip()
                        if currency_name in item_text:
                            self.driver.execute_script(
                                "arguments[0].scrollIntoView({block: 'center'});", item)
                            time.sleep(0.5)
                            self.driver.execute_script(
                                "arguments[0].click();", item)
                            return True
                    except:
                        pass
            except:
                pass

            raise ValueError(
                f"Não foi possível selecionar a moeda: {currency_name}")

        except Exception as e:
            raise ValueError(
                f"Erro ao selecionar moeda {currency_name}: {str(e)}")

    def get_exchange_rate(self, currency_pair: CurrencyPair, rate_date: Optional[date] = None) -> ExchangeRate:
        """Obtém a taxa de câmbio para o par de moedas especificado"""
        if not rate_date:
            rate_date = date.today()

        start_time = time.time()
        log_query_start(currency_pair)

        try:
            if not self.driver:
                self.setup_driver()

            # Acessa a página de conversão
            self.driver.get(self.url)

            # Aguarda carregar a página
            wait = WebDriverWait(self.driver, 30)
            wait.until(EC.presence_of_element_located(
                (By.ID, "button-converter-de")))

            # Tenta fechar quaisquer banners/avisos
            self._close_banners()

            # Seleciona a moeda de origem
            self._select_currency("button-converter-de",
                                  currency_pair.from_currency)

            # Seleciona a moeda de destino
            self._select_currency("button-converter-para",
                                  currency_pair.to_currency)

            # Preenche a data
            try:
                # Lista de possíveis seletores para o campo de data
                date_input_selectors = [
                    "//input[@placeholder='DD/MM/AAAA']",
                    "//input[contains(@placeholder, 'DD')]",
                    "//input[contains(@class, 'form-control') and @type='text']",
                    "//form//input[@type='text']",
                    "//label[contains(text(), 'Data')]/following-sibling::input",
                    "//input[@id='data-moeda']"
                ]

                date_input = None
                for selector in date_input_selectors:
                    try:
                        elements = self.driver.find_elements(
                            By.XPATH, selector)
                        for element in elements:
                            if element.is_displayed():
                                date_input = element
                                break
                        if date_input:
                            break
                    except:
                        continue

                if not date_input:
                    # Se não encontrou com seletores diretos, tenta JavaScript
                    date_str = rate_date.strftime('%d/%m/%Y')
                    js_script = f"""
                    var inputs = document.getElementsByTagName('input');
                    for(var i = 0; i < inputs.length; i++) {{
                        if(inputs[i].type === 'text') {{
                            inputs[i].value = '{date_str}';
                            inputs[i].dispatchEvent(new Event('change', {{ bubbles: true }}));
                            inputs[i].dispatchEvent(new Event('blur', {{ bubbles: true }}));
                            return true;
                        }}
                    }}
                    return false;
                    """

                    self.driver.execute_script(js_script)
                else:
                    # Limpa o campo antes
                    date_input.clear()

                    # Preenche o campo de data
                    date_str = rate_date.strftime('%d/%m/%Y')
                    date_input.send_keys(date_str)

                    # Também tenta via JavaScript para garantir
                    self.driver.execute_script(
                        "arguments[0].value = arguments[1];", date_input, date_str)

                    # Simula eventos para validação
                    self.driver.execute_script(
                        "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));"
                        "arguments[0].dispatchEvent(new Event('blur', { bubbles: true }));",
                        date_input
                    )

            except Exception as e:
                debug_log("ERROR", f"Erro ao preencher data: {str(e)}")

            # Clica no botão de pesquisa/conversão usando vários métodos
            try:
                # Lista de possíveis seletores para o botão de pesquisa
                search_button_selectors = [
                    "//button[@class='btn btn-primary' or contains(text(), 'Converter')]",
                    "//button[contains(text(), 'Converter')]",
                    "//button[@type='submit']",
                    "//button[contains(@class, 'btn-primary')]",
                    "//form//button"
                ]

                search_button = None
                for selector in search_button_selectors:
                    try:
                        elements = self.driver.find_elements(
                            By.XPATH, selector)
                        for element in elements:
                            if element.is_displayed():
                                search_button = element
                                break
                        if search_button:
                            break
                    except:
                        continue

                if search_button:
                    # Tenta clicar de várias maneiras
                    try:
                        # Scroll para garantir visibilidade
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({block: 'center'});", search_button)
                        time.sleep(0.5)

                        # Tenta clique direto
                        search_button.click()
                    except:
                        try:
                            # Tenta com JavaScript
                            self.driver.execute_script(
                                "arguments[0].click();", search_button)
                        except:
                            # Última tentativa com Actions
                            from selenium.webdriver.common.action_chains import ActionChains
                            ActionChains(self.driver).move_to_element(
                                search_button).click().perform()
                else:
                    # Se não encontrou botão específico, tenta por JavaScript
                    js_click = """
                    var buttons = document.getElementsByTagName('button');
                    for(var i = 0; i < buttons.length; i++) {
                        if(buttons[i].textContent.includes('Convert') || 
                           buttons[i].classList.contains('btn-primary') || 
                           buttons[i].type === 'submit') {
                            buttons[i].click();
                            return true;
                        }
                    }
                    return false;
                    """

                    self.driver.execute_script(js_click)

            except Exception as e:
                debug_log(
                    "ERROR", f"Erro ao clicar no botão de conversão: {str(e)}")

            # Aguarda o resultado aparecer
            # Aguarda mais tempo para processamento da requisição
            time.sleep(5)
            self.take_screenshot("result_screen_capture")

            # Extrai o resultado
            rate_value, actual_date = self._extract_result()

            execution_time = time.time() - start_time

            # Define status com base no resultado
            if rate_value > 0:
                if actual_date != rate_date:
                    status = "Cotação encontrada não é da data solicitada"
                    log_query_warning(
                        currency_pair, "Data diferente da solicitada", execution_time)
                else:
                    status = "Consulta ok"
                    log_query_end(
                        currency_pair, execution_time, rate_value)
            else:
                status = "Valor não encontrado"
                log_query_warning(
                    currency_pair, "Valor não encontrado", execution_time)

            return ExchangeRate(
                from_currency=currency_pair.from_currency,
                to_currency=currency_pair.to_currency,
                rate_value=rate_value,
                rate_date=actual_date,
                status=status,
                execution_time=execution_time
            )

        except Exception as e:
            execution_time = time.time() - start_time
            log_query_error(currency_pair, str(e), execution_time)

            # Captura screenshot do erro
            self.take_screenshot(
                f"error_{currency_pair.from_currency}_{currency_pair.to_currency}")

            return ExchangeRate(
                from_currency=currency_pair.from_currency,
                to_currency=currency_pair.to_currency,
                rate_value=0.0,
                rate_date=rate_date,
                status=f"Erro: {str(e)}",
                execution_time=execution_time
            )

    def _extract_result(self) -> Tuple[float, date]:
        """Extrai o resultado da conversão e a data usando abordagens variadas"""
        try:
            # Valor padrão caso não consiga extrair
            rate_value = 0.0
            actual_date = date.today()

            # Captura todo o texto do resultado
            result_text = ""
            try:
                # Captura o elemento que contém o resultado completo
                result_container = self.driver.find_element(
                    By.XPATH, "//div[contains(@class, 'card-body')]")
                if result_container:
                    result_text = result_container.text
                    debug_log(
                        "INFO", f"Texto completo encontrado: {result_text}")
            except Exception as e:
                debug_log(
                    "DEBUG", f"Erro ao capturar container de resultado: {str(e)}")
                # Captura HTML da página como alternativa
                result_text = self.driver.page_source

            # Extração da taxa de câmbio usando padrões específicos do BCB
            # Padrão 1: "Resultado da conversão: X,XXXX"
            conversion_match = re.search(
                r'Resultado da conversão:?\s*([\d,.]+)', result_text)
            if conversion_match:
                value_str = conversion_match.group(
                    1).replace(".", "").replace(",", ".")
                try:
                    rate_value = float(value_str)
                    debug_log(
                        "INFO", f"Valor extraído do padrão 'Resultado da conversão': {rate_value}")
                except ValueError:
                    debug_log(
                        "WARNING", f"Não foi possível converter '{value_str}' para número")

            # Padrão 2: "1 MoedaA = X,XXXX MoedaB"
            if rate_value == 0.0:
                tax_match = re.search(
                    r'1\s+[\w/()]+\s+=\s+([\d,.]+)', result_text)
                if tax_match:
                    value_str = tax_match.group(1).replace(
                        ".", "").replace(",", ".")
                    try:
                        rate_value = float(value_str)
                        debug_log(
                            "INFO", f"Valor extraído do padrão de taxa: {rate_value}")
                    except ValueError:
                        debug_log(
                            "WARNING", f"Não foi possível converter '{value_str}' para número")

            # Extração da data usando o padrão específico do BCB
            date_match = re.search(
                r'Data cotação utilizada:?\s*(\d{2}/\d{2}/\d{4})', result_text)
            if date_match:
                date_str = date_match.group(1)
                try:
                    actual_date = datetime.strptime(
                        date_str, "%d/%m/%Y").date()
                    debug_log("INFO", f"Data extraída: {actual_date}")
                except ValueError:
                    debug_log(
                        "WARNING", f"Não foi possível converter '{date_str}' para data")

            # Se não encontrou com os padrões específicos, usa as abordagens genéricas existentes
            if rate_value == 0.0:
                # Abordagens existentes para extração de taxa
                result_selectors = [
                    "//strong[contains(text(), 'Resultado da conversão')]/following-sibling::text()",
                    "//strong[contains(text(), 'Resultado da conversão')]/..",
                    "//h3[contains(text(), 'Resultado da conversão')]/..",
                    "//div[contains(@class, 'card-body')]//strong[2]"
                ]

                for selector in result_selectors:
                    try:
                        elements = self.driver.find_elements(
                            By.XPATH, selector)
                        if elements:
                            for element in elements:
                                # Extrai o texto
                                elem_text = element.text
                                debug_log(
                                    "INFO", f"Texto encontrado: {elem_text}")

                                # Procura por um valor numérico no formato X,XXX ou X.XXX
                                number_match = re.search(
                                    r'(\d+[,.]\d+)', elem_text)
                                if number_match:
                                    value_str = number_match.group(
                                        1).replace(".", "").replace(",", ".")
                                    try:
                                        rate_value = float(value_str)
                                        debug_log(
                                            "INFO", f"Valor extraído com sucesso: {rate_value}")
                                        break
                                    except ValueError:
                                        debug_log(
                                            "WARNING", f"Não foi possível converter '{value_str}' para número")
                    except Exception as e:
                        debug_log(
                            "DEBUG", f"Erro ao tentar extrair com seletor {selector}: {str(e)}")

            # Abordagem para data se ainda não encontrou
            if actual_date == date.today():
                date_selectors = [
                    "//strong[contains(text(), 'Data cotação')]/..",
                    "//div[contains(text(), 'Data')]"
                ]

                for selector in date_selectors:
                    try:
                        elements = self.driver.find_elements(
                            By.XPATH, selector)
                        if elements:
                            for element in elements:
                                date_text = element.text
                                date_match = re.search(
                                    r'(\d{2}/\d{2}/\d{4})', date_text)
                                if date_match:
                                    date_str = date_match.group(1)
                                    try:
                                        actual_date = datetime.strptime(
                                            date_str, "%d/%m/%Y").date()
                                        debug_log(
                                            "INFO", f"Data extraída: {actual_date}")
                                        break
                                    except ValueError:
                                        debug_log(
                                            "WARNING", f"Não foi possível converter '{date_str}' para data")
                    except Exception as e:
                        debug_log(
                            "DEBUG", f"Erro ao tentar extrair data com seletor {selector}: {str(e)}")

            return rate_value, actual_date

        except Exception as e:
            debug_log(
                "ERROR", f"Erro ao extrair resultado: {str(e)}", exc_info=True)
            # Retorna valores padrão para não bloquear o processo
            return 0.0, date.today()

    def extract_all_exchange_rates(self, currency_pairs: List[CurrencyPair], rate_date: Optional[date] = None) -> List[ExchangeRate]:
        """Extrai cotações para uma lista de pares de moedas"""
        if not rate_date:
            rate_date = date.today()

        # Log de início do processamento exatamente conforme solicitado
        logger.info("INÍCIO - Processamento de cotações")
        debug_log(
            "INFO", f"Iniciando extração de {len(currency_pairs)} cotações para a data {rate_date.strftime('%d/%m/%Y')}")

        results = []
        success_count = 0
        warning_count = 0
        error_count = 0

        try:
            for currency_pair in currency_pairs:
                try:
                    # Log de início da consulta é feito dentro de get_exchange_rate

                    exchange_rate = self.get_exchange_rate(
                        currency_pair=currency_pair,
                        rate_date=rate_date
                    )

                    results.append(exchange_rate)

                    if exchange_rate.rate_value > 0:
                        if "não é da data" in exchange_rate.status:
                            warning_count += 1
                            # Log já feito dentro do get_exchange_rate
                        else:
                            success_count += 1
                            # Log já feito dentro do get_exchange_rate
                    else:
                        error_count += 1
                        # Log já feito dentro do get_exchange_rate

                except Exception as e:
                    error_count += 1

                    # Cria um registro de erro para manter o processamento das demais moedas
                    results.append(
                        ExchangeRate(
                            from_currency=currency_pair.from_currency,
                            to_currency=currency_pair.to_currency,
                            rate_value=0.0,
                            rate_date=rate_date,
                            status=f"Erro: {str(e)}"
                        )
                    )

                # Um pequeno delay entre consultas para evitar bloqueio
                time.sleep(2)

            # Log de fim de processamento no formato padrão solicitado
            logger.info(
                f"FIM - Processamento de cotações - Total: {len(currency_pairs)} consultas, {success_count} ok, {warning_count} com avisos, {error_count} com erros")
            debug_log(
                "INFO", f"Extração concluída: {success_count} sucessos, {warning_count} avisos, {error_count} erros")
            return results

        finally:
            # Garante que o driver seja fechado ao final
            self.close_driver()

    def write_exchange_rates(self, exchange_rates: List[ExchangeRate], filename: Optional[str] = None) -> str:
        """Escreve as cotações em arquivo Excel com tratamento de erros de permissão"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            today = datetime.now().strftime("%Y%m%d")

            # Gera um hash único para o arquivo baseado nas entradas
            input_str = str(
                [f"{rate.from_currency}{rate.to_currency}{rate.rate_value}" for rate in exchange_rates])
            unique_hash = hashlib.md5(input_str.encode()).hexdigest()[:8]

            # Cria o nome do arquivo
            if not filename:
                filename = f"exchange_rates_{today}_{unique_hash}.xlsx"

            output_path = os.path.join(self.output_dir, filename)
            main_dir_path = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), filename)

            # Cria diretório de saída se não existir
            os.makedirs(self.output_dir, exist_ok=True)

            # Cria um DataFrame do pandas com os resultados
            data = []

            for rate in exchange_rates:
                # Extrai os dados da taxa
                from_currency = rate.from_currency
                to_currency = rate.to_currency
                rate_value = rate.rate_value  # Usa o valor real extraído

                # Formata a data corretamente
                if isinstance(rate.rate_date, date):
                    date_str = rate.rate_date.strftime("%d/%m/%Y")
                else:
                    date_str = str(rate.rate_date)

                # Define o status da cotação
                status = rate.status

                # Adiciona a linha ao dataframe
                data.append({
                    'Moeda entrada': from_currency,
                    'Taxa': 1,
                    'Moeda saída': to_currency,
                    # Usa o valor real da cotação
                    'Valor cotação': round(rate_value, 3) if rate_value else 0,
                    'Data': date_str,
                    'Status': status
                })

            # Cria o dataframe e salva como Excel
            df = pd.DataFrame(data)

            # Salva o arquivo no diretório de saída
            df.to_excel(output_path, index=False)
            debug_log(
                "INFO", f"Arquivo Excel gerado com sucesso em: {output_path}")

            # Salva também no diretório principal para facilitar o acesso
            df.to_excel(main_dir_path, index=False)
            debug_log(
                "INFO", f"Arquivo Excel também salvo no diretório principal: {main_dir_path}")

            return main_dir_path

        except PermissionError as e:
            # Trata erro de permissão tentando salvar com nome alternativo
            alt_filename = f"exchange_rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}.xlsx"
            alt_output_path = os.path.join(self.output_dir, alt_filename)
            alt_main_dir_path = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), alt_filename)

            debug_log(
                "WARNING", f"Erro de permissão ao salvar arquivo. Tentando nome alternativo: {alt_filename}")

            # Tenta salvar com o nome alternativo
            df = pd.DataFrame(data)
            df.to_excel(alt_output_path, index=False)
            df.to_excel(alt_main_dir_path, index=False)

            debug_log(
                "INFO", f"Arquivo Excel salvo com nome alternativo: {alt_main_dir_path}")
            return alt_main_dir_path

        except Exception as e:
            error_msg = f"Erro ao gerar arquivo Excel: {str(e)}"
            debug_log("ERROR", error_msg, exc_info=True)

            # Tenta salvar em diretório temporário como última alternativa
            try:
                import tempfile
                temp_dir = tempfile.gettempdir()
                temp_filename = f"exchange_rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                temp_filepath = os.path.join(temp_dir, temp_filename)

                df = pd.DataFrame(data)
                df.to_excel(temp_filepath, index=False)

                debug_log(
                    "INFO", f"Arquivo Excel salvo em diretório alternativo: {temp_filepath}")
                return temp_filepath
            except Exception as e2:
                error_msg = f"Erro ao salvar em diretório temporário: {str(e2)}"
                debug_log("ERROR", error_msg, exc_info=True)
                raise ValueError(
                    f"Falha ao salvar arquivo Excel: {str(e)}, tentativa alternativa falhou: {str(e2)}")

    def run(self):
        """Executa o fluxo completo de automação"""
        try:
            start_time = time.time()
            debug_log("INFO", "Iniciando automação de cotações do Banco Central")

            # Verifica se o arquivo de entrada existe, se não, cria
            input_file = os.path.join(self.input_dir, "currencies.xlsx")

            try:
                # Tenta ler os pares de moedas
                currency_pairs = self.read_currency_pairs()
            except ValueError as e:
                # Se ocorrer erro de formato, recria o arquivo
                if "Coluna obrigatória não encontrada" in str(e):
                    debug_log(
                        "WARNING", f"Arquivo {input_file} com formato incorreto. Recriando arquivo de exemplo.")
                    self.create_sample_input_file(force=True)
                    # Tenta ler novamente após criar o arquivo
                    currency_pairs = self.read_currency_pairs()
                else:
                    # Se for outro tipo de erro ValueError, propaga
                    raise

            # Extrai as cotações
            exchange_rates = self.extract_all_exchange_rates(currency_pairs)

            # Gera o relatório
            output_file = self.write_exchange_rates(exchange_rates)

            # Calcula o tempo total de execução
            elapsed_time = time.time() - start_time

            # Log de finalização
            debug_log(
                "INFO", f"Automação concluída com sucesso em {elapsed_time:.2f} segundos. Relatório gerado em {output_file}")

            # Retorna o caminho do arquivo de saída para uso posterior
            return output_file

        except Exception as e:
            error_msg = f"Erro na execução da automação: {str(e)}"
            debug_log("ERROR", error_msg, exc_info=True)
            raise


def main():
    """Menu principal"""
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n====== AUTOMAÇÃO DE COTAÇÕES DO BANCO CENTRAL ======")
        print("1. Executar automação completa")
        print("2. Debug do banner de cookies")
        print("3. Criar arquivo de exemplo de cotações")
        print("4. Executar automação com navegador visível")
        print("5. Recriar arquivo de exemplo de cotações (substituir existente)")
        print("0. Sair")

        option = input("\nEscolha uma opção: ")

        if option == "1":
            try:
                start_time = time.time()
                automation = BCBAutomation()
                output_file = automation.run()
                elapsed_time = time.time() - start_time

                print(f"\nAutomação concluída em {elapsed_time:.2f} segundos.")
                print(f"Relatório gerado em: {output_file}")

            except Exception as e:
                print(f"\nErro na execução da automação: {str(e)}")
                debug_log(
                    "ERROR", f"Erro detalhado: {traceback.format_exc()}", exc_info=True)

        elif option == "2":
            try:
                print("\n--- INICIANDO DEBUG DO BANNER DE COOKIES ---")
                # Código simplificado de debug do banner
                automation = BCBAutomation(
                    headless=False, debug_screenshots=True)
                automation.setup_driver()

                try:
                    automation.driver.get("https://www.bcb.gov.br/conversao")
                    time.sleep(5)

                    # Salva screenshot
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"debug/initial_page_{timestamp}.png"
                    os.makedirs("debug", exist_ok=True)
                    automation.driver.save_screenshot(filename)
                    print(f"Screenshot inicial salvo: {filename}")

                    # Tenta fechar o banner
                    automation._close_banners()

                    # Aguarda para verificar resultado
                    time.sleep(5)
                    after_filename = f"debug/after_banner_close_{timestamp}.png"
                    automation.driver.save_screenshot(after_filename)
                    print(
                        f"Screenshot após interação com banner: {after_filename}")

                    print("\nDebug do banner de cookies concluído!")

                finally:
                    automation.close_driver()

            except Exception as e:
                print(f"\nErro no debug do banner: {str(e)}")
                debug_log(
                    "ERROR", f"Erro detalhado: {traceback.format_exc()}", exc_info=True)

        elif option == "3":
            try:
                automation = BCBAutomation()
                filepath = automation.create_sample_input_file()
                print(f"\nArquivo de exemplo criado em: {filepath}")
            except Exception as e:
                print(f"\nErro ao criar arquivo de exemplo: {str(e)}")
                debug_log(
                    "ERROR", f"Erro detalhado: {traceback.format_exc()}", exc_info=True)

        elif option == "4":
            try:
                start_time = time.time()
                automation = BCBAutomation(
                    headless=False, debug_screenshots=True)  # Navegador visível
                output_file = automation.run()
                elapsed_time = time.time() - start_time

                print(f"\nAutomação concluída em {elapsed_time:.2f} segundos.")
                print(f"Relatório gerado em: {output_file}")

            except Exception as e:
                print(f"\nErro na execução da automação: {str(e)}")
                debug_log(
                    "ERROR", f"Erro detalhado: {traceback.format_exc()}", exc_info=True)

        elif option == "5":
            try:
                automation = BCBAutomation()
                filepath = automation.create_sample_input_file(force=True)
                print(f"\nArquivo de exemplo recriado em: {filepath}")
            except Exception as e:
                print(f"\nErro ao recriar arquivo de exemplo: {str(e)}")
                debug_log(
                    "ERROR", f"Erro detalhado: {traceback.format_exc()}", exc_info=True)

        elif option == "0":
            print("\nEncerrando programa...")
            return

        else:
            print("\nOpção inválida! Por favor, escolha uma opção válida.")

        # Perguntar se deseja continuar
        input("\nPressione ENTER para continuar...")
        main()  # Chama recursivamente para voltar ao menu

    except KeyboardInterrupt:
        print("\nPrograma interrompido pelo usuário.")
    except Exception as e:
        print(f"\nErro inesperado: {str(e)}")
        debug_log(
            "ERROR", f"Erro na interface principal: {traceback.format_exc()}", exc_info=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erro fatal: {str(e)}")
        debug_log(
            "CRITICAL", f"Erro fatal na inicialização: {traceback.format_exc()}", exc_info=True)