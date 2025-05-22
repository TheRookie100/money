"""Spider Scrapy para extração de cotações do Banco Central usando Selenium integrado"""

import scrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import date, datetime
import re
import time
from typing import List, Optional, Tuple
from dataclasses import dataclass

# Instalar webdriver-manager automaticamente se necessário
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "webdriver-manager"])
    from webdriver_manager.chrome import ChromeDriverManager

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

class MoedasSpider(scrapy.Spider):
    name = "moedas"
    allowed_domains = ["bcb.gov.br"]
    start_urls = ["https://www.bcb.gov.br/conversao"]
    
    # Configurações básicas do Scrapy
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
    }

    def __init__(self, *args, **kwargs):
        super(MoedasSpider, self).__init__(*args, **kwargs)
        
        # Pares de moedas padrão para teste
        self.currency_pairs = [
            CurrencyPair("USD", "BRL"),
            CurrencyPair("EUR", "BRL"),
            CurrencyPair("GBP", "BRL"),
        ]
        
        # Data alvo para consulta
        self.target_date = date.today()
        
        # Controle de processamento
        self.current_pair_index = 0
        self.results = []
        self.start_time = time.time()
        
        # Inicializar o driver Selenium
        self.driver = None
        self._setup_selenium_driver()
        
        self.logger.info(f"INÍCIO - Processamento de cotações")
        self.logger.info(f"Iniciando com {len(self.currency_pairs)} pares de moedas")

    def _setup_selenium_driver(self):
        """Configura o driver Selenium"""
        try:
            # Configurações do Chrome
            chrome_options = Options()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36')
            
            # Configurar o serviço do Chrome
            service = Service(ChromeDriverManager().install())
            
            # Criar o driver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.implicitly_wait(10)
            
            self.logger.info("Driver Selenium configurado com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao configurar driver Selenium: {str(e)}")
            self.driver = None

    def parse(self, response):
        """Método principal que processa todas as cotações"""
        if not self.driver:
            self.logger.error("Driver Selenium não está disponível")
            return
        
        # Processa todos os pares de moedas
        for i, pair in enumerate(self.currency_pairs):
            self.current_pair_index = i
            self.logger.info(f"CONSULTA - {pair} - INÍCIO")
            
            start_time = time.time()
            
            try:
                # Navega para a página do BCB
                self.driver.get("https://www.bcb.gov.br/conversao")
                
                # Aguarda a página carregar
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@id, 'button-converter-de')]"))
                )
                
                # Tenta fechar banners de cookies
                self._close_banners()
                
                # Realiza a conversão
                rate_value, actual_date = self._perform_conversion(pair)
                
                # Calcula o tempo de execução
                execution_time = time.time() - start_time
                
                # Determina status
                if rate_value > 0:
                    if actual_date != self.target_date:
                        status = "Cotação encontrada não é da data solicitada"
                        self.logger.warning(f"CONSULTA - {pair} - AVISO - Data diferente da solicitada - Tempo: {execution_time:.1f}s")
                    else:
                        status = "Consulta ok"
                        self.logger.info(f"CONSULTA - {pair} - CONCLUÍDO - Tempo: {execution_time:.1f}s - Valor: {rate_value:.3f}")
                else:
                    status = "Valor não encontrado"
                    self.logger.warning(f"CONSULTA - {pair} - AVISO - Valor não encontrado - Tempo: {execution_time:.1f}s")
                
                # Cria o objeto de resultado
                exchange_rate = ExchangeRate(
                    from_currency=pair.from_currency,
                    to_currency=pair.to_currency,
                    rate_value=rate_value,
                    rate_date=actual_date,
                    status=status,
                    execution_time=execution_time
                )
                
                # Adiciona ao resultado
                self.results.append(exchange_rate)
                
                # Retorna como item
                yield exchange_rate.to_dict()
                
            except Exception as e:
                self.logger.error(f"CONSULTA - {pair} - ERRO - {str(e)}")
                
                # Cria resultado com erro
                execution_time = time.time() - start_time
                error_result = ExchangeRate(
                    from_currency=pair.from_currency,
                    to_currency=pair.to_currency,
                    rate_value=0.0,
                    rate_date=self.target_date,
                    status=f"Erro: {str(e)}",
                    execution_time=execution_time
                )
                
                yield error_result.to_dict()
        
        # Log final
        success_count = sum(1 for r in self.results if r.status == "Consulta ok")
        warning_count = sum(1 for r in self.results if "não é da data" in r.status)
        error_count = len(self.results) - success_count - warning_count
        
        self.logger.info(
            f"FIM - Processamento de cotações - Total: {len(self.results)} consultas, "
            f"{success_count} ok, {warning_count} com avisos, {error_count} com erros"
        )

    def _perform_conversion(self, pair: CurrencyPair) -> Tuple[float, date]:
        """Realiza a conversão de uma moeda específica"""
        try:
            # Seleciona moeda de origem
            self._select_currency("button-converter-de", pair.from_currency)
            
            # Seleciona moeda de destino
            self._select_currency("button-converter-para", pair.to_currency)
            
            # Preenche data
            self._fill_date(self.target_date)
            
            # Clica no botão de converter
            self._click_convert_button()
            
            # Aguarda o resultado
            time.sleep(3)
            
            # Extrai o resultado
            return self._extract_result()
            
        except Exception as e:
            self.logger.error(f"Erro durante conversão: {str(e)}")
            return 0.0, self.target_date

    def _close_banners(self):
        """Tenta fechar banners/avisos que possam estar atrapalhando"""
        try:
            # Aguarda um pouco para garantir que os banners são carregados
            time.sleep(2)

            # Lista de seletores específicos para o banner de cookies do BCB
            cookie_selectors = [
                "//button[contains(@class, 'btn-primary') and contains(@class, 'btn-accept')]",
                "//button[contains(text(), 'Prosseguir')]",
                "//div[contains(@class, 'text-center')]/button[contains(@class, 'btn-accept')]",
                "//button[contains(text(), 'Aceitar')]",
                "//button[contains(text(), 'OK')]"
            ]

            for selector in cookie_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            self.driver.execute_script("arguments[0].click();", element)
                            time.sleep(1)
                            self.logger.info("Banner de cookies fechado")
                            return
                except:
                    continue
        except:
            # Não falha a execução por erro nesta etapa
            pass

    def _select_currency(self, button_id: str, currency_name: str):
        """Seleciona uma moeda no dropdown"""
        try:
            # Clica no botão dropdown
            button = self.driver.find_element(By.ID, button_id)
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(1.5)
            
            # Tenta vários seletores para encontrar o item da moeda
            selectors = [
                f"//ul[@id='{button_id.replace('button-', '')}']//a[contains(text(), '{currency_name}')]",
                f"//div[contains(@class, 'dropdown-menu')]//a[contains(text(), '{currency_name}')]",
                f"//a[contains(@class, 'dropdown-item')][contains(text(), '{currency_name}')]",
                f"//li//a[contains(text(), '{currency_name}')]"
            ]
            
            for selector in selectors:
                try:
                    items = self.driver.find_elements(By.XPATH, selector)
                    for item in items:
                        if item.is_displayed() and currency_name in item.text:
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item)
                            time.sleep(0.5)
                            self.driver.execute_script("arguments[0].click();", item)
                            self.logger.info(f"Moeda {currency_name} selecionada com sucesso")
                            return
                except:
                    continue
            
            raise ValueError(f"Não foi possível selecionar a moeda: {currency_name}")
            
        except Exception as e:
            self.logger.error(f"Erro ao selecionar moeda {currency_name}: {str(e)}")
            raise

    def _fill_date(self, target_date: date):
        """Preenche o campo de data"""
        try:
            date_str = target_date.strftime('%d/%m/%Y')
            
            # Tenta encontrar o campo de data
            selectors = [
                "//input[@placeholder='DD/MM/AAAA']",
                "//input[contains(@placeholder, 'DD')]",
                "//input[contains(@class, 'form-control') and @type='text']",
                "//input[@type='text' and contains(@class, 'date')]"
            ]
            
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            element.clear()
                            element.send_keys(date_str)
                            
                            # Também via JavaScript para garantir
                            self.driver.execute_script(
                                "arguments[0].value = arguments[1]; "
                                "arguments[0].dispatchEvent(new Event('change', { bubbles: true })); "
                                "arguments[0].dispatchEvent(new Event('blur', { bubbles: true }));", 
                                element, date_str
                            )
                            self.logger.info(f"Data {date_str} preenchida com sucesso")
                            return
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Erro ao preencher data: {str(e)}")

    def _click_convert_button(self):
        """Clica no botão de conversão"""
        try:
            selectors = [
                "//button[contains(text(), 'Converter')]",
                "//button[@class='btn btn-primary']",
                "//button[@type='submit']",
                "//button[contains(@class, 'btn-primary')]",
                "//input[@type='submit']"
            ]
            
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(0.5)
                            self.driver.execute_script("arguments[0].click();", element)
                            self.logger.info("Botão de conversão clicado com sucesso")
                            return
                except:
                    continue
            
            raise ValueError("Não foi possível encontrar o botão de conversão")
            
        except Exception as e:
            self.logger.error(f"Erro ao clicar no botão de conversão: {str(e)}")
            raise

    def _extract_result(self) -> Tuple[float, date]:
        """Extrai o resultado da conversão"""
        rate_value = 0.0
        actual_date = self.target_date
        
        try:
            # Aguarda um pouco para o resultado carregar
            time.sleep(2)
            
            # Captura todo o texto da página
            page_text = self.driver.page_source
            
            # Também tenta pegar de containers específicos
            result_containers = [
                "//div[contains(@class, 'card-body')]",
                "//div[contains(@class, 'result')]",
                "//div[contains(@class, 'conversao')]",
                "//*[contains(text(), 'Resultado')]/.."
            ]
            
            result_text = ""
            for container_xpath in result_containers:
                try:
                    container = self.driver.find_element(By.XPATH, container_xpath)
                    if container:
                        result_text += " " + container.text
                except:
                    continue
            
            # Se não achou nos containers, usa o texto da página inteira
            if not result_text:
                result_text = page_text
            
            self.logger.debug(f"Texto extraído para análise: {result_text[:500]}...")
            
            # Extração da taxa de câmbio usando regex
            patterns = [
                r'Resultado da conversão:?\s*([\d,.]+)',
                r'1\s+[\w/()]+\s+=\s+([\d,.]+)',
                r'Taxa:?\s*([\d,.]+)',
                r'Valor:?\s*([\d,.]+)',
                r'(\d+[,.]\d+)',  # Qualquer número decimal
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, result_text)
                for match in matches:
                    try:
                        # Converte formato brasileiro para float
                        if ',' in match and '.' in match:
                            # Formato: 1.234,56
                            value_str = match.replace(".", "").replace(",", ".")
                        elif ',' in match:
                            # Formato: 1234,56
                            value_str = match.replace(",", ".")
                        else:
                            # Formato: 1234.56
                            value_str = match
                        
                        potential_value = float(value_str)
                        
                        # Verifica se é um valor razoável para taxa de câmbio
                        if 0.01 <= potential_value <= 1000:
                            rate_value = potential_value
                            self.logger.info(f"Taxa extraída: {rate_value}")
                            break
                    except ValueError:
                        continue
                
                if rate_value > 0:
                    break
            
            # Extração da data
            date_patterns = [
                r'Data cotação utilizada:?\s*(\d{2}/\d{2}/\d{4})',
                r'Data:?\s*(\d{2}/\d{2}/\d{4})',
                r'(\d{2}/\d{2}/\d{4})'
            ]
            
            for date_pattern in date_patterns:
                date_matches = re.findall(date_pattern, result_text)
                for date_match in date_matches:
                    try:
                        actual_date = datetime.strptime(date_match, "%d/%m/%Y").date()
                        self.logger.info(f"Data extraída: {actual_date}")
                        break
                    except ValueError:
                        continue
                
                if actual_date != self.target_date:
                    break
                    
        except Exception as e:
            self.logger.error(f"Erro ao extrair resultado: {str(e)}")
        
        return rate_value, actual_date

    def closed(self, reason):
        """Chamado quando o spider é fechado"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Driver Selenium fechado")