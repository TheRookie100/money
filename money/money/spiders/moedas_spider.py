"""Spider para extração de cotações do Banco Central do Brasil"""
# Executar com: scrapy runspider money\money\spiders\moedas_spider.py
import scrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    ElementClickInterceptedException, 
    TimeoutException, 
    NoSuchElementException,
    WebDriverException,
    StaleElementReferenceException
)
from datetime import date, datetime
import re
import time
import json
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from webdriver_manager.chrome import ChromeDriverManager
import requests
import sys
import os

# Adiciona o diretório raiz ao path para importações
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

try:
    from money.utils.logger import ProfessionalLogger, ProgressTracker
except ImportError:
    # Fallback para logging básico se o módulo não existir
    import logging
    
    class ProfessionalLogger:
        def __init__(self, name="Spider"):
            self.internal_logger = logging.getLogger(name)
            self.internal_logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
            handler.setFormatter(formatter)
            self.internal_logger.addHandler(handler)
        
        def info(self, msg, **kwargs): self.internal_logger.info(msg)
        def success(self, msg, **kwargs): self.internal_logger.info(f"✅ {msg}")
        def warning(self, msg, **kwargs): self.internal_logger.warning(f"⚠️  {msg}")
        def error(self, msg, **kwargs): self.internal_logger.error(f"❌ {msg}")
        def debug(self, msg, **kwargs): self.internal_logger.debug(msg)
        def start_operation(self, op, **kwargs): self.internal_logger.info(f"🚀 Iniciando: {op}")
        def finish_operation(self, op, duration, **kwargs): self.internal_logger.info(f"🏁 Finalizado: {op} ({duration:.1f}s)")
        def progress(self, current, total, op): self.internal_logger.info(f"📊 {op}: {current}/{total}")
    
    class ProgressTracker:
        def __init__(self, logger): self.logger = logger
        def start(self, id, name, total): self.logger.start_operation(name)
        def update(self, id, inc=1): pass
        def add_error(self, id): pass
        def add_warning(self, id): pass
        def finish(self, id): pass

@dataclass
class CurrencyPair:
    from_currency: str
    to_currency: str

@dataclass
class ExchangeRate:
    from_currency: str
    to_currency: str
    rate_value: float
    rate_date: date
    status: str
    execution_time: float = 0.0

    def to_dict(self) -> dict:
        return {
            "Moeda entrada": self.from_currency,
            "Moeda saída": self.to_currency,
            "Valor cotação": round(self.rate_value, 6) if self.rate_value else 0.0,
            "Data": self.rate_date.strftime("%d/%m/%Y"),
            "Status": self.status,
            "Tempo (s)": round(self.execution_time, 2)
        }

class MoedasSpider(scrapy.Spider):
    name = "moedas"
    start_urls = ["https://www.bcb.gov.br/conversao"]
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1,
        'LOG_LEVEL': 'ERROR',
        'TELNETCONSOLE_ENABLED': False,
        'DOWNLOAD_TIMEOUT': 30,
        'CONCURRENT_REQUESTS': 1,
    }

    def __init__(self):
        super().__init__()
        
        # Usa um nome diferente para evitar conflito com o logger do Scrapy
        self.custom_logger = ProfessionalLogger("CotacaoSpider")
        self.progress = ProgressTracker(self.custom_logger)
        
        self.input_dir = Path("money/assets/input")
        self.output_dir = Path("money/assets/output")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.currency_pairs = self._load_currency_pairs()
        self.target_date = date.today()
        self.results = []
        self.driver = None
        
        # Log inicial limpo
        self.custom_logger.info("Sistema de Cotações BCB v2.0")
        self.custom_logger.info(f"Data alvo: {self.target_date.strftime('%d/%m/%Y')}")
        self.custom_logger.info(f"Pares de moedas: {len(self.currency_pairs)}")

    def _load_currency_pairs(self):
        """Carrega pares de moedas do Excel"""
        excel_file = self.input_dir / "currencies.xlsx"
        
        if not excel_file.exists():
            self.custom_logger.warning("Arquivo currencies.xlsx não encontrado, usando configuração padrão")
            return [CurrencyPair("USD", "BRL"), CurrencyPair("EUR", "BRL")]
        
        try:
            df = pd.read_excel(excel_file)
            pairs = []
            
            cols_lower = [col.lower() for col in df.columns]
            from_col = None
            to_col = None
            
            for i, col_name in enumerate(df.columns):
                col_lower = cols_lower[i]
                if any(keyword in col_lower for keyword in ['from', 'origem', 'entrada']):
                    from_col = col_name
                elif any(keyword in col_lower for keyword in ['to', 'destino', 'saída', 'saida']):
                    to_col = col_name
            
            if from_col and to_col:
                for _, row in df.iterrows():
                    from_curr = str(row[from_col]).strip().upper()
                    to_curr = str(row[to_col]).strip().upper()
                    if len(from_curr) == 3 and len(to_curr) == 3:
                        pairs.append(CurrencyPair(from_curr, to_curr))
            
            self.custom_logger.success(f"Carregados {len(pairs)} pares de moedas do Excel")
            return pairs if pairs else [CurrencyPair("USD", "BRL"), CurrencyPair("EUR", "BRL")]
            
        except Exception as e:
            self.custom_logger.error("Erro ao ler arquivo Excel", exception=e)
            return [CurrencyPair("USD", "BRL"), CurrencyPair("EUR", "BRL")]

    def _get_rate_via_api(self, pair: CurrencyPair):
        """Método alternativo usando API do BCB"""
        try:
            today = self.target_date.strftime('%m-%d-%Y')
            
            currency_codes = {
                'USD': '1',
                'EUR': '978',
                'GBP': '826',
                'JPY': '392',
                'CAD': '124',
                'AUD': '36',
                'CHF': '756'
            }
            
            # API
            if pair.from_currency in currency_codes and pair.to_currency == 'BRL':
                url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)?@moeda='{pair.from_currency}'&@dataCotacao='{today}'&$top=1&$orderby=dataHoraCotacao%20desc&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
                
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('value') and len(data['value']) > 0:
                        cotacao = data['value'][0]
                        rate = float(cotacao.get('cotacaoVenda', 0))
                        if rate > 0:
                            self.custom_logger.debug(f"API BCB retornou cotação: {rate}")
                            return rate, self.target_date, "Sucesso via API BCB"
            
            return 0.0, self.target_date, "API não disponível para este par"
            
        except Exception as e:
            self.custom_logger.debug(f"Falha na API BCB: {str(e)}")
            return 0.0, self.target_date, f"Erro na API: {str(e)}"

    def _setup_driver(self):
        """Configura WebDriver Chrome otimizado"""
        if self.driver:
            return self.driver
            
        try:
            self.custom_logger.info("Configurando navegador...")
            
            options = Options()
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--enable-javascript')
            options.add_argument('--log-level=3')
            options.add_argument('--silent')
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_experimental_option('useAutomationExtension', False)
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            
            driver.set_page_load_timeout(45)
            driver.implicitly_wait(10)
            
            self.driver = driver
            self.custom_logger.success("Navegador configurado com sucesso")
            return driver
            
        except Exception as e:
            self.custom_logger.error("Erro ao configurar navegador", exception=e)
            raise

    def parse(self, response):
        """Processa todas as cotações"""
        operation_id = "cotacao_extraction"
        self.progress.start(operation_id, "Extração de Cotações", len(self.currency_pairs))
        
        for i, pair in enumerate(self.currency_pairs):
            self.custom_logger.info(f"Processando {pair.from_currency}/{pair.to_currency}")
            
            start_time = time.time()
            
            # Tenta web scraping primeiro
            rate_value, actual_date, status = self._get_exchange_rate_hybrid(pair)
            
            # Fallback para API se necessário
            if rate_value == 0.0 and "Erro" in status:
                self.custom_logger.info("Tentando via API BCB...")
                api_rate, api_date, api_status = self._get_rate_via_api(pair)
                if api_rate > 0:
                    rate_value, actual_date, status = api_rate, api_date, api_status
            
            execution_time = time.time() - start_time
            
            # Log do resultado
            if "sucesso" in status.lower():
                self.custom_logger.success(f"{pair.from_currency}/{pair.to_currency}: {rate_value:.6f} ({execution_time:.1f}s)")
            elif "erro" in status.lower():
                self.custom_logger.error(f"{pair.from_currency}/{pair.to_currency}: {status} ({execution_time:.1f}s)")
                self.progress.add_error(operation_id)
            else:
                self.custom_logger.warning(f"{pair.from_currency}/{pair.to_currency}: {status} ({execution_time:.1f}s)")
                self.progress.add_warning(operation_id)
            
            exchange_rate = ExchangeRate(
                from_currency=pair.from_currency,
                to_currency=pair.to_currency,
                rate_value=rate_value,
                rate_date=actual_date,
                status=status,
                execution_time=execution_time
            )
            
            self.results.append(exchange_rate)
            self.progress.update(operation_id)
            yield exchange_rate.to_dict()
        
        self.progress.finish(operation_id)
        self._save_results()

    def _get_exchange_rate_hybrid(self, pair: CurrencyPair):
        """Método híbrido com múltiplas estratégias"""
        strategies = [
            ("Formulário Web", self._strategy_form_submission),
            ("Navegação Direta", self._strategy_direct_navigation),
            ("JavaScript Injection", self._strategy_javascript_injection)
        ]
        
        for strategy_name, strategy_func in strategies:
            try:
                self.custom_logger.debug(f"Executando estratégia: {strategy_name}")
                result = strategy_func(pair)
                if result[0] > 0:
                    self.custom_logger.debug(f"Estratégia {strategy_name} bem-sucedida")
                    return result
            except Exception as e:
                self.custom_logger.debug(f"Estratégia {strategy_name} falhou: {str(e)[:100]}")
                continue
        
        return 0.0, self.target_date, "Erro: Todas as estratégias falharam"

    def _strategy_form_submission(self, pair: CurrencyPair):
        """Estratégia 1: Submissão de formulário padrão"""
        if not self.driver:
            self._setup_driver()
        
        try:
            self.driver.get("https://www.bcb.gov.br/conversao")
            
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            self._close_cookie_banner()
            
            date_str = self.target_date.strftime('%d/%m/%Y')
            
            js_script = f"""
            function waitForElement(selector, timeout = 5000) {{
                return new Promise((resolve, reject) => {{
                    const startTime = Date.now();
                    const checkElement = () => {{
                        const element = document.querySelector(selector);
                        if (element) {{
                            resolve(element);
                        }} else if (Date.now() - startTime > timeout) {{
                            reject(new Error('Timeout waiting for element: ' + selector));
                        }} else {{
                            setTimeout(checkElement, 100);
                        }}
                    }};
                    checkElement();
                }});
            }}
            
            function findElementByText(text) {{
                const xpath = `//a[contains(text(), '${{text}}')]`;
                const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                return result.singleNodeValue;
            }}
            
            async function submitForm() {{
                try {{
                    await waitForElement('#button-converter-de');
                    
                    const btnDe = document.getElementById('button-converter-de');
                    btnDe.click();
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                    let usdOption = findElementByText('{pair.from_currency}');
                    if (!usdOption) {{
                        usdOption = document.querySelector(`[data-value="{pair.from_currency}"]`);
                    }}
                    if (usdOption) usdOption.click();
                    
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                    const btnPara = document.getElementById('button-converter-para');
                    btnPara.click();
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                    let brlOption = findElementByText('{pair.to_currency}');
                    if (!brlOption) {{
                        brlOption = document.querySelector(`[data-value="{pair.to_currency}"]`);
                    }}
                    if (brlOption) brlOption.click();
                    
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                    const dateInputs = document.querySelectorAll('input[placeholder*="DD"], input[type="date"], input[name*="data"]');
                    for (let dateInput of dateInputs) {{
                        if (dateInput.offsetParent !== null) {{
                            dateInput.value = '{date_str}';
                            dateInput.dispatchEvent(new Event('change'));
                            break;
                        }}
                    }}
                    
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                    const convertBtns = document.querySelectorAll('button, input[type="submit"]');
                    for (let btn of convertBtns) {{
                        if (btn.textContent.includes('Converter') || btn.value.includes('Converter')) {{
                            btn.click();
                            break;
                        }}
                    }}
                    
                    if (!document.querySelector('button:contains("Converter")')) {{
                        const forms = document.querySelectorAll('form');
                        if (forms.length > 0) forms[0].submit();
                    }}
                    
                    return 'Form submitted successfully';
                }} catch (error) {{
                    return 'Error: ' + error.message;
                }}
            }}
            
            return submitForm();
            """
            
            result = self.driver.execute_script(js_script)
            self.custom_logger.debug(f"JavaScript execution result: {result}")
            
            time.sleep(10)
            return self._extract_result_advanced(pair)
            
        except Exception as e:
            raise Exception(f"Strategy 1 failed: {str(e)}")

    def _strategy_direct_navigation(self, pair: CurrencyPair):
        """Estratégia 2: Navegação direta"""
        try:
            date_param = self.target_date.strftime('%d/%m/%Y')
            url = f"https://www.bcb.gov.br/conversao?de={pair.from_currency}&para={pair.to_currency}&data={date_param}"
            
            if not self.driver:
                self._setup_driver()
            
            self.driver.get(url)
            time.sleep(5)
            
            return self._extract_result_advanced(pair)
            
        except Exception as e:
            raise Exception(f"Strategy 2 failed: {str(e)}")

    def _strategy_javascript_injection(self, pair: CurrencyPair):
        """Estratégia 3: JavaScript injection"""
        try:
            if not self.driver:
                self._setup_driver()
            
            self.driver.get("https://www.bcb.gov.br/conversao")
            time.sleep(3)
            
            js_code = f"""
            function simulateConversion() {{
                if (window.fetch) {{
                    const data = {{
                        from: '{pair.from_currency}',
                        to: '{pair.to_currency}',
                        date: '{self.target_date.strftime('%Y-%m-%d')}'
                    }};
                    
                    const possibleEndpoints = [
                        '/api/conversao',
                        '/conversao/api',
                        '/ptax/api'
                    ];
                    
                    for (let endpoint of possibleEndpoints) {{
                        try {{
                            fetch(endpoint, {{
                                method: 'POST',
                                headers: {{'Content-Type': 'application/json'}},
                                body: JSON.stringify(data)
                            }})
                            .then(response => response.json())
                            .then(data => {{
                                if (data.rate || data.valor || data.cotacao) {{
                                    const rate = data.rate || data.valor || data.cotacao;
                                    document.body.setAttribute('data-rate', rate);
                                }}
                            }});
                        }} catch (e) {{
                            console.log('Endpoint failed:', endpoint);
                        }}
                    }}
                }}
                
                return 'JavaScript injection completed';
            }}
            
            return simulateConversion();
            """
            
            self.driver.execute_script(js_code)
            time.sleep(5)
            
            try:
                rate_value = self.driver.execute_script("return document.body.getAttribute('data-rate');")
                if rate_value:
                    return float(rate_value), self.target_date, "Sucesso via JavaScript"
            except:
                pass
            
            return self._extract_result_advanced(pair)
            
        except Exception as e:
            raise Exception(f"Strategy 3 failed: {str(e)}")

    def _close_cookie_banner(self):
        """Fecha banner de cookies"""
        try:
            time.sleep(1)
            
            js_close = """
            const closeButtons = [
                ...document.querySelectorAll('button'),
                ...document.querySelectorAll('a'),
                ...document.querySelectorAll('[data-dismiss="modal"]')
            ].filter(el => {
                const text = el.textContent.toLowerCase();
                return text.includes('prosseguir') || 
                       text.includes('aceitar') || 
                       text.includes('ok') ||
                       el.classList.contains('close') ||
                       el.classList.contains('btn-primary');
            });
            
            closeButtons.forEach(btn => {
                if (btn.offsetParent !== null) {
                    btn.click();
                }
            });
            
            const overlays = document.querySelectorAll('.modal, .overlay, .popup');
            overlays.forEach(overlay => {
                overlay.style.display = 'none';
            });
            """
            
            self.driver.execute_script(js_close)
            time.sleep(1)
            
        except:
            pass

    def _extract_result_advanced(self, pair: CurrencyPair):
        """Extração avançada de resultados"""
        try:
            WebDriverWait(self.driver, 30).until(
                lambda driver: any([
                    "resultado" in driver.page_source.lower(),
                    "cotação" in driver.page_source.lower(),
                    "real" in driver.page_source.lower(),
                    "brl" in driver.page_source.lower(),
                    pair.from_currency.lower() in driver.page_source.lower()
                ])
            )
            
            page_source = self.driver.page_source
            
            patterns = [
                r'1\s*USD\s*=\s*R\$?\s*([0-9]+[.,][0-9]+)',
                r'1\s*Dólar.*?([0-9]+[.,][0-9]+).*?Real',
                r'USD.*?([0-9]+[.,][0-9]{4,6}).*?BRL',
                r'Resultado.*?([0-9]+[.,][0-9]+)',
                r'Valor.*?([0-9]+[.,][0-9]+)',
                r'Cotação.*?([0-9]+[.,][0-9]+)',
                r'([4-8][.,][0-9]{4,6})',
                r'([0-9]{1,2}[.,][0-9]{4,6})',
                r'content="([0-9]+[.,][0-9]+)"',
                r'value="([0-9]+[.,][0-9]+)"',
                r'data-rate="([0-9]+[.,][0-9]+)"'
            ]
            
            rate_value = 0.0
            
            for pattern in patterns:
                matches = re.findall(pattern, page_source, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    try:
                        clean_value = re.sub(r'[^\d.,]', '', match)
                        clean_value = clean_value.replace(',', '.')
                        
                        potential_value = float(clean_value)
                        
                        if 3.0 <= potential_value <= 10.0:
                            rate_value = potential_value
                            break
                        elif 0.1 <= potential_value <= 50.0:
                            rate_value = potential_value
                            
                    except (ValueError, TypeError):
                        continue
                
                if rate_value > 0:
                    break
            
            # Busca data
            actual_date = self.target_date
            date_patterns = [
                r'(\d{2}/\d{2}/\d{4})',
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{2}-\d{2}-\d{4})'
            ]
            
            for date_pattern in date_patterns:
                date_matches = re.findall(date_pattern, page_source)
                for date_str in date_matches:
                    try:
                        if '/' in date_str:
                            parsed_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                        elif '-' in date_str and len(date_str) == 10:
                            parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        else:
                            parsed_date = datetime.strptime(date_str, "%d-%m-%Y").date()
                        
                        if abs((parsed_date - self.target_date).days) <= 7:
                            actual_date = parsed_date
                            break
                    except:
                        continue
                if actual_date != self.target_date:
                    break
            
            if rate_value > 0:
                if actual_date != self.target_date:
                    status = "Sucesso com data diferente"
                else:
                    status = "Sucesso"
            else:
                status = "Valor não encontrado"
            
            return rate_value, actual_date, status
            
        except TimeoutException:
            return 0.0, self.target_date, "Erro: Timeout na extração"
        except Exception as e:
            return 0.0, self.target_date, f"Erro na extração: {str(e)}"

    def _save_results(self):
        """Salva resultados com relatórios profissionais"""
        if not self.results:
            self.custom_logger.error("Nenhum resultado para salvar")
            return
        
        self.custom_logger.info("Salvando resultados...")
        
        df = pd.DataFrame([result.to_dict() for result in self.results])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Salva arquivos
        files = {
            f"cotacoes_{timestamp}.xlsx": lambda: df.to_excel(self.output_dir / f"cotacoes_{timestamp}.xlsx", index=False),
            f"cotacoes_{timestamp}.csv": lambda: df.to_csv(self.output_dir / f"cotacoes_{timestamp}.csv", index=False, encoding='utf-8-sig'),
            "cotacoes_latest.json": lambda: df.to_json(self.output_dir / "cotacoes_latest.json", orient='records', indent=2, force_ascii=False)
        }
        
        saved_files = []
        for filename, save_func in files.items():
            try:
                save_func()
                saved_files.append(filename)
            except Exception as e:
                self.custom_logger.error(f"Erro ao salvar {filename}", exception=e)
        
        # Estatísticas finais
        success_count = sum(1 for r in self.results if "sucesso" in r.status.lower())
        error_count = sum(1 for r in self.results if "erro" in r.status.lower())
        warning_count = len(self.results) - success_count - error_count
        
        total_time = sum(r.execution_time for r in self.results)
        success_rate = (success_count / len(self.results)) * 100 if self.results else 0
        
        # Relatório final
        self.custom_logger.info("=== RELATÓRIO FINAL ===")
        self.custom_logger.info(f"Total processado: {len(self.results)} pares")
        self.custom_logger.success(f"Sucessos: {success_count} ({success_rate:.1f}%)")
        if warning_count > 0:
            self.custom_logger.warning(f"Avisos: {warning_count}")
        if error_count > 0:
            self.custom_logger.error(f"Erros: {error_count}")
        self.custom_logger.info(f"Tempo total: {total_time:.1f}s")
        self.custom_logger.info(f"Arquivos salvos: {len(saved_files)}")
        self.custom_logger.info(f"Local: {self.output_dir.absolute()}")
        
        # Cotações obtidas
        for result in self.results:
            if result.rate_value > 0:
                self.custom_logger.success(f"Cotação {result.from_currency}/{result.to_currency}: {result.rate_value:.6f}")

    def closed(self, reason):
        """Limpa recursos"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        self.custom_logger.info("Processo finalizado com sucesso")