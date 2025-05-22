
import os
import time
import pandas as pd
import logging
from datetime import date, datetime
from dagster import asset, AssetExecutionContext
from typing import List, Dict, Any, Optional

# Importa as classes e utilitários do seu código
from .utils.selenium_utils import BCBAutomation
from .repository import save_to_duckdb
from .config import INPUT_DIR, OUTPUT_DIR, SCREENSHOTS_DIR, setup_logging

# Configurar logging
logger = logging.getLogger("currency_automation")

@asset(
    description="Lê os pares de moedas do arquivo de entrada",
    group_name="bcb_automation",
    compute_kind="pandas",
)
def read_currency_pairs(context: AssetExecutionContext) -> pd.DataFrame:
    """Asset que lê os pares de moedas do arquivo Excel"""
    context.log.info("Iniciando leitura de pares de moedas")
    
    automation = BCBAutomation(headless=True)
    currency_pairs = automation.read_currency_pairs()
    
    # Converte para dataframe para facilitar passagem entre assets
    df = pd.DataFrame([
        {"from_currency": pair.from_currency, "to_currency": pair.to_currency}
        for pair in currency_pairs
    ])
    
    context.log.info(f"Lidos {len(df)} pares de moedas")
    return df


@asset(
    description="Extrai cotações do Banco Central",
    group_name="bcb_automation",
    compute_kind="selenium",
    deps=["read_currency_pairs"],
)
def extract_exchange_rates(context: AssetExecutionContext, read_currency_pairs: pd.DataFrame) -> pd.DataFrame:
    """Asset que extrai as cotações do Banco Central"""
    context.log.info("INÍCIO - Processamento de cotações")
    
    # Inicializa a automação
    automation = BCBAutomation(headless=True)
    
    # Converte o dataframe para a estrutura esperada
    from dataclasses import dataclass
    
    @dataclass
    class CurrencyPair:
        from_currency: str
        to_currency: str
    
    currency_pairs = [
        CurrencyPair(
            from_currency=row["from_currency"], 
            to_currency=row["to_currency"]
        )
        for _, row in read_currency_pairs.iterrows()
    ]
    
    # Extrai as cotações
    try:
        exchange_rates = automation.extract_all_exchange_rates(currency_pairs)
        
        # Converte para dataframe para facilitar passagem entre assets
        results_df = pd.DataFrame([rate.to_dict() for rate in exchange_rates])
        
        context.log.info(f"Extração concluída: {len(results_df)} cotações processadas")
        return results_df
    
    finally:
        # Garante que o driver seja fechado
        if hasattr(automation, 'driver') and automation.driver:
            automation.close_driver()


@asset(
    description="Salva as cotações em formato Excel",
    group_name="bcb_automation",
    compute_kind="pandas",
    deps=["extract_exchange_rates"],
)
def save_exchange_rates(context: AssetExecutionContext, extract_exchange_rates: pd.DataFrame) -> str:
    """Asset que salva as cotações em um arquivo Excel"""
    context.log.info("Salvando cotações em Excel")
    
    if extract_exchange_rates.empty:
        context.log.warning("Nenhuma cotação para salvar")
        return "Nenhum arquivo gerado"
    
    # Salva o arquivo de saída
    automation = BCBAutomation(headless=True)
    
    # Usa a estrutura do dataframe diretamente para salvar
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"exchange_rates_{timestamp}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, filename)
    
    # Garante que o diretório existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Salva o dataframe
    extract_exchange_rates.to_excel(output_path, index=False)
    
    context.log.info(f"Arquivo Excel salvo em: {output_path}")
    return output_path


@asset(
    description="Analisa estatísticas das cotações",
    group_name="bcb_automation",
    compute_kind="pandas",
    deps=["extract_exchange_rates"],
)
def analyze_exchange_rates(context: AssetExecutionContext, extract_exchange_rates: pd.DataFrame) -> Dict[str, Any]:
    """Asset que analisa estatísticas das cotações extraídas"""
    context.log.info("Analisando estatísticas das cotações")
    
    if extract_exchange_rates.empty:
        return {"status": "Nenhum dado para análise"}
    
    # Calcula estatísticas básicas
    stats = {
        "total_cotacoes": len(extract_exchange_rates),
        "cotacoes_ok": len(extract_exchange_rates[extract_exchange_rates["Status"] == "Consulta ok"]),
        "cotacoes_data_diferente": len(extract_exchange_rates[extract_exchange_rates["Status"].str.contains("não é da data", na=False)]),
        "cotacoes_erro": len(extract_exchange_rates[extract_exchange_rates["Status"].str.contains("Erro", na=False)]),
        "moedas_unicas": len(extract_exchange_rates["Moeda entrada"].unique()),
        "data_execucao": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    }
    
    # Registra no log as estatísticas
    context.log.info(f"Total de cotações: {stats['total_cotacoes']}")
    context.log.info(f"Cotações OK: {stats['cotacoes_ok']}")
    context.log.info(f"Cotações com data diferente: {stats['cotacoes_data_diferente']}")
    context.log.info(f"Cotações com erro: {stats['cotacoes_erro']}")
    
    return stats

#asset