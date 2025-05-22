"""
Funções para persistência de dados (opcional)
Se quiser implementar salvamento em banco de dados
"""

import os
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger("currency_automation")

def save_to_duckdb(data: List[Dict[str, Any]], table_name: str) -> bool:
    """
    Salva dados em DuckDB (opcional)
    
    Args:
        data: Lista de dicionários com os dados
        table_name: Nome da tabela para salvar
        
    Returns:
        bool: True se salvou com sucesso, False caso contrário
    """
    try:
        import duckdb
        
        # Converte para DataFrame
        df = pd.DataFrame(data)
        
        # Conecta ao DuckDB
        db_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data.duckdb")
        
        # Cria ou atualiza a tabela
        with duckdb.connect(db_file) as conn:
            # Registra o DataFrame
            conn.register("df_view", df)
            
            # Verifica se a tabela existe
            table_exists = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchone()[0]
            
            if table_exists:
                # Apenda os dados à tabela existente
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_view")
            else:
                # Cria a tabela
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_view")
                
        logger.info(f"Dados salvos com sucesso na tabela {table_name}")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao salvar dados em DuckDB: {str(e)}")
        return False


def save_to_csv(data: List[Dict[str, Any]], file_path: str) -> str:
    """
    Salva dados em CSV
    
    Args:
        data: Lista de dicionários com os dados
        file_path: Caminho para o arquivo CSV
        
    Returns:
        str: Caminho do arquivo salvo
    """
    try:
        # Converte para DataFrame
        df = pd.DataFrame(data)
        
        # Salva em CSV
        df.to_csv(file_path, index=False)
        
        logger.info(f"Dados salvos com sucesso em {file_path}")
        return file_path
    
    except Exception as e:
        logger.error(f"Erro ao salvar dados em CSV: {str(e)}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt_file_path = file_path.replace(".csv", f"_error_{timestamp}.csv")
        try:
            df.to_csv(alt_file_path, index=False)
            logger.info(f"Dados salvos em caminho alternativo: {alt_file_path}")
            return alt_file_path
        except:
            return ""