# data_processor.py

import pandas as pd
import boto3
from io import BytesIO
from typing import List, Callable, Optional, Dict, Any

def read_raw_csv_from_s3(
    s3_client: boto3.client, 
    bucket: str, 
    prefix: str, 
    read_options: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Lê um ou múltiplos arquivos CSV de um prefixo no S3 e retorna um DataFrame consolidado.

    Args:
        s3_client (boto3.client): Cliente Boto3 S3 configurado.
        bucket (str): Nome do bucket.
        prefix (str): Caminho/prefixo dos arquivos CSV (ex: 'data/raw/').
        read_options (Dict): Opções adicionais para pd.read_csv (sep, encoding, etc.).

    Returns:
        pd.DataFrame: DataFrame consolidado.
    """
    read_options = read_options or {'sep': ';', 'encoding': 'utf-8', 'on_bad_lines': 'skip'}
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    dfs: List[pd.DataFrame] = []

    for obj in response.get("Contents", []):
        key = obj["Key"]
        file_bytes = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        
        df = pd.read_csv(BytesIO(file_bytes), **read_options)
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em s3://{bucket}/{prefix}")
        
    return pd.concat(dfs, ignore_index=True)


def apply_data_cleaning_and_typing(
    df: pd.DataFrame, 
    cleaning_func: Callable[[pd.DataFrame], pd.DataFrame]
) -> pd.DataFrame:
    """
    Aplica uma função de limpeza e conversão de tipos definida pelo usuário.

    Args:
        df (pd.DataFrame): O DataFrame a ser processado.
        cleaning_func (Callable): Função que recebe um DataFrame e retorna um DataFrame limpo.

    Returns:
        pd.DataFrame: DataFrame processado.
    """
    return cleaning_func(df)