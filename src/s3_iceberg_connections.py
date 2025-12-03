from typing import Dict, Tuple

import boto3
from pyiceberg.catalog import load_catalog

def setup_connections(config: Dict[str, str]) -> Tuple[boto3.client, load_catalog]:
    """
    Cria a conexão boto3 (S3 client) e inicializa o catálogo Iceberg/Hive.

    Args:
        config (Dict): Dicionário de propriedades do catálogo.

    Returns:
        Tuple[boto3.client, load_catalog]: Cliente S3 do Boto3 e o objeto Catalog PyIceberg.
    """
    s3_endpoint = config.get("s3.endpoint")
    s3_access_key = config.get("s3.access-key-id")
    s3_secret_key = config.get("s3.secret-access-key")

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
    )

    catalog = load_catalog("hive_minio", **config)
    
    return s3_client, catalog