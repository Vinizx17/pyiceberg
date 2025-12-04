# config_setup.py

import os
from typing import Dict

def configure_iceberg() -> Dict[str, str]:
    """
    Lê as variáveis de ambiente e configura o ambiente Python/PyArrow 
    para conexões S3 (MinIO) e Hive Metastore.

    Retorna:
        Dict[str, str]: Dicionário de propriedades do catálogo PyIceberg.
    """
    # Lê variáveis de ambiente
    s3_endpoint = os.getenv("PYICEBERG_CATALOG__HIVE__S3_ENDPOINT")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket = "raw"  # Ou torne isso uma variável de ambiente se o nome do bucket mudar
    hive_metastore_uri = "thrift://hive-metastore:9083"

    # AJUSTES CRÍTICOS para MinIO (PyArrow)
    if s3_endpoint:
        os.environ['AWS_ENDPOINT_URL'] = s3_endpoint
        os.environ['AWS_ALLOW_HTTP'] = 'true'
        os.environ['AWS_S3_ALLOW_UNSIGNERD'] = 'true'

    # Propriedades do Catálogo (Genéricas para Hive/S3)
    catalog_properties = {
        "uri": hive_metastore_uri,
        "warehouse": f"s3://{bucket}/iceberg_warehouse",
        "type": "hive",
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": s3_access_key,
        "s3.secret-access-key": s3_secret_key,
        "s3.allow-unauthenticated": "true",
        "s3.allow-http": "true"
    }

    return catalog_properties