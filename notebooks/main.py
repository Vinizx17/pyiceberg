import os
import uuid
from io import BytesIO
from typing import Dict, List, Callable, Optional, Any, Tuple

import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.types import StringType, LongType, DoubleType, TimestampType, NestedField
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.io import load_file_io


# --- MÓDULO: config_setup.py ---

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
        # Nota: Corrigindo o erro de digitação de UNSIGNERD para UNSIGNED, 
        # mas mantendo o original se for uma variável customizada do ambiente.
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


# --- MÓDULO: s3_iceberg_connections.py ---

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


# --- MÓDULO: iceberg_table_manager.py ---

def create_iceberg_schema(fields: List[NestedField]) -> Schema:
    """
    Cria um objeto Schema PyIceberg a partir de uma lista de campos (NestedField).

    Args:
        fields (List[NestedField]): Lista de objetos NestedField.

    Returns:
        Schema: Objeto Schema PyIceberg.
    """
    # A classe Schema aceita os campos como argumentos posicionais
    return Schema(*fields) 


def manage_table_lifecycle(
    catalog: load_catalog, 
    db_name: str, 
    table_name: str, 
    bucket: str, 
    schema: Schema
) -> load_catalog:
    """
    Cria o namespace, dropa a tabela antiga (se existir) e cria a nova tabela 
    Iceberg com o schema fornecido.

    Args:
        catalog: O objeto Catalog do PyIceberg.
        db_name (str): Nome do database (namespace).
        table_name (str): Nome da tabela.
        bucket (str): Nome do bucket S3.
        schema (Schema): Objeto Schema PyIceberg a ser usado na criação.

    Returns:
        load_catalog: O objeto da tabela PyIceberg criada/carregada.
    """
    table_identifier = (db_name, table_name)
    table_location = f"s3://{bucket}/{db_name}/{table_name}_table"

    # 1. Cria o Namespace
    try:
        catalog.create_namespace(db_name)
    except Exception:
        pass

    # 2. Deleta a tabela antiga (Para garantir aplicação do novo schema)
    try:
        catalog.drop_table(table_identifier)
        print(f"Tabela {table_identifier} antiga deletada.")
    except Exception:
        pass 

    # 3. Cria a tabela com o novo schema
    table = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        location=table_location
    )
    print(f"Tabela criada com o novo schema em: {table.location}")
    return table


# --- MÓDULO: data_processor.py ---

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


# --- MÓDULO: iceberg_writer.py ---

def write_dataframe_to_iceberg(
    table: load_catalog, 
    df_final: pd.DataFrame, 
    config: Dict[str, str]
) -> None:
    """
    Converte o DataFrame para Arrow Table e escreve no Iceberg usando uma transação.
    
    Args:
        table: O objeto da tabela PyIceberg carregado (já com o schema correto).
        df_final (pd.DataFrame): DataFrame com os dados a serem escritos.
        config (Dict): Dicionário de propriedades do catálogo para configurar o FileIO.
    """
    arrow_table = pa.Table.from_pandas(df_final, preserve_index=False)

    with table.transaction() as tx:
        # Obtém o FileIO configurado para o MinIO
        io = load_file_io(properties=config) 

        table_location_str = table.location() 
        file_path = f"{table_location_str}/data/{uuid.uuid4()}.parquet"

        # Escreve o arquivo Parquet
        output_file = io.new_output(file_path)
        with output_file.create() as outfile: 
            pq.write_table(
                table=arrow_table,
                where=outfile,
            )

        # Registra o arquivo na transação Iceberg
        tx.add_files([file_path])

    print("Dados escritos e commitados com sucesso no Iceberg!")


# --- FUNÇÃO DE TRANSFORMAÇÃO (Originalmente no Notebook) ---

def clean_and_cast_pedido_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa, renomeia e converte colunas de DataFrame para o tipo correto.
    """
    df = df.rename(columns={'Product ID': 'Product_ID'})
    
    cols_numericas = ["Total_Vendas", "Desconto", "Lucro"]
    for col in cols_numericas:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False).astype(float)

    df["Quantidade"] = (
        df["Quantidade"]
        .astype(str).str.replace(",", ".", regex=False)
        .astype(float).astype('int64')
    )
    
    # Corrigido para usar parênteses (PEP 8) em vez de barra invertida
    df["Data_Pedido"] = (
        pd.to_datetime(df["Data_Pedido"], format="%d-%m-%Y")
        .astype('datetime64[us]')
    )
    
    return df


# =============================================================================
# --- EXECUÇÃO PRINCIPAL (if __name__ == "__main__":) ---
# =============================================================================

if __name__ == "__main__":
    
    # 1. Configurações/Constantes (Corrigido PEP 8: 1 espaço em atribuição)
    RAW_BUCKET = "raw"
    ICEBERG_BUCKET = "pyiceberg"

    CSV_PREFIX = "dataset.csv"
    DB_NAME = "sales"
    TABLE_NAME = "pedidos"

    # 2. Definição do Schema (Corrigido PEP 8: Removido alinhamento vertical excessivo)
    pedido_fields = [
        NestedField(1, "ID_Pedido", StringType(), required=False),
        NestedField(2, "Data_Pedido", TimestampType(), required=False),
        NestedField(3, "ID_Cliente", StringType(), required=False),
        NestedField(4, "Segmento", StringType(), required=False),
        NestedField(5, "Regiao", StringType(), required=False),
        NestedField(6, "Pais", StringType(), required=False),
        NestedField(7, "Product_ID", StringType(), required=False),
        NestedField(8, "Categoria", StringType(), required=False),
        NestedField(9, "SubCategoria", StringType(), required=False),
        NestedField(10, "Total_Vendas", DoubleType(), required=False),
        NestedField(11, "Quantidade", LongType(), required=False),
        NestedField(12, "Desconto", DoubleType(), required=False),
        NestedField(13, "Lucro", DoubleType(), required=False),
        NestedField(14, "Prioridade", StringType(), required=False),
    ]

    # 3. Setup de Conexões e Catálogo
    catalog_properties = configure_iceberg()
    s3_client, catalog = setup_connections(catalog_properties)
    print("Conexões e ambiente configurados.")

    # 4. Criação/Gerenciamento da Tabela
    iceberg_schema = create_iceberg_schema(pedido_fields)

    table = manage_table_lifecycle(
        catalog,
        DB_NAME,
        TABLE_NAME,
        ICEBERG_BUCKET,
        iceberg_schema
    )
    
    # 5. Leitura e Processamento dos Dados
    df_raw = read_raw_csv_from_s3(s3_client, RAW_BUCKET, CSV_PREFIX)
    df_final = apply_data_cleaning_and_typing(df_raw, clean_and_cast_pedido_data)
    print(f"DataFrame processado. Linhas: {len(df_final)}")

    # 6. Escrita Final no Iceberg
    write_dataframe_to_iceberg(table, df_final, catalog_properties)