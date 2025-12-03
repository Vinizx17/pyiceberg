# iceberg_table_manager.py

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from typing import Tuple, List

# As classes de tipos devem ser importadas no script principal ou em um módulo de tipos dedicado
# from pyiceberg.types import StringType, LongType, DoubleType, TimestampType, NestedField

def create_iceberg_schema(fields: List[Tuple]) -> Schema:
    """
    Cria um objeto Schema PyIceberg a partir de uma lista de campos (NestedField).

    Args:
        fields (List[Tuple]): Lista de objetos NestedField ou tuplas (ID, Nome, Tipo, Required).

    Returns:
        Schema: Objeto Schema PyIceberg.
    """
    # A correção crítica para o construtor da classe Schema está aqui
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