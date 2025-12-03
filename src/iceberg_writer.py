import uuid
from typing import Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io import load_file_io
from pyiceberg.catalog import load_catalog

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