import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
import csv
from datetime import datetime
from dbfread import DBF


def dbf_to_csv(dbf_table_pth, output_path): 
    csv_fn = output_path
    table = DBF(dbf_table_pth, encoding="Latin-1")
    with open(csv_fn, 'w', newline = '',encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow(table.field_names)
        for record in table: 
            writer.writerow(list(record.values()))
    return csv_fn



def csv_to_parquet(csv_file, path_parquet):
    ini = time.time()
    filename = os.path.basename(csv_file)
    chunksize = 100_000
    # pd.read_parquet('/home/falkor/monitor_rosa/files/parquet/sampa_2003.parquet')
    # print("Inciando Leiturra: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))
    csv_stream = pd.read_csv(csv_file, chunksize=chunksize, low_memory=False, dtype=str)

    # print("Fim leitura: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))

    # print("Inicio convesao parquet: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))
    for i, chunk in enumerate(csv_stream):
        #print("Chunk", i)
        if i == 0:
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            parquet_writer = pq.ParquetWriter(path_parquet, parquet_schema, compression='gzip')
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)
    # print("Fim convesao parquet: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))
    parquet_writer.close()
    fim = time.time()
    # print("Executado em: ",fim-ini)
