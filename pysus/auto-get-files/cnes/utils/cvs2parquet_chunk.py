import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
import time


def csv2parquet(csv_file, path_parquet):
    ini = time.time()
    filename = os.path.basename(csv_file)
    path_parquet = path_parquet+filename.replace('csv', 'parquet.GZIP')
    chunksize = 100_000
    #pd.read_parquet('/home/falkor/monitor_rosa/files/parquet/sampa_2003.parquet')
    print("Inciando Leiturra: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))
    csv_stream = pd.read_csv(csv_file, chunksize=chunksize, low_memory=False, dtype=str)

    print("Fim leitura: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))

    print("Inicio convesao parquet: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))
    for i, chunk in enumerate(csv_stream):
        #print("Chunk", i)
        if i == 0:
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            parquet_writer = pq.ParquetWriter(path_parquet, parquet_schema, compression='gzip')
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)
    print("Fim convesao parquet: ",datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f"))
    parquet_writer.close()
    fim = time.time()
    print("Executado em: ",fim-ini)

csv_file = '/home/developer/csv-files/sampa_2003.csv'
parquet_file = '/home/developer/parquet-files'


csv2parquet(csv_file,parquet_file)