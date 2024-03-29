import csv
import os
import time
from datetime import datetime
import requests

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dbfread import DBF

import requests
import urllib3
import ssl

class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)


def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session
URL_STATES_IBGE = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/11|12|13|14|15|16|17|21|22|23|24|25|26|27|28|29|31|32|33|35|41|42|43|50|51|52|53/municipios'

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
    csv_stream = pd.read_csv(csv_file, chunksize=chunksize, low_memory=False, dtype=str, keep_default_na=False)

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

def get_ibge_data(url=URL_STATES_IBGE):
    r = get_legacy_session().get(url)
    if r.ok:
        return r.json()
    else:
        return f'Error {r.status_code}: {r.reason}'

def get_ibge_states_df(json_file): # add try block to proper deal with typing
    df = pd.DataFrame(json_file)
    df["id_uf"] = df["microrregiao"].apply(lambda x: x["mesorregiao"]["UF"]["id"])
    df["nome_uf"] = df["microrregiao"].apply(lambda x: x["mesorregiao"]["UF"]["nome"])
    return df[['id', 'nome', 'id_uf', 'nome_uf']]
