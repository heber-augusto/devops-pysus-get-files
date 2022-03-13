# import pysus as ps

# print('bla')

from custom_SIA import download as download_sia, dbf_2_parquet
from pysus.utilities.readdbc import read_dbc_dbf

from dbfread import DBF
import pandas as pd
import os


state = os.environ['STATE']
year = int(os.environ['YEAR'])
month = int(os.environ['MONTH'])

print(state)
print(year)
print(month)

print('downloading')
dbf_file_list = download_sia(state,year, month, cache=False, group= ['PA',])
for filepath in dbf_file_list:
    parquet_filepath = filepath.replace('.dbf', '.parquet.gzip')    
    dbf_2_parquet(filepath, parquet_filepath)