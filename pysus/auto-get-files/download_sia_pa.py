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


"""if type(df_pa) == pd.DataFrame:
    print('tratando arquivo unico')
    try:
        print(df_pa.head())
        year_2dig = str(year)[-2:]
        parquet_filepath = f'/home/developer/pysus/PA{state}{year_2dig.zfill(2)}{str(month).zfill(2)}.parquet.gzip'
        print(parquet_filepath)
        df_pa.to_parquet(parquet_filepath,
                compression='gzip')



    except Exception as exc:
        raise Exception(f"df_pa.head() failed with the following error:\n {exc}")


else:
    print('tratando multiplos arquivos')

    mypath = r'/home/developer/pysus'
    f = []
    for (dirpath, dirnames, filenames) in os.walk(mypath):
        for filename in filenames:
            if filename.endswith(".dbf"):
                f.append(filename)
        break
    print(f)

    for filename in f:
        filepath = os.path.join(mypath, filename)
        print(filepath)
        
        try:
            df = read_dbc_dbf(filepath)
        except Exception as exc:
            raise Exception(f"read_dbc_dbf() failed with the following error:\n {exc}")
        parquet_filepath = filepath.replace('.dbf', '.parquet.gzip')
        print(parquet_filepath)
        df.to_parquet(parquet_filepath,
                compression='gzip') 
        
        os.unlink(filepath)

    #read_dbc_dbf(f[0])"""