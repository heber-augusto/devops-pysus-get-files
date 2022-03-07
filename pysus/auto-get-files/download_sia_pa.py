# import pysus as ps

# print('bla')

from custom_SIA import download as download_sia
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
df_pa = download_sia(state,year, month, cache=False, group= ['PA',])

if type(df_pa) == pd.DataFrame:
    print('tratando arquivo unico')
    try:
        print(df_pa.head())
    except Exception as exc:
        raise Exception(f"df_pa.head() failed with the following error:\n {exc}")


else:
    print('tratando multiplos arquivos')

    mypath = r'/home/developer/pysus'
    f = []
    for (dirpath, dirnames, filenames) in os.walk(mypath):
        f.extend(filenames)
        break
    print(f)

    filepath = os.path.join(mypath, f[0])
    print(filepath)

    try:
        df = read_dbc_dbf(filepath)
    except Exception as exc:
        raise Exception(f"read_dbc_dbf() failed with the following error:\n {exc}")

    os.unlink(filepath)

    #read_dbc_dbf(f[0])