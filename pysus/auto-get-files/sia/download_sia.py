# import pysus as ps

# print('bla')

from custom_sia import download as download_sia, dbf_2_parquet, get_files_to_download
import subprocess
import pandas as pd
import os

import csv
import sys
from datetime import datetime
from dbfread import DBF

import pyarrow.csv as pv
import pyarrow.parquet as pq


def dbf_to_csv(dbf_table_pth, output_path): 
    
    csv_fn = output_path
    table = DBF(dbf_table_pth, encoding="Latin-1")
    with open(csv_fn, 'w', newline = '',encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow(table.field_names)
        for record in table: 
            writer.writerow(list(record.values()))
    return csv_fn


def csv_to_parquet(csv_path, pq_path ):
    table = pv.read_csv(csv_path)
    pq.write_table(table, pq_path, compression='GZIP')





inicio_processamento = datetime.now()


home_dir = os.path.expanduser('~')
print(home_dir)

state = os.environ['STATE']
year = int(os.environ['YEAR'])
month = int(os.environ['MONTH'])
remove_intermediate = (os.getenv('REM_INTERMEDIATE', 'True')) == 'True'
dbc_dir = os.getenv('DBC_DIR', os.path.join(home_dir,'dbc-files'))
dbf_dir = os.getenv('DBF_DIR', os.path.join(home_dir,'dbf-files'))
csv_dir = os.getenv('CSV_DIR', os.path.join(home_dir,'csv-files'))
output_dir = os.getenv('OUTPUT_DIR', os.path.join(home_dir,'output-files'))
file_types = os.getenv('FILE_TYPE', 'PA').split(',')

#groups = os.environ['FILE_TYPE']


print(state)
print(year)
print(month)
print(dbc_dir)
print(dbf_dir)
print(csv_dir)
print(output_dir)

print('downloading')

ftp_files = get_files_to_download(state,year, month, groups=file_types)
print(ftp_files)

for file_group in ftp_files:
    for ftp_file in file_group['ftp_paths']:
        # get files from FTP
        output_dbc = subprocess.check_output(['./collect_from_ftp.sh',ftp_file,dbc_dir])
        filename = os.path.basename(ftp_file)
        nm, ext = filename.split('.')
        dbf_file_path = f'{dbf_dir}/{nm}.dbf'
        dbc_file_path = f'{dbc_dir}/{nm}.dbc'
        csv_file_path = f'{csv_dir}/{nm}.csv'
        pq_file_path = f'{output_dir}/{nm}.parquet.gzip'
        
        print('creating dbf file')
        # convert dbc file to dbf
        output_dbf = subprocess.check_output(['./dbc-2-dbf',dbc_file_path,dbf_file_path])
        if remove_intermediate == True:
            os.unlink(dbc_file_path)
        
        print('creating csv file')
        # convert dbf to csv
        dbf_to_csv(dbf_file_path, csv_file_path)
        if remove_intermediate == True:
            os.unlink(dbf_file_path)
        
        print('creating parquet file')        
        # convert csv to parquet
        csv_to_parquet(csv_file_path, pq_file_path)
        if remove_intermediate == True:
            os.unlink(csv_file_path)        

fim_processamento = datetime.now()
print(f"Tempo de processamento {fim_processamento - inicio_processamento}")



"""dbf_file_list = download_sia(state,year, month, cache=False, group= ['PA',])
for filepath in dbf_file_list:
    parquet_filepath = filepath.replace('.dbf', '.parquet.gzip')    
    dbf_2_parquet(filepath, parquet_filepath)"""