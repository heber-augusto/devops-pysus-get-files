import subprocess
import pandas as pd
import os
import csv
import sys
import json

from pathlib import Path

from datetime import datetime

from sia.custom_sia   import get_files_to_download as sia_get_files
from cnes.custom_cnes import get_files_to_download as cnes_get_files
from sih.custom_sih  import get_files_to_download as sih_get_files

from ibge.custom_ibge import CustomIbge

from utils.utils import dbf_to_csv, csv_to_parquet, get_ibge_data, get_ibge_states_df


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
ibge_output_dir = f'{output_dir}/ibge_data'
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

# dicionario de grupos de arquivo por tipo
file_group_per_type = {
  'CNES':['DC','EP','EQ','HB','IN','LT','PF','RC','SR','ST'],
  'SIA':['PA','AQ','AR','BI','AM','SAD', 'PS'],
  'SIH':['RD',],
}



# dicionario de funcoes por tipo
func_per_type = {
  'CNES':cnes_get_files,
  'SIA':sia_get_files,
  'SIH':sih_get_files,
}

def check_file_already_processed(
    completed_file_path,
    ftp_file_dict):
    """
    
    """
    f = open(completed_file_path, 'r')
    completed_file_dict = json.loads(f.read())
    return (completed_file_dict['file_size'] == ftp_file_dict['file_size']) and \
           (completed_file_dict['file_date'] == ftp_file_dict['file_date']) and \
           (completed_file_dict['file_time'] == ftp_file_dict['file_time'])


IBGE_FILE_PATH = f'{ibge_output_dir}/ibge_cidades.csv'
if not os.path.isfile(IBGE_FILE_PATH):
    path = Path(ibge_output_dir)
    path.mkdir(parents=True, exist_ok=True)
    get_ibge_states_df(get_ibge_data()).to_csv(
        IBGE_FILE_PATH, 
        sep=';', 
        encoding='utf-8',
        index=False)
    print('Loaded IBGE information cities')
else:
    print('IBGE information already loaded')


IBGE_POP_FILE_PATH = f'{ibge_output_dir}/ibge_pop_sexo_grupos_idade_municipios.csv'
if not os.path.isfile(IBGE_POP_FILE_PATH):
    path = Path(ibge_output_dir)
    path.mkdir(parents=True, exist_ok=True)

    custom_ibge = CustomIbge()
    custom_ibge.agregador = "200"

    metadata = custom_ibge.get_metadata_list()

    custom_ibge.get_files_to_download(ids=metadata).to_csv(
        IBGE_POP_FILE_PATH,
        sep=';',
        encoding='utf-8',
        index=False)
    print('Loaded IBGE population information')
else:
    print('IBGE information already loaded')


for file_type,file_groups in file_group_per_type.items():
    print(f'Listing files from {file_type} type')
    get_file = func_per_type[file_type]
    ftp_files = get_file(state, year, month, groups=file_groups)
    print(f'Got {len(ftp_files)} from {file_type}')
    # print(ftp_files)
    for file_group in ftp_files:
        # print(f'Collecting files from {file_group}')
        for ftp_file_dict in file_group['ftp_paths']:
            step = 'unknown'
            try:
                ftp_file = ftp_file_dict['ftp_path']
                print(f'checking file {ftp_file}')
                filename = os.path.basename(ftp_file)
                nm, ext = filename.split('.')
                dbf_file_path = f'{dbf_dir}/{nm}.dbf'
                dbc_file_path = f'{dbc_dir}/{nm}.dbc'
                csv_file_path = f'{csv_dir}/{nm}.csv'


                # <ESTADO>/<ANO>/<MES>/<TIPO>/<GRUPO>
                output_file_folder = f"""{ftp_file_dict['state']}/{ftp_file_dict['year']}/{ftp_file_dict['month']}/{file_type}/{ftp_file_dict['file_group']}"""
                pq_file_dir = f'{output_dir}/{output_file_folder}'
                completed_file_path = f'{pq_file_dir}/{nm}.done'
                pq_file_path = f'{pq_file_dir}/{nm}.parquet.gzip'

                # se arquivo ja existe e não tem modificacao, continua para o proximo
                if (os.path.exists(completed_file_path) == True) and \
                   (check_file_already_processed(completed_file_path , ftp_file_dict) == True):
                    print(f'file {nm} already exists')
                    continue
                step = 'get files from FTP'
                # get files from FTP
                output_dbc = subprocess.check_output(['./collect_from_ftp.sh',ftp_file,dbc_dir])

                print('creating dbf file')
                step = 'convert dbc file to dbf'
                # convert dbc file to dbf
                output_dbf = subprocess.check_output(['./dbc-2-dbf',dbc_file_path,dbf_file_path])
                if remove_intermediate == True:
                    os.unlink(dbc_file_path)

                print('creating csv file')
                step = 'convert dbf to csv'
                # convert dbf to csv
                dbf_to_csv(dbf_file_path, csv_file_path)
                if remove_intermediate == True:
                    os.unlink(dbf_file_path)

                step = 'creating folders for file'
                print(f'creating folders for file {output_file_folder}')
                path = Path(pq_file_dir)
                path.mkdir(parents=True, exist_ok=True)

                step = 'creating parquet file'
                print(f'creating parquet file {pq_file_path}')
                # convert csv to parquet
                csv_to_parquet(csv_file_path, pq_file_path)
                if remove_intermediate == True:
                    os.unlink(csv_file_path)

                step = 'saving json done file'
                print(ftp_file_dict)
                f = open(completed_file_path, 'w+')
                f.write(json.dumps(ftp_file_dict))                
            except:
                try:
                    print(f'Erro during step {step} from file {ftp_file}')
                except:
                    print(f'Erro during step {step}')
                    pass
                continue


fim_processamento = datetime.now()
print(f"Tempo de processamento {fim_processamento - inicio_processamento}")



"""dbf_file_list = download_sia(state,year, month, cache=False, group= ['PA',])
for filepath in dbf_file_list:
    parquet_filepath = filepath.replace('.dbf', '.parquet.gzip')    
    dbf_2_parquet(filepath, parquet_filepath)"""
