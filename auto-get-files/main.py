import subprocess
import pandas as pd
import os
import csv
import sys
import json

from pathlib import Path

from datetime import datetime
from sia.custom_sia import CustomSia
from sih.custom_sih import CustomSih
from cnes.custom_cnes import CustomCnes
from sim.custom_sim import CustomSim
from ibge.custom_ibge import CustomIbge

from utils.utils import dbf_to_csv, csv_to_parquet, get_ibge_data, get_ibge_states_df
from utils.collect_parameters import *
from multiprocessing import Pool


home_dir = os.path.expanduser('~')
#home_dir = f"{home_dir}{r'/projetos/devops-pysus-get-files/'}"
#print(home_dir)

#state = os.environ['STATE']
#year = int(os.environ['YEAR'])
#month = int(os.environ['MONTH'])

remove_intermediate = (os.getenv('REM_INTERMEDIATE', 'True')) == 'True'
dbc_dir = os.getenv('DBC_DIR', os.path.join(home_dir,'dbc-files'))
dbf_dir = os.getenv('DBF_DIR', os.path.join(home_dir,'dbf-files'))
csv_dir = os.getenv('CSV_DIR', os.path.join(home_dir,'csv-files'))
output_dir = os.getenv('OUTPUT_DIR', os.path.join(home_dir,'output-files'))
ibge_output_dir = f'{output_dir}/ibge_data'
file_types = os.getenv('FILE_TYPE', 'PA').split(',')

#groups = os.environ['FILE_TYPE']


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
  'SIM':['DORES',],  
}

monthly_file_types = ['CNES','SIA','SIH']
yearly_file_types = ['SIM',]

# dicionario de funcoes por tipo
func_per_type = {
  'CNES': CustomCnes(),
  'SIA':CustomSia(),
  'SIH':CustomSih(),
  'SIM':CustomSim(),  
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

def collect_ibge_data():
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


def dbc_2_parquet_process(ftp_file_dict, pq_file_path):
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
        pq_file_dir = os.path.dirname(pq_file_path)
        completed_file_path = f'{pq_file_dir}/{nm}.done'

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
        print(f'creating folders for file {pq_file_path}')
        path = Path(pq_file_dir)
        path.mkdir(parents=True, exist_ok=True)

        step = 'creating parquet file'
        print(f'creating parquet file {pq_file_path}')
        # convert csv to parquet
        csv_to_parquet(csv_file_path, pq_file_path)
        if remove_intermediate == True:
            os.unlink(csv_file_path)

        step = 'saving json done file'
        f = open(completed_file_path, 'w+')
        f.write(json.dumps(ftp_file_dict)) 
    except:
        try:
            print(f'Erro during step {step} from file {ftp_file}')
        except:
            print(f'Erro during step {step}')
            pass


def get_sus_data_to_collect(file_type, state, year, month):
    files_to_collect = []
    file_groups = file_group_per_type[file_type]
    print(f'Listing files from {file_type} type')
    ftp_classes = func_per_type[file_type]
    ftp_files = ftp_classes.get_files_to_download(state, year, month, groups=file_groups)
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
                files_to_collect_dict = {}
                files_to_collect_dict['pq_file_path']  = pq_file_path
                files_to_collect_dict['ftp_file_dict'] = ftp_file_dict
                files_to_collect.append(files_to_collect_dict)
              
            except:
                try:
                    print(f'Erro during step {step} from file {ftp_file}')
                except:
                    print(f'Erro during step {step}')
                    pass
                continue
    return files_to_collect





# def collect_sus_data(state, year, month):

#     for file_type,file_groups in file_group_per_type.items():
#         print(f'Listing files from {file_type} type')
#         ftp_classes = func_per_type[file_type]
#         ftp_files = ftp_classes.get_files_to_download(state, year, month, groups=file_groups)
#         print(f'Got {len(ftp_files)} from {file_type}')
#         # print(ftp_files)
#         for file_group in ftp_files:
#             # print(f'Collecting files from {file_group}')
#             for ftp_file_dict in file_group['ftp_paths']:
#                 step = 'unknown'
#                 try:
#                     ftp_file = ftp_file_dict['ftp_path']
#                     print(f'checking file {ftp_file}')
#                     filename = os.path.basename(ftp_file)
#                     nm, ext = filename.split('.')
#                     dbf_file_path = f'{dbf_dir}/{nm}.dbf'
#                     dbc_file_path = f'{dbc_dir}/{nm}.dbc'
#                     csv_file_path = f'{csv_dir}/{nm}.csv'


#                     # <ESTADO>/<ANO>/<MES>/<TIPO>/<GRUPO>
#                     output_file_folder = f"""{ftp_file_dict['state']}/{ftp_file_dict['year']}/{ftp_file_dict['month']}/{file_type}/{ftp_file_dict['file_group']}"""
#                     pq_file_dir = f'{output_dir}/{output_file_folder}'
#                     completed_file_path = f'{pq_file_dir}/{nm}.done'
#                     pq_file_path = f'{pq_file_dir}/{nm}.parquet.gzip'

#                     # se arquivo ja existe e não tem modificacao, continua para o proximo
#                     if (os.path.exists(completed_file_path) == True) and \
#                     (check_file_already_processed(completed_file_path , ftp_file_dict) == True):
#                         print(f'file {nm} already exists')
#                         continue
#                     step = 'get files from FTP'
#                     # get files from FTP
#                     output_dbc = subprocess.check_output(['./collect_from_ftp.sh',ftp_file,dbc_dir])

#                     print('creating dbf file')
#                     step = 'convert dbc file to dbf'
#                     # convert dbc file to dbf
#                     output_dbf = subprocess.check_output(['./dbc-2-dbf',dbc_file_path,dbf_file_path])
#                     if remove_intermediate == True:
#                         os.unlink(dbc_file_path)

#                     print('creating csv file')
#                     step = 'convert dbf to csv'
#                     # convert dbf to csv
#                     dbf_to_csv(dbf_file_path, csv_file_path)
#                     if remove_intermediate == True:
#                         os.unlink(dbf_file_path)

#                     step = 'creating folders for file'
#                     print(f'creating folders for file {output_file_folder}')
#                     path = Path(pq_file_dir)
#                     path.mkdir(parents=True, exist_ok=True)

#                     step = 'creating parquet file'
#                     print(f'creating parquet file {pq_file_path}')
#                     # convert csv to parquet
#                     csv_to_parquet(csv_file_path, pq_file_path)
#                     if remove_intermediate == True:
#                         os.unlink(csv_file_path)

#                     step = 'saving json done file'
#                     print(ftp_file_dict)
#                     f = open(completed_file_path, 'w+')
#                     f.write(json.dumps(ftp_file_dict))                
#                 except:
#                     try:
#                         print(f'Erro during step {step} from file {ftp_file}')
#                     except:
#                         print(f'Erro during step {step}')
#                         pass
#                     continue



"""dbf_file_list = download_sia(state,year, month, cache=False, group= ['PA',])
for filepath in dbf_file_list:
    parquet_filepath = filepath.replace('.dbf', '.parquet.gzip')    
    dbf_2_parquet(filepath, parquet_filepath)"""


if __name__ == "__main__":
    inicio_processamento = datetime.now()

    collect_ibge_data()

    dbc_files_to_collect = []
    monthly_dates = print_parameters_monthly()
    for monthly_date in monthly_dates:
        for monthly_file_type in monthly_file_types:
            dbc_files_to_collect.extend(
                get_sus_data_to_collect(
                    file_type=monthly_file_type,
                    state = monthly_date['state'],
                    year = monthly_date['year'],
                    month = monthly_date['month'],                                        
                )
            )
        

    yearly_dates =  print_parameters_yearly()
    for yearly_date in yearly_dates:
        for yearly_file_type in yearly_file_types:
            dbc_files_to_collect.extend(
                get_sus_data_to_collect(
                    file_type=yearly_file_type,
                    state = yearly_date['state'],
                    year = yearly_date['year'],
                    month = yearly_date['month'],                                        
                )
            )


    to_download = [(file_to_collect_dict['ftp_file_dict'], file_to_collect_dict['pq_file_path']) for file_to_collect_dict in dbc_files_to_collect]
    # Tempo com pool de 10: 0:35:56.498708
    # Tempo com pool de 1: 

    with Pool(10) as pool:
        pool.starmap(dbc_2_parquet_process, to_download)
    
    #file_to_collect_dict = dbc_files_to_collect[0]
    #dbc_2_parquet_process(
    #    ftp_file_dict=file_to_collect_dict['ftp_file_dict'],
    #    pq_file_path=file_to_collect_dict['pq_file_path']       
    #)
    fim_processamento = datetime.now()
    print(f"Tempo de processamento {fim_processamento - inicio_processamento}")
