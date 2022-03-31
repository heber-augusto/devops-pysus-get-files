from custom_cnes import get_files_to_download
from utils import utils
import subprocess
import os
from datetime import datetime

inicio_processamento = datetime.now()


state = os.environ['STATE']
year = int(os.environ['YEAR'])
month = int(os.environ['MONTH'])

dbc_dir = os.environ['DBC_DIR']
dbf_dir = os.environ['DBF_DIR']
csv_dir = os.environ['DBF_DIR']

#groups = os.environ['FILE_TYPE']


print(state)
print(year)
print(month)
print(dbc_dir)
print(dbf_dir)
print(csv_dir)

print('downloading')

ftp_files = get_files_to_download(state,year, month, group= ['DC','EP'])
print(ftp_files)

for ftp_file in ftp_files:
    # get files from FTP
    print("arquivo",ftp_file)
    print("DIR",dbc_dir)
    output_dbc = subprocess.check_output(['./collect_from_ftp.sh',ftp_file,dbc_dir])
    filename = os.path.basename(ftp_file)
    nm, ext = filename.split('.')
    dbf_file_path = f'{dbf_dir}/{nm}.dbf'
    dbc_file_path = f'{dbc_dir}/{nm}.dbc'
    csv_file_path = f'{csv_dir}/{nm}.csv'
    # convert dbc file to dbf
    output_dbf = subprocess.check_output(['./dbc-2-dbf',dbc_file_path,dbf_file_path])

    utils.dbf_to_csv(dbf_file_path, csv_file_path)

fim_processamento = datetime.now()
print(f"Tempo de processamento {fim_processamento - inicio_processamento}")