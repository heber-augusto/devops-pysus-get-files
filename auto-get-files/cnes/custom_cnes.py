from ftplib import FTP
from typing import List, Union

generic_path="/dissemin/publicos/CNES/200508_/Dados"
def get_files_to_download(
        state: str,
        year: int,
        month: int,
        groups: Union[str, List[str]] = ["DC", "EQ"],
) -> list:
    """
    Download CNES-SUS records for state year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    :param cache: whether to cache files locally. default is True
    :param groups: 2-3 letter document code or a list of 2-3 letter codes,
        defaults to ['DC', 'EQ']. Codes should be one of the following:
        DC - Dados Complementares OK
        EE - 
        EF - 
        EP - Equipes OK 
        EQ - Equipamentos OK
        GM - 
        HB - Habilitação OK
        IN - Incentivos OK
        LT - Leitos OK
        PF - Profissional OK
        RC - Regra Contratual OK
        SR - Serviço Especializado OK
        ST - Estabelecimentos OK

    :return: A tuple of dataframes with the documents in the order given
        by the , when they are found
    """
    state = state.upper()
    year2 = str(year)[-2:]
    month = str(month).zfill(2)
    # -1 defines all months from year
    if int(month) == -1:
        months = [str(month).zfill(2) for month in range(1,13)]
    else:
        months = [str(month).zfill(2),]

    if isinstance(groups, str):
        groups = [groups]
    
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    if year >= 1994 and year < 2008:
        ftp.cwd(generic_path)
    
    elif year >= 2008:
        ftp.cwd(generic_path)
    else:
        raise ValueError("CNES does not contain data before 1994")
    
    list_of_ftp_paths = []
    
    for gname in groups:
        gname = gname.upper()

        ftp_paths_dict = {
            'type':f"SIA-{gname}",
            'ftp_paths':[]
        }


        ftp.cwd(generic_path)

        ftp.cwd(gname)
        path = ftp.pwd()
        
        # flist = ftp.nlst()
        flist = []
        fdicts = {}
        ftp.dir("", flist.append)
        for file_info in flist:
            tmp = file_info.split()
            file_date = tmp[0]
            file_time = tmp[1]
            file_size = tmp[2]
            file_name = tmp[3]
            fdicts[file_name] = {
                'file_date': file_date,
                'file_time': file_time,
                'file_size': file_size,
                'file_name': file_name,
            }

        for month in months:
            fname = f"{gname}{state}{year2.zfill(2)}{month}.dbc"
            files = []
            if fname not in fdicts:
                for l in ['a', 'b', 'c', 'd', 'e', 'f']:
                    nm, ext = fname.split('.')
                    file_name = f'{nm}{l}.{ext}'
                    if file_name in flist:
                        files.append(fdicts[file_name])
            else:
                files = [fdicts[fname],]
            for fdict in files:
                file_name = fdict['file_name']
                ftp_path = f"{path}/{file_name}"
                ftp_file_dict = {
                    'ftp_path':ftp_path,
                    'state':state,
                    'year':year,
                    'month':month,
                    'file_group':gname,
                    'file_date' : fdict['file_date'],
                    'file_time' : fdict['file_time'],
                    'file_size' : fdict['file_size'],
                    'file_name' : file_name,
                }


                ftp_paths_dict['ftp_paths'].append(ftp_file_dict)
        list_of_ftp_paths.append(ftp_paths_dict)

    return list_of_ftp_paths