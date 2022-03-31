"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
Modified on 18/04/21
by bcbernardo
license: GPL V3 or Later
"""

import os
import warnings
from datetime import date
from ftplib import FTP
from typing import Dict, List, Optional, Tuple, Union
from pprint import pprint

import pandas as pd
from dbfread import DBF



group_dict: Dict[str, Tuple[str, int, int]] = {
    "SP": ("TBD", 1, 1992),
    "RD": ("TBD", 1, 1992),
    "RJ": ("TBD", 1, 1992),
    "ER": ("TBD", 1, 2008),
}

def show_datatypes():
    pprint(group_dict)




def _fetch_file(fname, ftp, ftype):
    """
    Does the FTP fetching.
    :param fname: file name
    :param ftp: ftp connection object
    :param ftype: file type: DBF|DBC
    :return: pandas dataframe
    """
    multiples = False
    fnames = check_file_split(fname, ftp)

    multiples = len(fnames) > 1

    if multiples:
        return download_multiples(fnames, ftp)
    else:
        return [download_single_file(fname, ftp), ]

def download_single_file(fname, ftp):
    fnfull = os.path.join(CACHEPATH, fname)
    dbf_filepath = fnfull.replace('.dbc', '.dbf')    
    print(f"Downloading {fname}...")
    fobj = open(fnfull, "wb")
    try:
        ftp.retrbinary(f"RETR {fname}", fobj.write)
        fobj.close()
        dbc2dbf(fnfull, dbf_filepath)
        os.unlink(fnfull)
    except Exception as exc:
        raise Exception(f"Retrieval of file {fname} failed with the following error:\n {exc}")
    return dbf_filepath


def download_multiples(fnames, ftp):
    dbf_list = []
    for fn in fnames:
        dbf_list.append(download_single_file(fn, ftp))
    return dbf_list

def check_file_split(fname: str, ftp: FTP) -> list:
    """
    Check for split filenames. Sometimes when files are too large, they are split into multiple files ending in a, b, c, ...
    :param fname: filename
    :param ftp: ftp conection
    :return: list
    """
    files = []
    flist = ftp.nlst()
    if fname not in flist:
        for l in ['a', 'b', 'c', 'd', 'e', 'f']:
            nm, ext = fname.split('.')
            if f'{nm}{l}.{ext}' in flist:
                files.append(f'{nm}{l}.{ext}')

    return files

def dbf_2_parquet(dbf_filepath, parquet_filepath, compression='gzip'):
    try:
        df = read_dbc_dbf(dbf_filepath)
        df.to_parquet(
            parquet_filepath,
            compression=compression)
        os.unlink(dbf_filepath)       
    except Exception as exc:
        raise Exception(f"dbf_2_parquet() failed with the following error:\n {exc}")



def get_files_to_download(
        state: str,
        year: int,
        month: int,
        groups: Union[str, List[str]] = ["SP", "RD"],
) -> list:
    """
    Download SIHSUS records for state year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    :param cache: whether to cache files locally. default is True
    :param groups: 2-3 letter document code or a list of 2-3 letter codes,
        defaults to ['SP', 'RD']. Codes should be one of the following:
        RD - 
        ER - 
        RJ - 
        SP - 
    :return: A tuple of dataframes with the documents in the order given
        by the , when they are found
    """
    list_of_ftp_paths = []
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    
    state = state.upper()
    year2 = str(year)[-2:]
    # -1 defines all months from year
    if month == -1:
        months = [str(month).zfill(2) for month in range(1,13)]
    else:
        months = [str(month).zfill(2),]
    
 
    
    if year >= 1992 and year < 2008:
        ftp.cwd("/dissemin/publicos/SIHSUS/199201_200712/Dados")
        file_prefix = "/dissemin/publicos/SIHSUS/199201_200712/Dados"
    elif year >= 2008:
        ftp.cwd("/dissemin/publicos/SIHSUS/200801_/Dados")
        file_prefix = "/dissemin/publicos/SIHSUS/200801_/Dados"
    else:
        raise ValueError("SIH does not contain data before 1992")



    for gname in groups:
        gname = gname.upper()
        
        if gname not in group_dict:
            raise ValueError(f"SIH does not contain files named {gname}")
        
        ftp_paths_dict = {
            'type':f"SIH-{gname}",
            'ftp_paths':[]
        }

        for month in months:
            # Check available
            input_date = date(int(year), int(month), 1)
            available_date = date(group_dict[gname][2], group_dict[gname][1], 1)
            if input_date < available_date:
                # NOTE: raise Warning instead of ValueError for
                # backwards-compatibility with older behavior of returning
                # (PA, None) for calls after 1994 and before Jan, 2008
                warnings.warn(
                    f"SIA does not contain data for {gname} "
                    f"before {available_date:%d/%m/%Y}"
                )
                continue




            fname = f"{gname}{state}{year2.zfill(2)}{month}.dbc"
            files = []
            files = [fname,]
            for filename in files:
                ftp_path = f"{file_prefix}/{filename}"
                ftp_paths_dict['ftp_paths'].append(ftp_path)
        list_of_ftp_paths.append(ftp_paths_dict)
        
    return list_of_ftp_paths


