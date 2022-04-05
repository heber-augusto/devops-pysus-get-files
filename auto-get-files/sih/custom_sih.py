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

group_dict: Dict[str, Tuple[str, int, int]] = {
    "SP": ("TBD", 1, 1992),
    "RD": ("TBD", 1, 1992),
    "RJ": ("TBD", 1, 1992),
    "ER": ("TBD", 1, 2008),
}

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
                ftp_file_dict = {
                    'ftp_path':ftp_path,
                    'state':state,
                    'year':year,
                    'month':month,
                    'file_group':gname
                }


                ftp_paths_dict['ftp_paths'].append(ftp_file_dict)
        list_of_ftp_paths.append(ftp_paths_dict)
        
    return list_of_ftp_paths


