"""
Downloads SIM data from Datasus FTP server
"""

import warnings
from datetime import date
from ftplib import FTP
from typing import Dict, List, Tuple, Union
from custom_get_files.custom_get_files import CustomGetFiles



class CustomSim(CustomGetFiles):
    def __init__(self):
        self.group_dict: Dict[str, Tuple[str, str, int, int]] = {
            "DORES": ("DO", "Declarações de óbitos por residência", 1996),
        }   

    def get_files_to_download(
            self,
            state: str,
            year: int,
            month: int = 1,
            groups: Union[str, List[str]] = ["DORES", ],
        ) -> list:
        """
        Return SIASUS ftp file paths for state year and month and returns as a list of dicts
        :param month: 1 (not used - just maintain compatibility)
        :param state: 2 letter state code
        :param year: 4 digit integer
        :param groups: 2-3 letter document code or a list of 2-3 letter codes,
            defaults to ['DORES', ]. Codes should be one of the following:
            DORES - Declarações de óbitos por residência
        :return: A tuple of dataframes with the documents in the order given
            by the , when they are found
        """
        list_of_ftp_paths = []
        
        state = state.upper()
        first_year = 1996
        # -1 defines all years since 1996
        if year == -1:
            years = [str(year_value) for year_value in range(first_year,date.today().year)]
        else:
            years = [str(year),]
        
        month = '01'

        if isinstance(groups, str):
            groups = [groups,]

        if int(years[0]) >= 1996:
            file_prefix = "/dissemin/publicos/SIM/CID10/DORES"
        else:
            raise ValueError("SIM does not contain data before 1996")

        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftype = "DBC"
        ftp.cwd(file_prefix)
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

        for gname in groups:
            gname = gname.upper()
            
            if gname not in self.group_dict:
                raise ValueError(f"SIM does not contain files named {gname}")
            
            ftp_paths_dict = {
                'type':f"SIM-{gname}",
                'ftp_paths':[]
            }

            for year in years:
                try:
                    # Check available
                    input_date = date(int(year), 1, 1)
                    available_date = date(self.group_dict[gname][2], 1, 1)
                    if input_date < available_date:
                        warnings.warn(
                            f"SIM does not contain data for {gname} "
                            f"before {available_date:%d/%m/%Y}"
                        )
                        continue

                    fname = f"{self.group_dict[gname][0]}{state}{year}.dbc"
                    files = []
                    if fname not in fdicts:
                        for l in ['a', 'b', 'c', 'd', 'e', 'f']:
                            nm, ext = fname.split('.')
                            file_name = f'{nm}{l}.{ext}'
                            if file_name in fdicts:
                                files.append(fdicts[file_name])
                    else:
                        files = [fdicts[fname],]

                    for fdict in files:
                        file_name = fdict['file_name']
                        ftp_path = f"{file_prefix}/{file_name}"
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
                except:
                    continue
            list_of_ftp_paths.append(ftp_paths_dict)
        return list_of_ftp_paths


