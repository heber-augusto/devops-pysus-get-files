import warnings
from datetime import date
from ftplib import FTP
from typing import Dict, List, Tuple, Union
from custom_get_files.custom_get_files import CustomGetFtpFiles

group_dict: Dict[str, Tuple[str, int, int]] = {
        "PA": ("Produção Ambulatorial", 7, 1994),
        "BI": ("Boletim de Produção Ambulatorial individualizado", 1, 2008),
        "AD": ("APAC de Laudos Diversos", 1, 2008),
        "AM": ("APAC de Medicamentos", 1, 2008),
        "AN": ("APAC de Nefrologia", 1, 2008),
        "AQ": ("APAC de Quimioterapia", 1, 2008),
        "AR": ("APAC de Radioterapia", 1, 2008),
        "AB": ("APAC de Cirurgia Bariátrica", 1, 2008),
        "ACF": ("APAC de Confecção de Fístula", 1, 2008),
        "ATD": ("APAC de Tratamento Dialítico", 1, 2008),
        "AMP": ("APAC de Acompanhamento Multiprofissional", 1, 2008),
        "SAD": ("RAAS de Atenção Domiciliar", 1, 2008),
        "PS": ("RAAS Psicossocial", 1, 2008),
         }


class CustomSia(CustomGetFtpFiles):
    """
    Downloads SIA data from Datasus FTP server

    """
    def __init__(self):
        self.ftp_files = {} 
        self.current_year_limit = 2008
        self.ftp_path = "/dissemin/publicos/SIASUS/200801_/Dados"
        self.load_ftp_files()
        print(f'Encontrados {len(self.ftp_files)} arquivos Sia')

    def get_files_to_download(self,
                              state: str,
                              year: int,
                              month: int,
                              groups: Union[str, List[str]] = ["PA", "BI"],
                              ) -> list:
        """
        Return SIASUS ftp file paths for state year and month and returns as a list of dicts
        :param month: 1 to 12 (-1 defines all months from year)
        :param state: 2 letter state code
        :param year: 4 digit integer
        :param groups: 2-3 letter document code or a list of 2-3 letter codes,
            defaults to ['PA', 'BI']. Codes should be one of the following:
            PA - Produção Ambulatorial
            BI - Boletim de Produção Ambulatorial individualizado
            AD - APAC de Laudos Diversos
            AM - APAC de Medicamentos
            AN - APAC de Nefrologia
            AQ - APAC de Quimioterapia
            AR - APAC de Radioterapia
            AB - APAC de Cirurgia Bariátrica
            ACF - APAC de Confecção de Fístula
            ATD - APAC de Tratamento Dialítico
            AMP - APAC de Acompanhamento Multiprofissional
            SAD - RAAS de Atenção Domiciliar
            PS - RAAS Psicossocial
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
            months = [str(month).zfill(2) for month in range(1, 13)]
        else:
            months = [str(month).zfill(2), ]

        if isinstance(groups, str):
            groups = [groups, ]

        if year >= 1994 and year < 2008:
            file_prefix = "/dissemin/publicos/SIASUS/199407_200712/Dados"
            new_year_limit = 1992
        elif year >= 2008:
            file_prefix = "/dissemin/publicos/SIASUS/200801_/Dados"
            new_year_limit = 2008
        else:
            raise ValueError("SIA does not contain data before 1994")

        if self.current_year_limit != new_year_limit:
            self.current_year_limit = new_year_limit
            self.ftp_path = file_prefix
            self.load_ftp_files()
                            
        ftp.cwd(file_prefix)

        for gname in groups:
            gname = gname.upper()

            if gname not in group_dict:
                raise ValueError(f"SIA does not contain files named {gname}")

            ftp_paths_dict = {
                'type': f"SIA-{gname}",
                'ftp_paths': []
            }

            for month in months:
                try:
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
                    if fname not in self.ftp_files:
                        for l in ['a', 'b', 'c', 'd', 'e', 'f']:
                            nm, ext = fname.split('.')
                            file_name = f'{nm}{l}.{ext}'
                            if file_name in self.ftp_files:
                                files.append(self.ftp_files[file_name])
                    else:
                        files = [self.ftp_files[fname], ]

                    for fdict in files:
                        file_name = fdict['file_name']
                        ftp_path = f"{file_prefix}/{file_name}"
                        ftp_file_dict = {
                            'ftp_path': ftp_path,
                            'state': state,
                            'year': year,
                            'month': month,
                            'file_group': gname,
                            'file_date': fdict['file_date'],
                            'file_time': fdict['file_time'],
                            'file_size': fdict['file_size'],
                            'file_name': file_name,
                        }
                        ftp_paths_dict['ftp_paths'].append(ftp_file_dict)
                except:
                    continue
            list_of_ftp_paths.append(ftp_paths_dict)
        return list_of_ftp_paths
