from abc import abstractmethod
from ftplib import FTP

class CustomGetFiles:
    def __int__(self):
        pass

    @abstractmethod
    def get_files_to_download(self):
        pass


class CustomGetFtpFiles(CustomGetFiles):
    def __init__(self):
        self.ftp_path = ""
        pass

    def load_ftp_files(self):
        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftp.cwd(self.ftp_path)
        flist = []
        ftp.dir("", flist.append)
        
        for file_info in flist:
            tmp = file_info.split()
            file_date = tmp[0]
            file_time = tmp[1]
            file_size = tmp[2]
            file_name = tmp[3]
            self.ftp_files[file_name] = {
                'file_date': file_date,
                'file_time': file_time,
                'file_size': file_size,
                'file_name': file_name,
            }        





