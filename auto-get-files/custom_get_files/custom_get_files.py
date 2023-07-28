from abc import abstractmethod


class CustomGetFiles:
    def __int__(self):
        pass

    @abstractmethod
    def get_files_to_download(self):
        pass
