from abc import *

class PublicApiBase(metaclass=ABCMeta):
    @abstractmethod
    def fetch_data(self):
        pass
    
    