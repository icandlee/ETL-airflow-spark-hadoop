
import requests
import os 
from base_public_api import PublicApiBase 

class GoPublicDataAPI(PublicApiBase):
    """
    [공공데이터 포털] 공공데이터 API를 호출하기 위한 기본 클래스.
    """
    def __init__(self):
        self.base_url = os.getenv("GO_PUBLIC_API_URL")
        self.service_key = os.getenv("GO_PUBLIC_API_KEY")

    def fetch_data(self, endpoint:str, params:dict):
        params['serviceKey'] = self.service_key
        response = requests.get(f"{self.base_url}{endpoint}", params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
            
            
            
class SeoulPublicDataAPI(PublicApiBase):
    """
    [서울 열린데이터광장] 공공데이터 API를 호출하기 위한 기본 클래스.
    """
    def __init__(self):
        self.base_url = os.getenv("SEOUL_PUBLIC_API_URL")
        self.service_key = os.getenv("SEOUL_PUBLIC_API_KEY")

    def fetch_data(self, endpoint:str, *args):
        
        path_params = '/'.join(args)
        response = requests.get(f"{self.base_url}{endpoint}/{self.service_key}/{path_params}")
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
            
            
            
class VworldPublicDataAPI:
    """
    [vworld 브이월드] 공공데이터 API를 호출하기 위한 기본 클래스.
    """
    def __init__(self):
        self.base_url = os.getenv("VWORLD_PUBLIC_API_URL")
        self.service_key = os.getenv("VWORLD_PUBLIC_API_KEY")

    def fetch_data(self, endpoint:str, params:dict):
        params['key'] = self.service_key
        response = requests.get(f"{self.base_url}{endpoint}", params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()