from .common.public_api import SeoulPublicDataAPI

class BusPassengerData(SeoulPublicDataAPI):
    """
    버스 승하차 인원 데이터를 가져오기 위한 클래스.
    """
    def __init__(self):

        super().__init__()
    
    def get_passenger_data(self, bus_stop_id, date):
        """
        특정 버스 정류장과 날짜의 승하차 인원 데이터를 URL 경로에 파라미터를 붙여 가져오는 메서드.
        
        Args:
            bus_stop_id (str): 버스 정류장 ID.
            date (str): 데이터를 조회할 날짜 (형식: YYYYMMDD).
        
        Returns:
            dict: 버스 승하차 인원 데이터.
        """
        endpoint = "/BUS_API_ENDPOINT"  
        return self.fetch_data(endpoint, bus_stop_id, date)
