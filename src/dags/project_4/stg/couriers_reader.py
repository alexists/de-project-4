from datetime import datetime
from typing import Dict, List
import requests
import json

class CouriersReader:   
    def get_couriers(self, base_url) -> List[Dict]:
        api_token = "25c27781-8fde-4b30-a22e-524044a7580f" #api_conn.password
        nickname = "ZHENYOK"
        cohort = "0"
        headers = {
                "X-API-KEY": api_token,
                "X-Nickname": nickname,
                "X-Cohort": cohort 
                }
        couriers_data = []       
        offset=0   
        while True:
            params = {
                'limit': '50', 
                'offset': offset,
                'sort_field': '_id',
                'sort_direction': 'asc'
            }                 
            result = requests.get(f'https://{base_url}/couriers/',
            headers = headers, params=params).json()
            if len(result) == 0:
                break
            offset += 50
            couriers_data.append(result)          
        return couriers_data