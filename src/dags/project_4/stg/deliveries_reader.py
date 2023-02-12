import datetime
from typing import Dict, List
import requests
import json

bissness_date = '{{ ds }}'

class DeliveriesReader:   
    def get_couriers(self, base_url) -> List[Dict]:
        print(bissness_date)
        api_token = "25c27781-8fde-4b30-a22e-524044a7580f"
        nickname = "AlexeyMarkov"
        cohort = "8"
        headers = {
                "X-API-KEY": api_token,
                "X-Nickname": nickname,
                "X-Cohort": cohort 
                }
        to_date = bissness_date.strftime("%Y-%m-%d") + " 00:00:00"
        from_date = bissness_date.strftime("%Y-%m-%d") + " 23:59:59"
        couriers_data = []
        offset = 0
        while True:
            params = {
                    'from': from_date,
                    'to': to_date,
                    'limit': '50', 
                    'offset': offset,
                    'sort_field': 'asc',
                    'sort_direction': 'delivery_ts'
                    }
            result = requests.get(f'https://{base_url}/deliveries/',
            headers = headers, params=params).json()
            if len(result) == 0:
                break
            offset += 50
            couriers_data.append(result)      
        return couriers_data