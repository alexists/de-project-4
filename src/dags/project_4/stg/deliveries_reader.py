import datetime
from typing import Dict, List
import requests
import json

class DeliveriesReader:   
    def get_couriers(self, base_url) -> List[Dict]:
        #bissness_date = {{'ds'}}
        api_token = "25c27781-8fde-4b30-a22e-524044a7580f"
        nickname = "ZHENYOK"
        cohort = "0"
        headers = {
                "X-API-KEY": api_token,
                "X-Nickname": nickname,
                "X-Cohort": cohort 
                }

        today_date = datetime.datetime.now() - datetime.timedelta(days=1)
        from_date = today_date.strftime("%Y-%m-%d") + " 00:00:00"
        
        #fr_date = today_date + datetime.timedelta(days=-7)
        to_date =  today_date.strftime("%Y-%m-%d") + " 23:59:59"
        print(from_date)
        print(to_date)
        couriers_data = []
        offset = 0
        while True:
            params = {
                    'from': str(from_date),
                    'to': str(to_date),
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