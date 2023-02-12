from datetime import datetime
from logging import Logger

from examples.stg import EtlSetting, StgEtlSettingsRepository
from project_4.stg.pg_couriers_saver import PgCouriersSaver
from project_4.stg.couriers_reader import CouriersReader
from lib import PgConnect
from lib.dict_util import json2str


class CouriersLoader:
    #_LOG_THRESHOLD = 2
    #_SESSION_LIMIT = 1000000
    WF_KEY = "delivery_system_couriers_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    
    base_url = "d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
    api_token = "25c27781-8fde-4b30-a22e-524044a7580f" #api_conn.password
    nickname = "ZHENYOK"
    cohort = "0"
    headers = {
        "X-API-KEY": api_token,
        "X-Nickname": nickname,
        "X-Cohort": cohort }


    def __init__(self, collection_loader: CouriersReader, pg_dest: PgConnect, pg_saver: PgCouriersSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            load_queue = self.collection_loader.get_couriers(self.base_url)
            print(load_queue)
            self.log.info(f"Found {len(load_queue)} documents to sync from Orders collection.") 
            flat = [x for lst in load_queue for x in lst]
            for obj in flat:
                #columns = ','.join([i for i in m[0]])
                #values = [value for value in m]
                #print(columns)
                self.pg_saver.save_object(conn, obj)
                print(obj)