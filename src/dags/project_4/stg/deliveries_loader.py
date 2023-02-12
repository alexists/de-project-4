from datetime import datetime
from logging import Logger
from functools import reduce 
from examples.stg import EtlSetting, StgEtlSettingsRepository
from project_4.stg.pg_deliveries_saver import PgDeliveriesSaver
from project_4.stg.deliveries_reader import DeliveriesReader
from lib import PgConnect
from lib.dict_util import json2str


class DeliveriesLoader:
    
    base_url = "d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

    def __init__(self, collection_loader: DeliveriesReader, pg_dest: PgConnect, pg_saver: PgDeliveriesSaver, logger: Logger) -> None:
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
            self.log.info(f"Found {reduce(lambda count, l: count + len(l), load_queue, 0)} documents to sync from Delivery collection.") 
            flat = [x for lst in load_queue for x in lst]
            for obj in flat:
                #columns = ','.join([i for i in m[0]])
                #values = [value for value in m]
                #print(columns)
                self.pg_saver.save_object(conn, obj)
                print(obj)