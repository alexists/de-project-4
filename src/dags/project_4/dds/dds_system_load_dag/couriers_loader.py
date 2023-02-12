from logging import Logger
from typing import List, Dict

from examples import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from lib.dict_util import str2json
from lib.dict_util import to_dict
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import pandas as pd
from datetime import datetime


class CouriersObj(BaseModel):
    id: int
    courier_id: str
    name: str


class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, rank_threshold: int) -> List[CouriersObj]:
        with self._db.client().cursor(row_factory=class_row(CouriersObj)) as cur:
            cur.execute(
                """
                    SELECT  id, courier_id, name
                    FROM stg.couriers
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
           
                """, {
                    "threshold": rank_threshold,
                    #"limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class CouriersDestRepository:
    def insert_couriers(self, conn: Connection, couriers: CouriersObj) -> None:
        #deliveries=str2json(deliveries.object_value)
        print(couriers.name)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id_source, name)
                    VALUES (%(courier_id_source)s, %(name)s)
                    ON CONFLICT (courier_id_source) DO NOTHING
                
                """,
                {
                    "courier_id_source": couriers.courier_id,
                    "name": couriers.name
                },
            )


class CouriersLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository(pg_origin)
        self.dds = CouriersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_couriers(last_loaded)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for courier in load_queue:              
                self.dds.insert_couriers(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
