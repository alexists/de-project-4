from logging import Logger
from typing import List, Dict

from project_4 import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from lib.dict_util import str2json
from lib.dict_util import to_dict
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import pandas as pd
from datetime import datetime

class DeliveriesLoader(BaseModel):
    id: int
    object_value: str 
    #update_ts: datetime

class DeliveryObj(BaseModel):
    delivery_id_dwh: int
    delivery_id_source: str 

class CouriersObj(BaseModel):
    courier_id_dwh: int
    courier_id_source: str 

class OrdersObj(BaseModel):
    order_id_dwh: int
    order_id_source: str

class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, rank_threshold: int) -> List[DeliveriesLoader]:
        with self._db.client().cursor(row_factory=class_row(DeliveriesLoader)) as cur:
            cur.execute(
                """
                    SELECT  id, object_value
                    FROM stg.deliveries
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
 
class DeliveriesDestRepository:
    def insert_deliveries(self, conn: Connection, deliveries: DeliveriesLoader) -> None:
        deliveries=str2json(deliveries.object_value)
        delivery_id_source = deliveries['delivery_id']
        couriers_id_source = deliveries['courier_id']
        orders_id_source = deliveries['order_id']
        order_ts = deliveries['order_ts']
        delivery_ts = deliveries['delivery_ts']
        address = deliveries['address']
        rate = deliveries['rate']
        tip_sum = deliveries['tip_sum']
        total_sum = deliveries['sum']
        print(delivery_id_source)
        res_delivery = self.get_delivery_id(conn, delivery_id_source)
        res_couriers = self.get_couriers_id(conn, couriers_id_source )
        res_orders = self.get_orders_id(conn, orders_id_source)
        print(res_orders)
        deliveries = {}
        deliveries['delivery_id_dwh'] = res_delivery.delivery_id_dwh
        deliveries['order_id_dwh'] = res_orders.order_id_dwh
        deliveries['courier_id_dwh'] = res_couriers.courier_id_dwh
        deliveries['order_ts'] = order_ts
        deliveries['delivery_ts'] = delivery_ts
        deliveries['address'] = address
        deliveries['rate'] = rate        
        deliveries['tip_sum'] = tip_sum
        deliveries['total_sum'] = total_sum
        print(deliveries)

        with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.fct_deliveries(order_id_dwh, delivery_id_dwh, courier_id_dwh, order_ts, delivery_ts, address, rate, tip_sum, total_sum)
                        VALUES (%(order_id_dwh)s, %(delivery_id_dwh)s, %(courier_id_dwh)s, %(order_ts)s, %(delivery_ts)s, %(address)s, %(rate)s, %(tip_sum)s, %(total_sum)s)
                        ON CONFLICT (order_id_dwh) DO UPDATE
                        SET
                            delivery_id_dwh = EXCLUDED.delivery_id_dwh,
                            courier_id_dwh = EXCLUDED.courier_id_dwh,
                            order_ts = EXCLUDED.order_ts,
                            delivery_ts = EXCLUDED.delivery_ts,
                            address = EXCLUDED.address,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum,
                            total_sum = EXCLUDED.total_sum;
                    """,
                    {
                        "order_id_dwh": deliveries['order_id_dwh'],
                        "delivery_id_dwh": deliveries['delivery_id_dwh'],
                        "courier_id_dwh": deliveries['courier_id_dwh'],
                        "order_ts": deliveries['order_ts'],
                        "delivery_ts": deliveries['delivery_ts'],
                        "address": deliveries['address'],
                        "rate": deliveries['rate'],
                        "tip_sum": deliveries['tip_sum'],
                        "total_sum": deliveries['total_sum']
                    },
                )

    def get_delivery_id(self, conn: Connection, delivery_id_source: str) -> List[DeliveryObj]:
        with conn.cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT
                        delivery_id_dwh,
                        delivery_id_source
                    FROM dds.dm_deliveries
                    WHERE delivery_id_source = %(delivery_id_source)s;
                """,
                {"delivery_id_source": delivery_id_source},
            )
            obj = cur.fetchone()
        return obj

    def get_couriers_id(self, conn: Connection, courier_id_source: str) -> List[CouriersObj]:
        with conn.cursor(row_factory=class_row(CouriersObj)) as cur:
            cur.execute(
                """
                    SELECT
                        courier_id_dwh,
                        courier_id_source
                    FROM dds.dm_couriers
                    WHERE courier_id_source = %(courier_id_source)s;
                """,
                {"courier_id_source": courier_id_source},
            )
            obj = cur.fetchone()
        return obj

    def get_orders_id(self, conn: Connection, order_id_source: str) -> List[OrdersObj]:
        with conn.cursor(row_factory=class_row(OrdersObj)) as cur:
            cur.execute(
                """
                    SELECT
                        order_id_dwh,
                        order_id_source
                    FROM dds.dm_orders
                    WHERE order_id_source = %(order_id_source)s;
                """,
                {"order_id_source": order_id_source},
            )
            obj = cur.fetchone()
        return obj

class FactDeliveriesLoader:
    WF_KEY = "fct_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 100     # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct_deliveries1(self):
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
            load_queue = self.origin.list_deliveries(last_loaded)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for order in load_queue:             
                self.dds.insert_deliveries(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
