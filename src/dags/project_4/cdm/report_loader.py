from logging import Logger
from typing import List, Dict
from decimal import Decimal

from project_4 import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from lib.dict_util import str2json
from lib.dict_util import to_dict
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date
import pandas as pd
import numpy as np


class DeliveriesObj(BaseModel):
    order_id_dwh: int
    courier_id_dwh: int
    order_ts: datetime 
    rate: float
    tip_sum: float
    total_sum: float

class CourierObj(BaseModel):
    courier_id_dwh: int
    name: str 

class LineObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: int
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class LoadReportLine:
    def load_data(self, conn: Connection, line: LineObj) -> None:
        print(line)
        with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.couriers_pay_mart(  courier_id,
                                                            courier_name,
                                                            settlement_year,
                                                            settlement_month,
                                                            orders_count,
                                                            orders_total_sum,
                                                            rate_avg,
                                                            order_processing_fee,
                                                            courier_order_sum,
                                                            courier_tips_sum,
                                                            courier_reward_sum)
                        VALUES ( %(courier_id)s, %(courier_name)s,  %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s,
                                 %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)                      
                        ; 
                   """,
                    {
                        "courier_id": line['courier_id'],
                        "courier_name":line['courier_name'],
                        "settlement_year": line['settlement_year'],
                        "settlement_month": line['settlement_month'],
                        "orders_count": line['orders_count'],
                        "orders_total_sum": line['orders_total_sum'],
                        "rate_avg": line['rate_avg'],
                        "order_processing_fee": line['order_processing_fee'],
                        "courier_order_sum": line['courier_order_sum'],
                        "courier_tips_sum": line['courier_tips_sum'],
                        "courier_reward_sum": line['courier_reward_sum']
                    },
                )

class ProductSaleOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, rank_threshold: int, limit: int) -> List[DeliveriesObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveriesObj)) as cur:
            cur.execute(
                """
                    SELECT
                        order_id_dwh,
                        courier_id_dwh,
                        order_ts,
                        rate,
                        tip_sum,
                        total_sum
                    FROM dds.fct_deliveries 
                    WHERE order_id_dwh > %(threshold)s - 100  --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY order_id_dwh DESC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class ReportDestRepository:
    def combine_data(self, conn: Connection, delivery: DeliveriesObj) -> None:   
        #print(delivery) 
        ts = delivery.order_ts
        courier = self.get_courier(conn, delivery.courier_id_dwh)
        orders_total_sum = delivery.total_sum
        rate_avg = delivery.rate
        order_processing_fee = orders_total_sum * 0.25
        if rate_avg < 4:
            if orders_total_sum * 0.05 < 100:
                courier_order_sum = 100
            else:
                courier_order_sum = orders_total_sum * 0.05
        elif 4 <= rate_avg < 4.5:
            if orders_total_sum * 0.07 < 150:
                courier_order_sum = 150
            else:
                courier_order_sum = orders_total_sum * 0.07
        elif 4.5 <= rate_avg < 4.9:
            if orders_total_sum * 0.08 < 175:
                courier_order_sum = 175
            else:
                courier_order_sum = orders_total_sum * 0.08
        elif 4.9 <= rate_avg:
            if orders_total_sum * 0.1 < 200:
                courier_order_sum = 200
            else:
                courier_order_sum = orders_total_sum * 0.1
        df = pd.DataFrame([t.__dict__ for t in courier])
        df['settlement_year'] =  ts.strftime('%Y')
        df['settlement_month'] = ts.strftime('%m')
        df['orders_count'] = 1
        df['orders_total_sum'] = orders_total_sum
        df['rate_avg'] = rate_avg
        df['order_processing_fee'] = order_processing_fee
        df['courier_order_sum'] = courier_order_sum
        df['courier_tips_sum'] = delivery.tip_sum
        df['courier_reward_sum'] = courier_order_sum + delivery.tip_sum * 0.95
        print(df)
        return df

    def get_courier(self, conn: Connection, courier_id_dwh: str) -> List[CourierObj]:
        with conn.cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT
                        courier_id_dwh,
                        name
                    FROM dds.dm_couriers
                    WHERE courier_id_dwh = %(courier_id_dwh)s;
                """,
                {"courier_id_dwh": courier_id_dwh},
            )
            obj = cur.fetchmany()
        return obj

class ReportLoader:
    WF_KEY = "cdm_report_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000     # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductSaleOriginRepository(pg_origin)
        self.dds = ReportDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.into = LoadReportLine()
        self.log = log

    def load_report(self):
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
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} product_sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            df = pd.DataFrame()
            for delivery in load_queue:             
                res = self.dds.combine_data(conn, delivery)
                df = pd.concat([df, res])
            df = df.rename(columns={'courier_id_dwh': 'courier_id', 'name': 'courier_name'})
            df = df.groupby(['courier_id', 'courier_name', 'settlement_year', 'settlement_month'], as_index=False).aggregate({
                    'orders_count':np.sum,
                    'orders_total_sum':sum,
                    'rate_avg':np.average,
                    'order_processing_fee':sum,
                    'courier_order_sum':sum,
                    'courier_tips_sum':sum,
                    'courier_reward_sum':sum
                    })
            print(df)
            for idx, row in df.iterrows():
                self.into.load_data(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_id_dwh for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
