from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgDeliveriesSaver:
    def save_object(self, conn: Connection, obj: Any):
        str_obj = json2str(obj)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveries(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts
                """,
                {
                    "object_id": obj["order_id"],
                    "object_value": str_obj,
                    "update_ts": obj["delivery_ts"]    
                }
            )
