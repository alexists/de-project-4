from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgCouriersSaver:
    def save_object(self, conn: Connection, obj: Any):
        #str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.couriers(courier_id, name)
                    VALUES (%(courier_id)s, %(name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                    name = EXCLUDED.name                 
                """,
                {
                    "courier_id": obj["_id"],
                    "name": obj["name"]
                
                }
            )
