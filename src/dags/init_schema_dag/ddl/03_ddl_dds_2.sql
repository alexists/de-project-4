CREATE TABLE IF NOT EXISTS dds.fct_deliveries(
    order_id_dwh integer PRIMARY KEY,
	delivery_id_dwh	integer,
	courier_id_dwh	integer,
	order_ts timestamp,
    delivery_ts timestamp,
	address varchar,
	rate  integer,
	tip_sum numeric (14, 2),
	total_sum numeric (14, 2),

    CONSTRAINT fct_deliveries_order_id_dwh_fkey
    FOREIGN KEY (order_id_dwh)
    REFERENCES dds.dm_orders(order_id_dwh),

    CONSTRAINT fct_deliveries_delivery_id_dwh_fkey
    FOREIGN KEY (delivery_id_dwh)
    REFERENCES dds.dm_deliveries(delivery_id_dwh),

    CONSTRAINT fct_deliveries_courier_id_dwh_fkey
    FOREIGN KEY (courier_id_dwh)
    REFERENCES dds.dm_couriers(courier_id_dwh)
);