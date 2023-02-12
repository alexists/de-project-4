CREATE TABLE IF NOT EXISTS stg.couriers(
	id serial PRIMARY KEY,
	courier_id varchar(30),
	name varchar 
	CONSTRAINT stg_couriers_unique UNIQUE (courier_id)
);

CREATE TABLE IF NOT EXISTS stg.deliveries(
	id serial PRIMARY KEY,
	object_id varchar(30),
	object_value text NOT NULL,
	update_ts    TIMESTAMP NOT NULL,
	CONSTRAINT eliveries_unique UNIQUE (object_id)
);