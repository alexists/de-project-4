import logging
import pendulum
from airflow.decorators import dag, task
from project_4.dds.deliveries_loader import DeliveriesLoader
from project_4.dds.couriers_loader import CouriersLoader
from project_4.dds.orders_loader import OrdersLoader
from project_4.dds.fct_deliveries_loader import FactDeliveriesLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)
default_args = {
    'retries': 2,
    'retry_delay': pendulum.duration(seconds=30)}

@dag(
    schedule_interval='10 8 * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 2, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project-4', 'dds', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False,  # Остановлен/запущен при появлении. Сразу запущен.
    default_args=default_args  
)

def project_4_dds_delivery_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveriesLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    deliveries_dict = load_deliveries()

    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CouriersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_dict = load_couriers()

    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrdersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    orders_dict = load_orders()

    @task(task_id="fct_deliveries_load")
    def load_fct_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = FactDeliveriesLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_fct_deliveries1()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    fct_deliveries_dict = load_fct_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    deliveries_dict >> couriers_dict >> orders_dict >> fct_deliveries_dict
# type: ignore


stg_bonus_system_ranks_dag = project_4_dds_delivery_system()
