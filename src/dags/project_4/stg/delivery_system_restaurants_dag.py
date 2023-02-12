import logging
import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from project_4.stg.pg_deliveries_saver import PgDeliveriesSaver
from project_4.stg.deliveries_loader import DeliveriesLoader
from project_4.stg.deliveries_reader import DeliveriesReader
from project_4.stg.pg_couriers_saver import PgCouriersSaver
from project_4.stg.couriers_loader import CouriersLoader
from project_4.stg.couriers_reader import CouriersReader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)
default_args = {
    'retries': 2,
    'retry_delay': pendulum.duration(seconds=30)}

@dag(
    schedule_interval='00 8 * * *',
    start_date=pendulum.datetime(2023, 2, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    #start_date=datetime.datetime.today() - datetime.timedelta(days=7),
    tags=['project_4', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False,  # Остановлен/запущен при появлении. Сразу запущен.
    default_args=default_args 
)

def project_4_stg_delivery_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgCouriersSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CouriersReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CouriersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    couriers_loader = load_couriers()
    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    couriers_loader # type: ignore

    @task()
    def load_deliveries():
            # Инициализируем класс, в котором реализована логика сохранения.
            pg_saver = PgDeliveriesSaver()
            
            # Инициализируем класс, реализующий чтение данных из источника.
            collection_reader = DeliveriesReader()

            # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
            loader = DeliveriesLoader(collection_reader, dwh_pg_connect, pg_saver, log)

            # Запускаем копирование данных.
            loader.run_copy()

    deliveries_loader = load_deliveries()
        # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    deliveries_loader # type: ignore

deliveries_stg_dag = project_4_stg_delivery_system()  # noqa
