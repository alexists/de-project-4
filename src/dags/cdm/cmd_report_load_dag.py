import logging

import pendulum
from airflow.decorators import dag, task
from project_4.cdm.report_loader import ReportLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)
default_args = {
    'retries': 2,
    'retry_delay': pendulum.duration(seconds=30)}

@dag(
    schedule_interval='20 8 * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    #start_date=airflow.utils.dates.days_ago(7),
    start_date=pendulum.datetime(2023, 2, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project_4', 'cdm', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False,  # Остановлен/запущен при появлении. Сразу запущен.
    default_args=default_args   
)

def project_4_cdm_report_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="report_load")
    def load_report():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ReportLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_report()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    report_dict = load_report()

    

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    report_dict 


dds_cdm_report_dag = project_4_cdm_report_dag()
