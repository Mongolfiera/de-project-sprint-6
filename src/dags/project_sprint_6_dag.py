import boto3
import logging
import pendulum
import vertica_python
from airflow.decorators import dag, task
from airflow.models.variable import Variable


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 */1 * * *',  # Задаю расписание выполнения дага
    start_date=pendulum.datetime(2024, 5, 5, tz='UTC'),  # Дата начала выполнения дага.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project6', 's3', 'Vertica'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def project6_dag():

    @task(task_id="load_s3_files")
    def load_s3_files_task():
        logging.info ('Start load from S3')

        AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
        ENDPOINT_URL = Variable.get("s3_endpoint_url")
        file_name = 'group_log.csv'

        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        logging.info(f'Downloading {file_name}')

        s3_client.download_file(
            Bucket='sprint6',
            Key=file_name,
            Filename='/data/' + file_name
        )

    @task(task_id="load_to_vertica_stg")
    def load_to_vertica_stg_task():
        logging.info('Start load to Vertica STG')

        conn_info = {'host': Variable.get("VERTICA_HOST"),  # Адрес сервера
                     'port': '5433',  # Порт из инструкции,
                     'user': Variable.get("VERTICA_USER"),  # Полученный логин
                     'password': Variable.get("VERTICA_PASSWORD"),
                     'database': 'dwh',
                     'autocommit': True,
                     }

        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            user = Variable.get("VERTICA_USER").upper()

            cur.execute(f"truncate table {user}__STAGING.group_log")

            cur.execute(f"""COPY {user}__STAGING.group_log(
                        group_id, user_id, user_id_from, event, event_datetime
                        )
                        FROM LOCAL '/data/group_log.csv'
                        DELIMITER ','
                        SKIP 1
                        REJECTED DATA AS TABLE {user}__STAGING.group_log_rej""",
                        buffer_size=65536
                        )

            result = cur.fetchall()
            logging.info(f"Rows loaded to group_log: {str(result)}")

    load_s3_files = load_s3_files_task()
    load_to_vertica_stg = load_to_vertica_stg_task()

    load_s3_files >> load_to_vertica_stg


project6_dag = project6_dag()
