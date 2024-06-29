import boto3
import logging
import pendulum
import vertica_python

from airflow.decorators import dag, task
from airflow.models.variable import Variable


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 */1 * * *',
    start_date=pendulum.datetime(2024, 6, 6, tz='UTC'),
    catchup=False,
    tags=['project6', 's3', 'Vertica'],
    is_paused_upon_creation=True
)
def project6_dag():

    @task(task_id='start')
    def start_task():
        logging.info('start')

    @task(task_id='load_s3_files')
    def load_s3_files_task():
        logging.info('Start load from S3')

        AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
        ENDPOINT_URL = Variable.get("s3_endpoint_url")

        files_to_load = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']

        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        for file_name in files_to_load:
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
        tables_to_load = {'users': ['id', 'chat_name', 'registration_dt', 'country', 'age'],
                          'groups': ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'],
                          'dialogs': ['message_id', 'message_dt', 'message_from', 'message_to', 'message', 'message_group'],
                          'group_log': ['group_id', 'user_id', 'user_id_from', 'event', 'event_datetime']
                          }

        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            user = Variable.get("VERTICA_USER").upper()

            for table in tables_to_load:
                logging.info(f'Downloading {table}')
                cur.execute(f"truncate table {user}__STAGING.{table}")
                fields = ', '.join(tables_to_load[table])

                cur.execute(f"""COPY {user}__STAGING.{table}(
                            {fields}
                            )
                            FROM LOCAL '/data/{table}.csv'
                            DELIMITER ','
                            SKIP 1
                            REJECTED DATA AS TABLE {user}__STAGING.{table}_rej""",
                            buffer_size=65536
                            )

                result = cur.fetchall()
                logging.info(f"Rows loaded to {table}: {str(result)}")

    @task(task_id='populate_dwh')
    def populate_dwh_task():
        logging.info('populate_DWH')
        user = Variable.get("VERTICA_USER").upper()

        conn_info = {'host': Variable.get("VERTICA_HOST"),  # Адрес сервера
                     'port': '5433',  # Порт из инструкции,
                     'user': Variable.get("VERTICA_USER"),  # Полученный логин
                     'password': Variable.get("VERTICA_PASSWORD"),
                     'database': 'dwh',
                     'autocommit': True,
                     }

        sql_hubs_query = f"""
            insert into {user}__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
            select distinct
                hash(id) as  hk_user_id,
                id as user_id,
                registration_dt,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.users
            where hash(id) not in (select hk_user_id from {user}__DWH.h_users);

            insert into {user}__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
            select distinct
                hash(id) as  hk_group_id,
                id as group_id,
                registration_dt,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.groups
            where hash(id) not in (select hk_group_id from {user}__DWH.h_groups);

            insert into {user}__DWH.h_dialogs(hk_message_id, message_id, message_dt, load_dt, load_src)
            select distinct
                hash(message_id) as hk_message_id,
                message_id,
                message_dt,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.dialogs
            where hash(message_id) not in (select hk_message_id from {user}__DWH.h_dialogs);
        """

        sql_links_query = f"""
            insert into {user}__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
            select distinct
                hash(hg.hk_group_id, hu.hk_user_id) as hk_l_admin_id,
                hg.hk_group_id as hk_group_id,
                hu.hk_user_id as hk_user_id,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.groups as g
            left join {user}__DWH.h_users as hu on g.admin_id = hu.user_id
            left join {user}__DWH.h_groups as hg on g.id = hg.group_id
            where hash(hg.hk_group_id, hu.hk_user_id) not in (select hk_l_admin_id from {user}__DWH.l_admins);

            insert into {user}__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
            select distinct
                hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
                hu.hk_user_id as hk_user_id,
                hg.hk_group_id as hk_group_id,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.group_log as gl
            left join {user}__DWH.h_users as hu on gl.user_id = hu.user_id
            left join {user}__DWH.h_groups as hg on gl.group_id = hg.group_id
            where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from {user}__DWH.l_user_group_activity);

            insert into {user}__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
            select distinct
                hash(hd.hk_message_id, hu.hk_user_id) as hk_l_user_message,
                hu.hk_user_id as hk_user_id,
                hd.hk_message_id as hk_message_id,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.dialogs as d
            left join {user}__DWH.h_users as hu on hu.user_id = d.message_from
            left join {user}__DWH.h_dialogs as hd on d.message_id = hd.message_id
            where hash(hd.hk_message_id, hu.hk_user_id) not in (select hk_l_user_message from {user}__DWH.l_user_message);

            insert into {user}__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
            select distinct
                hash(hg.hk_group_id, hd.hk_message_id) as hk_l_groups_dialogs,
                hd.hk_message_id as hk_message_id,
                hg.hk_group_id as hk_group_id,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.dialogs as d
            left join {user}__DWH.h_groups as hg on d.message_group = hg.group_id
            left join {user}__DWH.h_dialogs as hd on d.message_id = hd.message_id
            where hg.hk_group_id is not null
                and hash(hg.hk_group_id, hd.hk_message_id) not in (select hk_l_groups_dialogs from {user}__DWH.l_groups_dialogs);
        """

        sql_satellites_query = f"""
            insert into {user}__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
            select distinct
                hu.hk_user_id as hk_user_id,
                su.country as country,
                su.age as age,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.users as su
            left join {user}__DWH.h_users as hu on su.id = hu.user_id
            where hu.hk_user_id not in (select distinct hk_user_id from {user}__DWH.s_user_socdem);

            insert into {user}__DWH.s_user_chat_info(hk_user_id, chat_name, load_dt, load_src)
            select distinct
                hu.hk_user_id as hk_user_id,
                su.chat_name as chat_name,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.users as su
            left join {user}__DWH.h_users as hu on su.id = hu.user_id
            where hu.hk_user_id not in (select distinct hk_user_id from {user}__DWH.s_user_chat_info);

            insert into {user}__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
            select distinct
                hg.hk_group_id as hk_group_id,
                sg.group_name,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.groups as sg
            left join {user}__DWH.h_groups as hg on sg.id = hg.group_id
            where hg.hk_group_id not in (select distinct hk_group_id from {user}__DWH.s_group_name);

            insert into {user}__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
            select distinct
                hg.hk_group_id as hk_group_id,
                sg.is_private,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.groups as sg
            left join {user}__DWH.h_groups as hg on sg.id = hg.group_id
            where hg.hk_group_id not in (select distinct hk_group_id from {user}__DWH.s_group_private_status);

            insert into {user}__DWH.s_admins(hk_l_admin_id, is_admin, admin_from, load_dt, load_src)
            select distinct
                la.hk_l_admin_id as hk_l_admin_id,
                True as is_admin,
                hg.registration_dt as admin_from,
                now() as load_dt,
                's3' as load_src
            from {user}__DWH.l_admins as la
            left join {user}__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id
            where la.hk_l_admin_id not in (select distinct hk_l_admin_id from {user}__DWH.s_admins);

            insert into {user}__DWH.s_dialog_info(hk_message_id, message_from, message_to, message, load_dt, load_src)
            select distinct
                hd.hk_message_id as hk_message_id,
                d.message_from as message_from,
                d.message_to as message_to,
                d.message message,
                now() as load_dt,
                's3' as load_src
            from {user}__DWH.h_dialogs as hd
            left join {user}__STAGING.dialogs as d on hd.message_id = d.message_id;

            insert into {user}__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
            select distinct
                lug.hk_l_user_group_activity as hk_l_user_group_activity,
                gl.user_id_from as user_id_from,
                gl.event as event,
                gl.event_datetime as event_dt,
                now() as load_dt,
                's3' as load_src
            from {user}__STAGING.group_log as gl
            left join {user}__DWH.h_users as hu on gl.user_id = hu.user_id
            left join {user}__DWH.h_groups as hg on gl.group_id = hg.group_id
            left join {user}__DWH.l_user_group_activity as lug on (hg.hk_group_id = lug.hk_group_id) and (hu.hk_user_id = lug.hk_user_id)
            where lug.hk_l_user_group_activity not in (select distinct hk_l_user_group_activity from {user}__DWH.s_auth_history)
                and gl.event = 'add' or gl.event = 'leave';
        """

        queries = {'hubs': sql_hubs_query,
                   'links': sql_links_query,
                   'satellites': sql_satellites_query,
                   }

        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()

            for query in queries:
                logging.info(f'Populating DWH {query}')
                cur.execute(queries[query])
                result = cur.fetchall()
                logging.info(f"Populating {query} - {len(result)} ended")

    @task(task_id='finish')
    def end_task():
        logging.info('finish')

    start = start_task()
    load_s3_files = load_s3_files_task()
    load_to_vertica_stg = load_to_vertica_stg_task()
    populate_dwh = populate_dwh_task()
    finish = end_task()

    start >> load_s3_files >> load_to_vertica_stg >> populate_dwh >> finish


project6_dag = project6_dag()
