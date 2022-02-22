import airflow.utils.dates
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator


args = {
    'owner': 'me',
    'start_date': airflow.utils.dates.days_ago(3)
}

with DAG( 'db_dag',
          description= 'postgreSQL db',
          schedule_interval= '@daily',
          catchup=False,
          default_args= args

) as dag:

    pg = PostgresOperator(
        task_id = 'pg_db',
        postgres_conn_id= 'my_conn_plsql',
        sql = 'Select 2+2;'
    )

    pg
