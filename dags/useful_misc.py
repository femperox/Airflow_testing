from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pprint import pprint

args = {
    'owner': 'me',
    'start_date': datetime(2022, 2, 21),
	'provide_context': True
}

with DAG( 'useful_misc',
          description = 'Полезные всякие штуки-дрюки',
          default_args = args,
          schedule_interval= '1 5 */1 * *',
          catchup = False
) as dag:

    def print_context(**kwargs):
        pprint(kwargs)

    # вывод контекста таска
    task_context = PythonOperator(
        task_id = 'task_context',
        python_callable= print_context,
        provide_context= True
    )

