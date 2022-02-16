from airflow import DAG

from airflow.operators.dummy import DummyOperator

from datetime import datetime

args = {
	'owner': 'me',
	'start_date': datetime(2022,2,15),
	'provide_context': True
}

with DAG('TESTING',
		 description = 'okkk',
		 schedule_interval = '0 4 * * 3',
		 catchup = False,
		 default_args = args
		 ) as dag:

		 task1 = DummyOperator(task_id = 'start')

		 task1