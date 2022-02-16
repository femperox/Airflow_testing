from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator

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

		 task2 = EmailOperator(task_id= 'email',
							   to = "ifilchukova1@gmail.com",
							   subject= 'testing',
							   html_content= 'hello')

		 task1 >> task2