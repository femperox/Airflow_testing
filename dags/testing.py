from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.decorators import task

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

		 @task
		 def ret_one():
		 	return 1

		 @task
		 def get_one(one):
			 print(f'got {one}')

		 task0 = DummyOperator(task_id = 'start')

		 task11 = DummyOperator(task_id = 't11')
		 task12 = DummyOperator(task_id='t12')
		 task21 = DummyOperator(task_id='t21')
		 task22 = DummyOperator(task_id='t22')
		 task3 = DummyOperator(task_id='t3')


		 one = ret_one()


		 task0 >> [task12, task11]
		 task11 >> task21
		 task12 >> task22
		 [task21, task22] >> task3 >> get_one(one)