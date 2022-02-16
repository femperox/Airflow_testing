from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os
import requests
import pandas as pd
from datetime import datetime

args = {
	'owner': 'me',
	'start_date': datetime(2022,2,16),
	'provide_context': True
}

FILENAME = os.path.join('//c/Users/User/AirflowHome/output', 'titanic.csv')

def download_dataset():
	url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
	response = requests.get(url, stream=True)
	response.raise_for_status()
	with open(FILENAME, 'w', encoding='utf-8') as f:
		for chunk in response.iter_lines():
			f.write('{}\\n'.format(chunk.decode('utf-8')))

def make_dataset():
	titanic_df = pd.read_csv(FILENAME)
	pvt = titanic_df.pivot_table(
		index=['Sex'], columns=['Pclass'], values='Name', aggfunc='count'
	)
	df = pvt.reset_index()
	df.to_csv(os.path.join(os.path.join('//c/Users/User/AirflowHome/output', 'titanic_pivot.csv')))

with DAG('titanic_example',
		 schedule_interval = '0 4 * * 3',
		 catchup = False,
		 default_args = args
		 ) as dag:

		task1 = PythonOperator( task_id= 'download_dataset',
								python_callable= download_dataset
		)

		task2 = PythonOperator( task_id= 'make_dataset',
								python_callable= make_dataset
		)

		task1 >> task2