from airflow import DAG
import os
import glob

from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.models import Variable

args = {
	'owner': 'me',
	'start_date': datetime(2022,2,15),
	'provide_context': True
}

with DAG('push_to_airflowhome',
		 description = 'перенос новый файлов дага из рабочей директории в папку airflowHome',
		 schedule_interval = '0 0 * * *',
		 catchup = False,
		 default_args = args
		 ) as dag:

		def wait_for_new_file(**context):

			list_of_files = glob.glob(f'{Variable.get("path_for_learning")}/*')  # * means all if need specific format then *.csv
			latest_file = max(list_of_files, key=os.path.getctime)
			if latest_file:
				context['task_instance'].xcom_push(key = 'file_name', value=latest_file)
				return True
			else:
			    return False

		def print_new_file(**context):

			file = context['task_instance'].xcom_pull(key = 'file_name')
			print(file)


		wait = FileSensor(
			task_id='wait',
			filepath='{{ var.value.path_for_learning }}/ok.txt'
		)

		wait_new = PythonSensor(
			task_id='wait_new',
			python_callable=wait_for_new_file
		)

		print_new = PythonOperator(
			task_id='print_new',
			python_callable=print_new_file,
			provide_context= True
		)

		wait >> wait_new >> print_new