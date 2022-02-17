'''
Получить информацию о грядущих запусках ракет.
Сохранить полученную инфу
По полученным данным найти фотографии ракет и сохранить их
Рассылка по полученным данным
'''

import json
import pathlib
from datetime import datetime

import requests
import requests.exceptions as requests_exceptions

#import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator


args = {
	'owner': 'me',
	'start_date': datetime(2022, 2, 17), #airflow.utils.dates.days_ago(14)
	'provide_context': True
}

with DAG('rocket_launches_example',
		 description = 'download info and pics, then notify',
		 schedule_interval = '@daily',
		 catchup = False,
		 default_args = args
		 ) as dag:

        def get_pics():

            path = Variable.get("Airflow_path")
            pathlib.Path(path+'output/rocket_images').mkdir(parents=True, exist_ok=True)

            with open(path+'output/launches.json') as f:
                launches = json.load(f)

                img_urls = [launch["image"] for launch in launches["results"]]

                for img in img_urls:
                    try:
                        response = requests.get(img)
                        img_filename = img.split("/")[-1]
                        print(img_filename)

                        target_file = path+f"output/rocket_images/{img_filename}"

                        with open(target_file, 'wb') as f:
                            f.write(response.content)
                        print(f'Downloaded {img} to {target_file}')
                    except requests_exceptions.MissingSchema:
                        print(f'{img} appears to be an invalid URL')
                    except requests_exceptions.ConnectionError:
                        print(f"Could not connect to {img}")



        task_download_launches = BashOperator(
            task_id = 'task_download_launches',
            bash_command = "curl -o /$AIRFLOW_HOME/output/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
            dag = dag
        )

        task_get_pics = PythonOperator(
            task_id= 'task_get_pics',
            python_callable = get_pics,
            dag = dag
        )

        task_email = EmailOperator(
            task_id = 'task_email',
            to = Variable.get('my_email'),
            subject = 'Notification',
            html_content = 'Check your pics!'
        )

        task_download_launches >> task_get_pics >> task_email