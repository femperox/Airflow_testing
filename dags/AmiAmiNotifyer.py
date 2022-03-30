'''
Полчать информацию о товарах с https://goodsmileshop.com/en/
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import json
from pprint import pprint
from bs4 import BeautifulSoup
import requests


args = { 'owner': 'me',
         'start_date': datetime(2022, 3, 23),
         'provide_context': True
}

with DAG ( 'AmiAmiNotifyer',
           description= 'Уведомление по почте о товарах из ежедневной подборки',
           schedule_interval= '@daily',
           catchup = False,
		   default_args = args
    ) as dag:

    def get_info():

        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
        url = 'https://goodsmileshop.com/en/'
        page = requests.get(url, headers)
        soup = BeautifulSoup(page.text, "html.parser")


        allFigures = soup.find('div', id='product-contentRight')

        allFigures = allFigures.find_all('div', class_="productGridItem pull-left goodsmile-global")

        infos = []
        for figure in allFigures:
            info = {}

            img = figure.find('img', class_="lazy")
            info['name'] = img.get('title')
            info['img_link'] = url[:-4]+img.get('data-original')

            price = figure.find('span', class_='product__price')
            info['price_jpy'] = float(price.contents[0].replace(',', '').replace('¥',''))

            product_url = figure.find('a')
            info['product_lnk'] = url[:-4]+product_url.get('href')
            infos.append(info)

        pprint(infos)
        #allFigures = allFigures.find_all('div', class_='product-area-1col')
        #pprint(allFigures)

        #plsql_hook = PostgresHook(postgres_conn_id='my_conn_plsql')
        #plsql_hook.get_conn()




    task_get_info = PythonOperator(
        task_id = 'task_get_info',
        python_callable= get_info
    )
    task_get_info