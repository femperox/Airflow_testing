'''
Полчать информацию о товарах с https://goodsmileshop.com/en/
'''

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import XCom

from datetime import datetime
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

        plsql_hook = PostgresHook(postgres_conn_id='my_conn_plsql')
        conn = plsql_hook.get_conn()
        cursor = conn.cursor()

        for info in infos:
            sql = "INSERT INTO FIGURES(NAME, PRICE_JPY, IMG_LNK, PRODUCT_LNK, CREATED_AT)" \
                  f"VALUES ('{info['name'].replace(chr(39), chr(39)+chr(39))}', {info['price_jpy']}, '{info['img_link']}', '{info['product_lnk']}', '{datetime.now()}')" \
                  "ON CONFLICT (NAME) DO NOTHING;"
            cursor.execute(sql)
            conn.commit()

        conn.close()

    def branching(**context):

        plsql_hook = PostgresHook(postgres_conn_id='my_conn_plsql')
        conn = plsql_hook.get_conn()
        cursor = conn.cursor()

        task_instance = context['task_instance']

        sql = f"select count(name) from figures where created_at='{datetime.date(datetime.now())}'"
        cursor.execute(sql)
        result = (cursor.fetchall())[0][0]

        task_instance.xcom_push(key='branching_amount', value=result)

        if result > 0:
            return 'task_make_ok_email'
        else:
            return 'task_make_notok_email'


    def make_ok_email(**context):

        plsql_hook = PostgresHook(postgres_conn_id='my_conn_plsql')
        conn = plsql_hook.get_conn()
        cursor = conn.cursor()

        task_instance = context['task_instance']

        sql = f"select * from figures where created_at='{datetime.date(datetime.now())}'"
        cursor.execute(sql)
        result = cursor.fetchall()

        content = """
        <table width="AUTO" border="1" cellpadding="0" cellspacig="0">
        <tr><th>Товар</th><th>Цена в йенах</th><th>Изображение</th>
        """

        for row in result:
            content += '<tr>'
            content += '<td align="center">'+f'<span style="font-size: 13pt"><a href="{row[4]}">{row[1]}</a>'+'</span></td>'
            content += '<td align="center"><span style="font-size: 13pt">'+str(row[2])+'</span></td>'
            content += '<td align="center">'+f'<img src="{row[3]}" alt="{row[1]}" title="{row[1]}" width ="50%" height = "50%"/>'+'</td>'
            content += '</tr>'
        content += '</table>'


        amaount = task_instance.xcom_pull(key='branching_amount')

        content = f'<p><span style="font-size: 13pt">Привет, сегодня в подборке предзаказов {amaount} новых товаров: <span style="font-size: 13pt"></p><br>' + content + '</br>'

        task_instance.xcom_push(key='email', value=content)


    def make_notok_email(**context):

        task_instance = context['task_instance']

        content = '<p><span style="font-size: 13pt">Привет, сегодня в подборке нет новых товаров!<span style="font-size: 13pt"></p>'

        task_instance.xcom_push(key='email', value=content)


    task_get_info = PythonOperator(
        task_id = 'task_get_info',
        python_callable= get_info
    )


    task_branch = BranchPythonOperator(
        task_id='task_branch',
        python_callable= branching
    )

    task_make_ok_email = PythonOperator(
        task_id='task_make_ok_email',
        python_callable=make_ok_email
    )

    task_make_notok_email = PythonOperator(
        task_id='task_make_notok_email',
        python_callable=make_notok_email
    )

    task_send_email = EmailOperator(
        task_id='task_send_email',
        to = Variable.get('my_email'),
        subject = 'GSC-расслыка',
        html_content= "{{ task_instance.xcom_pull(key='email') }}",
        trigger_rule='none_failed'
    )

    task_get_info >> task_branch >> [task_make_ok_email, task_make_notok_email] >> task_send_email