'''
https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz

'''

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

args = {
	'owner': 'me',
	'start_date': airflow.utils.dates.days_ago(3),
	'provide_context': True
}

with DAG( 'wikimedia_example',
          default_args = args,
          schedule_interval = '@hourly',
          catchup= False
        ) as dag:

        def get_data_python(execution_date):
            year, month, day, hour, *_ = execution_date.timetuple()
            print(year, month, day, hour)

        def fetch_pageviews(pagenames):
            result = dict.fromkeys(pagenames, 0)

            path = Variable.get("Airflow_path")
            with open(path+'/output/wikimedia'):
                print('ok')

        get_data_bash = BashOperator(
            task_id = 'get_data_by_Bash',
            bash_command = ( "mkdir -p /{{ '$AIRFLOW_HOME' }}/output/wikimedia && "  
                "curl -o /{{ '$AIRFLOW_HOME' }}/output/wikimedia/wikipageviews.gz "
                "https://dumps.wikimedia.org/other/pageviews/"
                "{{ execution_date.year }}/"
                "{{ execution_date.year }}-"
                "{{ '{:02}'.format(execution_date.month) }}/"
                "pageviews-{{ execution_date.year }}"
                "{{ '{:02}'.format(execution_date.month) }}"
                "{{ '{:02}'.format(execution_date.day) }}-"
                "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
            )
        )

        get_data_python = PythonOperator(
            task_id='get_data_by_python',
            python_callable = get_data_python,
            provide_context= True
        )

        extract_gz = BashOperator(
            task_id = 'extract_gz',
            bash_command= 'gunzip --force /$AIRFLOW_HOME/output/wikimedia/wikipageviews.gz'
        )

        fetch_pageviews = PythonOperator(
            task_id= 'fetch_pageviews',
            python_callable= fetch_pageviews,
            op_kwargs={
                "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
            }
        )

        get_data_bash >> get_data_python >> extract_gz >> fetch_pageviews