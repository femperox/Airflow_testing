'''
https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz

'''

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

args = {
	'owner': 'me',
	'start_date': airflow.utils.dates.days_ago(3),
	'provide_context': True
}

with DAG( 'wikimedia_example',
          default_args = args,
          schedule_interval = '@hourly'
        ) as dag:

        get_data = BashOperator(
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

        get_data