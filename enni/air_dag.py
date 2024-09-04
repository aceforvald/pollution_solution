## base exercise at https://www.youtube.com/watch?v=IH1-0hwFZRQ

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime
import os
import configparser

import api
import cleansing
import postgres

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
config = configparser.ConfigParser()
config.read(AIRFLOW_HOME + '/airflow.cfg')
AIRFLOW_DAGS = config.get('core', 'dags_folder')

TARGET_FOLDER = CURR_DIR_PATH

with DAG("project_dag", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
            api_call = PythonOperator (
                    task_id="api_call",
                    python_callable=api._start_api
            )

            cleanse_data = PythonOperator (
                    task_id="cleanse_data",
                    python_callable=cleansing._start_cleansing
            )
            postgres_call = PythonOperator (
                    task_id="postgres_call",
                    python_callable=postgres._start_post
            )


            api_call >> cleanse_data >> postgres_call