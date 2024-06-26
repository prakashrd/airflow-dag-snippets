

import pendulum
from airflow import DAG
from airflow import XComArg
from airflow.decorators import task_group
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import sys


SOURCE = 'REFERENCE'
APPLICATION = 'REFERENCE'
SCHEMA = 'REFERENCE'

local_tz = pendulum.timezone("Australia/Perth")

dag_config = Variable.get("af_dag_variables", deserialize_json=True)
email_list= dag_config["variables_config"]["email_list"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 6, tzinfo=local_tz),
    'email': email_list,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    dag_id='srci_reference_refactor',
    default_args=default_args,
    schedule_interval=None,
    tags=['srci', 'to-be-deleted']
)


START_TASK = START_Task=DummyOperator(task_id='START',dag=dag)
END_TASK = START_Task=DummyOperator(task_id='END',dag=dag)


def one_two_three_traditional():
    # this adjustment is due to op_args expecting each argument as a list
    return [[1], [2], [3]]

def plus_10_traditional(x):
    return x + 10

one_two_three_task = PythonOperator(
    task_id="one_two_three_task",
    python_callable=one_two_three_traditional,
    dag=dag
)

plus_10_task = PythonOperator.partial(
    task_id="plus_10_task",
    python_callable=plus_10_traditional,
    dag=dag
).expand(op_args=one_two_three_task.output)

# when only using traditional operators, define dependencies explicitly
START_TASK >> one_two_three_task >> plus_10_task >> END_TASK
