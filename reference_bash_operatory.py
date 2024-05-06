from framework_module.processes.adls_current_to_adls_stage import adls_current_to_adls_stage
from framework_module.processes.truncate_snowflake_raw import truncate_snowflake_raw
from framework_module.processes.adls_stage_to_snowflake_raw import adls_stage_to_snowflake_raw
from framework_module.processes.adls_stage_to_adls_processed import adls_stage_to_adls_processed
from framework_module.edp_kubernetes_operator import EDPDAGHelper, EDPDBTKubernetesPodOperator

import pendulum
from airflow import DAG
from airflow import XComArg
from airflow.decorators import task_group
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import XComArg

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
    dag_id='srci_reference_bash_operator',
    default_args=default_args,
    schedule_interval=None,
    tags=['srci', 'to-be-deleted']
)

edp_dag_helper = EDPDAGHelper(dag=dag)

START_TASK = edp_dag_helper.start_task()
END_TASK = edp_dag_helper.end_task()

# Creating the task to fetch the config passed when triggering the dag
def fetch_dag_config(**kwargs):
    # bash_commands is the key of the config that we passed while triggering the dag
    return kwargs["dag_run"].conf.get("bash_commands")

fetch_dag_config_task = PythonOperator(
    task_id="fetch_dag_config",
    python_callable=fetch_dag_config,
    dag=dag
)

# Write the below task just below the fetch_dag_config_task we created earlier
bash_operator_tasks = BashOperator.partial(
        task_id="sleep",
        depends_on_past=False,
        retries=3,
        do_xcom_push=False,
        dag=dag
).expand(bash_command=XComArg(fetch_dag_config_task))

START_TASK >> fetch_dag_config_task >> bash_operator_tasks >> END_TASK
