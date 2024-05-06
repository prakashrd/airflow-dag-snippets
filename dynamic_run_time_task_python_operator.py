from framework_module.processes.adls_current_to_adls_stage import adls_current_to_adls_stage
from framework_module.processes.truncate_snowflake_raw import truncate_snowflake_raw
from framework_module.processes.adls_stage_to_snowflake_raw import adls_stage_to_snowflake_raw
from framework_module.processes.adls_stage_to_adls_processed import adls_stage_to_adls_processed
from framework_module.edp_kubernetes_operator import EDPDAGHelper, EDPDBTKubernetesPodOperator

from airflow.operators.dummy import DummyOperator
import pendulum
from airflow import DAG
from airflow import XComArg
from airflow.decorators import task_group
from airflow.operators.python_operator import PythonOperator

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

def my_python_operator_function(*args, **kwargs):
    logging.info("args are : {0}".format(args))
    logging.info("kwargs are : {0}".format(kwargs))
    kwargs["SOURCE_OBJECT"] = args[0].upper()
    logging.info("source object is : {0}".format(kwargs["SOURCE_OBJECT"]))
    logging.info("process is : {0}".format(kwargs["PROCESS"]))

    if kwargs["PROCESS"] == "ADLS_CURRENT_TO_STAGE":
        return adls_current_to_adls_stage(**kwargs)
    elif kwargs["PROCESS"] == "TRUNCATE_SNOWFLAKE_RAW":
        return truncate_snowflake_raw(**kwargs)
    elif kwargs["PROCESS"] == "ADLS_TO_SNOWFLAKE_RAW":
        return adls_stage_to_snowflake_raw(**kwargs)
    elif kwargs["PROCESS"] == "ADLS_STAGE_TO_PROCESSED":
        return adls_stage_to_adls_processed(**kwargs)
    else:
        logging.error("Invalid process name")
        sys.exit(1)

def get_source_objects(**kwargs):
    logging.info("kwargs are : {0}".format(kwargs))
    # config at run time to be provided like : {"source_objects":["KVP_LIST","KVP_CAMPAIGNS"]}
    inputs =  kwargs["dag_run"].conf.get("source_objects")
    
    input_list = [[s] for s in inputs]
    logging.info("input_list is : {0}".format(input_list))
    return input_list

dag = DAG(
    dag_id='srci_reference_adhoc_test',
    default_args=default_args,
    schedule_interval=None,
    tags=['srci', 'to-be-deleted']
)

edp_dag_helper = EDPDAGHelper(dag=dag)

START_Task = edp_dag_helper.start_task()
END_Task = edp_dag_helper.end_task()

GET_SOURCE_OBJECTS_Task = PythonOperator(
    task_id="get_source_objects_task_id",
    python_callable=get_source_objects,
    dag=dag
)

ADLS_CURRENT_TO_STAGE_Task = PythonOperator.partial(
    task_id='adls_current_to_stage',
    python_callable=my_python_operator_function,
    op_kwargs={
        "APPLICATION": APPLICATION,
        "SCHEMA": SCHEMA,
        "PROCESS": "ADLS_CURRENT_TO_STAGE",
        'EXECUTION_DATE': "{{ execution_date.in_timezone('Australia/Perth').to_date_string() }}",
        'RUN_ID': '{{ run_id }}',
        'DAG_ID': '{{ dag.dag_id }}'
    },
    dag=dag
).expand(op_args=GET_SOURCE_OBJECTS_Task.output)

ADLS_STAGE_TO_PROCESSED_Task = PythonOperator.partial(
    task_id='adls_stage_to_processed',
    python_callable=my_python_operator_function,
    op_kwargs={
        "APPLICATION": APPLICATION,
        "SCHEMA": SCHEMA,
        "EXECUTION_DATE": "{{ execution_date.in_timezone('Australia/Perth').to_date_string() }}",
        "RUN_ID": '{{ run_id }}',
        "DAG_ID": '{{ dag.dag_id }}',
        "PROCESS": "ADLS_STAGE_TO_PROCESSED"
    },
    dag=dag
).expand(op_args=GET_SOURCE_OBJECTS_Task.output)

START_Task >> GET_SOURCE_OBJECTS_Task >> ADLS_CURRENT_TO_STAGE_Task >> ADLS_STAGE_TO_PROCESSED_Task >> END_Task

