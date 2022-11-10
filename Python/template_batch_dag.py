# -*- coding: utf-8 -*-
"""
This is the dag which will be deployed on cloud composer for running batch jobs
"""

from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from datetime import datetime, timedelta
#from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
#from google.cloud import storage
#from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('Batch_Dataflow_dag',
            catchup=False,
            schedule_interval=None,
            default_args=default_args)


start = DummyOperator(task_id='start',dag=dag)

job = DataflowTemplateOperator(
        task_id='template_test_job',
        template="gs://gcp-file-source/batchtemplate",
        job_name='job_test_from_airflow',
        
        dataflow_default_options={
            "project": "fabric-1333",
            "stagingLocation": "gs://gcp-file-source/dataflow/staging",
            "tempLocation": "gs://gcp-file-source/dataflow/temp",
            "serviceAccountEmail": "svc-rw@fabric-1333.iam.gserviceaccount.com"},
        parameters={
            "input_path": "gs://gcp-file-source/data3/*",
            "table": "test.business",
            "error_table": "test.error"           
        },
                dag=dag)

start >> job

"""with dag:
    start = DummyOperator(task_id='start')
    t1 = PythonOperator(
        task_id='Pipeline_definition',
        python_callable=upload_pipeline_def)

    start >> t1
    """