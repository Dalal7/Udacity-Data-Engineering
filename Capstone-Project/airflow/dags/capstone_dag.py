import airflow
from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Dalal',
    'start_date': airflow.utils.dates.days_ago(1),
    'email':['dalalalbdah@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,              
    'retries':3,                         
    'retry_delay':timedelta(minutes=5),   
    'depends_on_past':False,    
    'catchup':False  
}

dag = DAG('DEND_Capstone_DAG',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    table = "staging_immigration",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    s3_bucket="udacity-dend-dalal",
    s3_key="immigration_data",
    file_type="CSV" 
)

stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperature',
    dag=dag,
    table = "staging_temperature",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    s3_bucket="udacity-dend-dalal",
    s3_key="temperature_data",
    file_type="CSV"
)

stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographic',
    dag=dag,
    table = "staging_demographic",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_default",
    s3_bucket="udacity-dend-dalal",
    s3_key="demographic_data",
    file_type="CSV"
)

load_city_table = LoadFactOperator(
    task_id='Load_city_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_insert_query =SqlQueries.city_table_insert
)

load_immigrant_dimension_table = LoadDimensionOperator(
    task_id='Load_immigrant_dim_table',
    dag=dag,
    table="immigrant",
    redshift_conn_id="redshift",
    sql_insert_query =SqlQueries.immigrant_table_insert    
)

load_temperature_dimension_table = LoadDimensionOperator(
    task_id='Load_temperature_dim_table',
    dag=dag,
    table="temperature",
    redshift_conn_id="redshift",
    sql_insert_query =SqlQueries.temperature_table_insert    
)

load_demographic_dimension_table = LoadDimensionOperator(
    task_id='Load_demographic_dim_table',
    dag=dag,
    table="demographic",
    redshift_conn_id="redshift",
    sql_insert_query =SqlQueries.demographic_table_insert    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    provide_context=True,
    params= {
        "table": ["city","immigrant","temperature","demographic"] 
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_immigration_to_redshift,
                   stage_temperature_to_redshift, 
                   stage_demographic_to_redshift] >> load_city_table >> [load_immigrant_dimension_table,
                                                                        load_temperature_dimension_table,
                                                                        load_demographic_dimension_table] >> run_quality_checks >> end_operator