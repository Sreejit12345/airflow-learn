'''
This .py files uses the old airflow syntax, i.e not the decorator based syntax

'''

from datetime import datetime
from airflow.sdk import DAG, task 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.python import PythonSensor
import requests
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



# A Dag represents a workflow, a collection of tasks

with DAG(dag_id="data_pipeline1", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    
    sqlquery= """
    CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,        -- Auto-incrementing unique identifier
    username VARCHAR(50) UNIQUE NOT NULL, -- Text up to 50 chars, must be unique
    email VARCHAR(255) NOT NULL,       -- Required field
    created_at TIMESTAMP DEFAULT NOW() -- Automatically sets the current time
);
        """
    op1= SQLExecuteQueryOperator(task_id='create_table',conn_id="postgr",sql=sqlquery)
    # remeber the conn_id "postgr" is configured in the airflow ui admin tab
    '''
    host : postgres
    login : airflow
    password : airflow
    port : 5432
    database: airflow 
    
    '''

    def check_api_availability(ti):
        resp=requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")

        if( resp.status_code ==200):
            ti.xcom_push(key="user_resp", value=resp.json())
            return True
    
        else:
            return False


    op2= PythonSensor(task_id="check_api",python_callable=check_api_availability,poke_interval=60,timeout=120)


    def parse_data(ti):
        
        resp= ti.xcom_pull(key="user_resp", task_ids="check_api")

        full_name= resp['personalInfo']['firstName'] + ' ' + resp['personalInfo']['lastName']
        email=resp['personalInfo']['email']

        return {
        "full_name": full_name,
        "email": email,
        'source_dag' : "simple_data_pipeline"
        }
        

    op3=PythonOperator(task_id="parse_response",python_callable=parse_data)

    def write_csv(ti):

        '''
        Here we write to local filesystem - but in PROD we should write to s3/adls.
        '''
        data= ti.xcom_pull(key="return_value", task_ids="parse_response")   #by default when python callable returns an xcom with key return_value is pushed
        df = pd.DataFrame([data])
        df.to_csv('/tmp/user.csv', index=False)

    op4=PythonOperator(task_id="write_csv_file",python_callable=write_csv)


    def copy_file(**kwargs):

        hk=PostgresHook(postgres_conn_id ="postgr")

        # for hook install pip install apache-airflow-providers-postgres

        sql="""
        COPY users(username, email,source_dag)
        FROM STDIN
        WITH CSV HEADER
        """

        # COPY FROM STDIN to copy to table
        # COPY TO STDOUT to copy to a file     -to do this just change the sql the below methid remains same

        
        hk.copy_expert(sql, '/tmp/user.csv')
        # this method opens connection/ executes copy and streams file content to DB

    op5=PythonOperator(task_id="write_to_db",python_callable=copy_file)

        

    op1 >> op2 >> op3 >> op4 >> op5



