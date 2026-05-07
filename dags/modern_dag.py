'''
This .py files uses the modern airflow syntax, i.e not the decorator based syntax.

Look at official doc to find diff, basic diff is, we substiture the constructor call with decorator

Also remeber, we can mix old and new styles , if the operator does not have. Use documentation :

https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html

https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/sensors/python.html#howto-operator-pythonsensor

'''


from airflow.sdk import dag,task
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.base import PokeReturnValue
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(dag_id="data_pipeline_modern",start_date=datetime(2021, 1, 1), schedule="0 0 * * *")
def generate_dag():



    @task.bash
    def welcome_to_DAG():
        return "echo Modern Bash Operator!!"




    @task.sql(task_id='create_table',conn_id="postgr")
    def create_table():

        sqlquery= """
        CREATE TABLE IF NOT EXISTS users (
        user_id SERIAL PRIMARY KEY,        -- Auto-incrementing unique identifier
        username VARCHAR(50)  NOT NULL, -- Text up to 50 chars, must be unique
        email VARCHAR(255) NOT NULL,       -- Required field
        source_dag VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW() -- Automatically sets the current time
        );
    """
        return sqlquery

    # remeber the conn_id "postgr" is configured in the airflow ui admin tab
    '''
    host : postgres
    login : airflow
    password : airflow
    port : 5432
    database: airflow 
    
    '''




    @task.sensor(task_id="check_api",poke_interval=60,timeout=120)
    def check_api_availability():
        resp=requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")

        if( resp.status_code ==200):
            #ti.xcom_push(key="user_resp", value=resp.json())
            return PokeReturnValue(is_done=True, xcom_value=resp.json())   #xcom pushed automatically
    
        else:
            return PokeReturnValue(is_done=False)




    @task.python(task_id="parse_response")    #same as @task
    def parse_data(resp):
        # resp is the xcom pushed from earlier task (based on orchestration), which in this case is sensor
    
        full_name= resp['personalInfo']['firstName'] + ' ' + resp['personalInfo']['lastName']
        email=resp['personalInfo']['email']
       

       # return by default pushes data for next task
        return {
            "full_name": full_name,
            "email": email,
            'source_dag' : "modern_dag"
            }




    @task.python(task_id="write_csv")
    def write_csv(resp):
        # resp is the xcom pushed from earlier task (based on orchestration), which in this case is parse_response

        '''
        Here we write to local filesystem - but in PROD we should write to s3/adls.
        '''

        df = pd.DataFrame([resp])
        df.to_csv('/tmp/user.csv', index=False)

    
    @task.python(task_id="write_to_db")
    def copy_file():

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



    welcome = welcome_to_DAG()
    create = create_table()
    check_api_op = check_api_availability()
    parse_data_op = parse_data(check_api_op)
    write_csv_op = write_csv(parse_data_op)
    copy = copy_file()

    welcome >> create >> check_api_op >> parse_data_op >> write_csv_op >> copy
            
  

generate_dag()