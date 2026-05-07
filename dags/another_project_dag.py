from airflow.sdk import dag,task
import boto3
import boto3.session
from aws_boto_ex import orchestrate,download_file
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(dag_id='another_project',schedule='@daily')
def generate_dag():

    # this fn runs every 60 secs for max total run of 300 secs and returns true if successful
    # for any error it returns False and fucntion retries for timeout duration.

    #basically sensor runs untill the python callable returns True or timeout is over.

    @task.sensor(poke_interval=60, timeout=300, mode="poke")
    def wait_for_api():
        desired_bukcet_name="stock-data"
        file_name= "stock.csv"

        my_session=boto3.session.Session(aws_access_key_id= 'changemeuser',aws_secret_access_key='changemepass')
        client= my_session.client(service_name='s3',endpoint_url='http://minio:9000')

        x=orchestrate(client,desired_bukcet_name,file_name)    #this internally calls api and writes file to miniIo, it returns True if things ae okay and false if not

        return PokeReturnValue(is_done=x)

    
    t2 = DockerOperator(
    task_id='run_docker_spark_image',
    image='sreejits/my-spark-job_updated',

    entrypoint="/opt/spark/bin/spark-submit",

    # note these oveeride the cmd in image
    command=[
        "--master", "local[*]",
        "--class", "com.apple.airflowapp",

        # --- MinIO / S3A config (IMPORTANT FIXES) ---
        "--conf", "spark.hadoop.fs.s3a.endpodint=http://minio:9000",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",

        # --- CRITICAL FIX (prevents hanging on InstanceProfileCredentialsProvider) ---
        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
 

        # --- timeouts ---
        "--conf", "spark.hadoop.fs.s3a.connection.timeout=60000",
        "--conf", "spark.hadoop.fs.s3a.socket.timeout=60000",
        "--conf", "spark.network.timeout=120s",

        "/app/app.jar"
    ],

    docker_url='unix://var/run/docker.sock',
    network_mode='airflow-learning_default',    #this is the network where docker compose is runnign along with minio.
                                                #this is requird for docker coantienr to reach minio by host name.
    mount_tmp_dir=False,
    auto_remove='force'
)

    @task.sql(task_id='create_table',conn_id="postgr")
    def create_table():

        sqlquery= """
        CREATE TABLE IF NOT EXISTS apple_stock (
        id SERIAL PRIMARY KEY,        -- Auto-incrementing unique identifier
        date DATE  NOT NULL, -- date
        up FLOAT NOT NULL,       -- Required field
        down FLOAT NOT NULL,
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

    @task.python
    def download_file_and_load():

        desired_bukcet_name="stock-data"
        my_session=boto3.session.Session(aws_access_key_id= 'changemeuser',aws_secret_access_key='changemepass')
        client= my_session.client(service_name='s3',endpoint_url='http://minio:9000')

        download_file(client,desired_bukcet_name)

        hk=PostgresHook(postgres_conn_id ="postgr")

        # for hook install pip install apache-airflow-providers-postgres

        sql="""
        COPY apple_stock(date, up,down)
        FROM STDIN
        WITH CSV HEADER
        """

        # COPY FROM STDIN to copy to table
        # COPY TO STDOUT to copy to a file     -to do this just change the sql the below methid remains same
        
        hk.copy_expert(sql, '/tmp/spark_op.csv')




    wait_for_api() >> t2 >> create_table() >> download_file_and_load()

# for /f "tokens=5" %a in ('netstat -aon ^| find ":8080" ^| find "LISTENING"') do taskkill /F /PID %a
generate_dag()