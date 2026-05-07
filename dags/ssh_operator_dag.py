# for installing ssh operator install    pip install apache-airflow-providers-ssh
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag,task
from datetime import datetime

'''
This is one way of running spark jobs by airflow.

step 1: Turn windows ssh "able" by turing ssh feature on.
step 2: Create ssh connection in airflow
step 3: now airflow runs inside docker container (as defined by setup) , then from the container it does an ssh into local windows
step 3: It chanegs path to "spark home directory" of local machine and does a spark submit from a compiled jar present in local machine.


'''



@dag(dag_id="data_pipeline_ssh",start_date=datetime(2021, 1, 1), schedule="0 0 * * *")
def sample_dag():
    cmd = r"cd C:\spark-3.5.8-bin-hadoop3-scala2.13\bin && spark-submit --master local[*] --class com.batch.agg C:\Users\SREEJIT\OneDrive\Documents\Scala1234Learn\target\scala-2.13\scala1234learn-assembly-0.1.0-SNAPSHOT.jar"


    ssh1=SSHOperator(task_id="ssh",ssh_conn_id='mmm',
                command=cmd,cmd_timeout=10000)
    
    ssh1


sample_dag()