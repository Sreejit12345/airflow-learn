'''
We will run a spark job by bash operator and spark submit operator


to run sparksubmit operator install  : pip install apache-airflow-providers-apache-spark
#remember this has to be installed in airflow workers, so if airflow is running in docker install there as part of airflow container setup

'''

from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(schedule='@daily')
def spark_DAG():

    @task.bash
    def submit_spark_job():

        spark_sumit_cmd="""
        spark-submit --master local[*] --class com.batch.agg /tmp/scala1234learn-assembly-0.1.0-SNAPSHOT.jar 
        
        
        """
        return spark_sumit_cmd
    
 
    t2= SparkSubmitOperator(task_id='run_spark_job',
                            application="/tmp/scala1234learn-assembly-0.1.0-SNAPSHOT.jar",
                            name='sreejit spark app',
                            conn_id='spark_conn_local',
                            java_class= 'com.batch.agg'
                            )

    # remeber to create a conn_id in airflow with host as local[*] in this case
    


    submit_spark_job()  >> t2

spark_DAG() 