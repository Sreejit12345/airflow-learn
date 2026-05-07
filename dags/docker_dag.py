'''

Example of running a docker image as a container from inside airflow.

Install pip install apache-airflow-providers-docker in venv and also make sure its present in airflow worker.

If airflow itself runs on docker container . To make sure we can run properly : put this line in docker compose

- /var/run/docker.sock:/var/run/docker.sock


'''

from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator


@dag(schedule='@daily')
def docker_DAG():
    

    # image is the image naame , it tries to find from dockerHub or local
    # command is what to run after launching container


    t1= DockerOperator(task_id='run_docker_image',
                       image='python:3',
                       command='python --version',
                       docker_url='unix://var/run/docker.sock',
                       network_mode='bridge',
                       auto_remove ='force')

    t2= DockerOperator(task_id='run_docker_spark_image',
                       image='sreejits/my-spark-job',
                       entrypoint="/opt/spark/bin/spark-submit",   #override defaulr entry point of image
                        command=[
                        "--master", "local[*]",
                        "--class", "com.batch.basic",
                        "/app/app.jar"
                        ],
                       docker_url='unix://var/run/docker.sock',
                       network_mode='bridge',
                       mount_tmp_dir = False,
                       auto_remove ='force')
    
    t1 << t2

docker_DAG() 