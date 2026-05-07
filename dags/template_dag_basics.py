from airflow.sdk import task,dag,literal
import datetime
from airflow.providers.standard.operators.bash import BashOperator


@dag(dag_id="data_pipeline_modern_templating",start_date=datetime.datetime(2021, 1, 1), schedule="0 0 * * *")
def generate_dag():


    #enable templating
    @task.bash
    def welcome_to_DAG():
        return "echo {{ ds }}"

    

    #disable templating
    welcome_to_DAG1 = BashOperator(
        task_id="welcome_to_DAG1",
        bash_command=literal("echo {{ ds }}")
    )

    welcome_to_DAG() >> welcome_to_DAG1

generate_dag()