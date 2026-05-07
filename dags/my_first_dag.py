from datetime import datetime
from airflow.sdk import DAG, task 
from airflow.sdk import dag as d
from airflow.providers.standard.operators.bash import BashOperator


# A Dag represents a workflow, a collection of tasks

with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")
    bye = BashOperator(task_id="bye", bash_command="echo bye")

    hello >> bye

#############################################################################################################

dag1= DAG(dag_id="demo1", start_date=datetime(2022, 1, 1), schedule="0 0 * * *")

t1=BashOperator(task_id="hello1", bash_command="echo hello",dag=dag1)
t2=BashOperator(task_id="bye1", bash_command="echo bye",dag=dag1)

t1>> t2

#############################################################################################################

@d(dag_id="demo2",start_date=datetime(2021, 1, 1), schedule="0 0 * * *")
def generate_dag():
    t1=BashOperator(task_id="task123",bash_command="echo decorator hello")
    t2=BashOperator(task_id="task124",bash_command="echo decorator bye")
    t1 >> t2


generate_dag()