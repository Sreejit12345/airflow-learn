'''
Represents branch operator by legacy way

'''

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

with DAG(dag_id="dag_branch_legacy",schedule='@daily') as dag:


    
    b1=BashOperator(task_id='b1',bash_command='echo "Welcome to DAG"')

    
    def python_func1(**kwargs):

        print(kwargs)   #returns entire context dict

        #value=-1  # If this runs the f chain runs
        value =1   # If this runs the c chain runs

        kwargs['ti'].xcom_push(key="python_val", value=value)

    
    p1= PythonOperator(task_id='p1',python_callable=python_func1)




    def branch_here(**kwargs):

        val=kwargs['ti'].xcom_pull(key="python_val", task_ids='p1')

        if(val>=1):
            return ['c']

        else:
            return ['f']


    bb1= BranchPythonOperator(task_id='branch1',python_callable=branch_here)


    
    c=BashOperator(task_id='c',bash_command='echo "executing task c"')
    d=BashOperator(task_id='d',bash_command='echo "executing task d"')
    e=BashOperator(task_id='e',bash_command='echo "executing task e"')

    f=BashOperator(task_id='f',bash_command='echo "executing task f"')
    g=BashOperator(task_id='g',bash_command='echo "executing task g"')
    h=BashOperator(task_id='h',bash_command='echo "executing task h"')


    
    b1 >> p1 >> bb1 >> [c,f]

    c >> d >> e

    f >> g >> h






    



        






