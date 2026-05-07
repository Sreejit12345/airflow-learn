from airflow.sdk import task,dag,literal
import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


@dag(dag_id="template_exampels",start_date=datetime.datetime(2021, 1, 1), schedule="0 0 * * *")
def generate_dag():
    
    # from the definiton of BashOperator : template_fields: Sequence[str] = ("bash_command", "env", "cwd") 
    # so it means only "bash_command", "env", "cwd" paramters can accept templates by default

    # here we are passing thee current_date as env variable and printing that by echo
    t1= BashOperator(task_id='templated_id',bash_command='echo $current_date',
                     env={'current_date':'{{ds}}'})



    # from the definiton of Python operator : template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")
    # so it means ("templates_dict", "op_args", "op_kwargs") can be templated


    def python_func(**kwargs):
        print("run id can be ", kwargs['templates_dict']['dag_id_sreejit'])
        print("dag  can be ",kwargs['templates_dict']['dag_name_sreejit'])
        print("dag start can be ",kwargs['sreejit_start_time'])
        print("dag end can be ",kwargs['sreejit_end_time'])

    t2= PythonOperator(task_id='dd',python_callable=python_func,
                       
                       templates_dict={'dag_id_sreejit':'{{run_id }}',
                                       'dag_name_sreejit': '{{dag.dag_id}}'},

                        op_kwargs={'sreejit_start_time':'{{data_interval_start }}',
                                   'sreejit_end_time':'{{data_interval_end  }}'}

                       )
    
    t1  >> t2

generate_dag()