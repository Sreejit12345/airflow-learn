'''
Simple application to demonstrate task groups

'''

from airflow.sdk import dag,task,task_group

@dag(schedule="@daily")
def dag_task_group():


    @task_group
    def group1():

        @task
        def method1():
            print("Hi i am method 1")
            return 'A'

        @task
        def method2(val):
            # the earlier task automatically pushed to xcom annd i have acccess via value
            print(f"Hi i am method 2, I have retrieved value {val}")


        op1=method1()
        op2=method2(op1)
        
        op1 >> op2
    

    @task
    def method3():
        print("Hi i am method 3")
    

    group1()>> method3()

dag_task_group()


        