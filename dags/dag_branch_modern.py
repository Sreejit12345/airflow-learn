from airflow.sdk import dag,task


@dag(schedule="@daily")
def branch_in_dag():
    

    @task
    def task_A():

        a_ret_vale=1   #if this is run then task_C runs
        #a_ret_vale=-1   #if this is run then task_D runs
        print(f"My value is {a_ret_vale}")

        return a_ret_vale

    @task.branch    #this wraps the below func into BranchPythonOperator
    def task_B(value):
        
        if(value>=1):
            return ['task_C']    # we pass the taskId of reqd task here, we can also pass list of multiple task if reqd

        else:
            return ['task_D']    # we pass the taskId of reqd task here, we can also pass list of multiple task if reqd

    @task
    def task_C():
        print("I got executed!")

    @task
    def task_D():
        print("I got executed!")

    
    op1=task_A()
    op2=task_B(op1)


    op1 >> op2 >> [task_C(),task_D()]



branch_in_dag()

