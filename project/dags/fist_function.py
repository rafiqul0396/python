# 1. import modules
try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.bash_operator import BashOperator

    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))



# define task
def first_function_execute(**context):
    print("first_function_execute   ")
    return " Rafik made this,this from fist function"

def second_function_execute():


    print("Rafik make this function,I  am using seond _function _execute ")
    return " this from second function"

def third_function():
    print(" i am Rafik , this is ok ,i dont know what i am doing here")
    return "no one knows"

# 3. defulat  arugments
with DAG(
        dag_id="Rafik_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

# dependences
    first_function_execute = PythonOperator(
        task_id="fist_functions",
        python_callable=first_function_execute,
        provide_context=False

    )

#
    second_function_execute = PythonOperator(
        task_id="second_function",
        python_callable=second_function_execute,
        provide_context=False,
    )
    third_function = PythonOperator(
        task_id="third_function",
        python_callable=third_function,
        provide_context=False,
    )
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

# setting dependencies

first_function_execute >> [second_function_execute, third_function]
t1 >> t2


#first_function_execute >> second_function_execute>>third_function

