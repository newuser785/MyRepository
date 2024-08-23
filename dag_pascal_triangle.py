from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def generate_pascals_triangle(n):
    triangle = []
    for i in range(n):
        row = [1] * (i + 1)
        for j in range(1, i):
            row[j] = triangle[i - 1][j - 1] + triangle[i - 1][j]
        triangle.append(row)
    return triangle

def print_pascals_triangle(**kwargs):
    n = kwargs['n']
    triangle = generate_pascals_triangle(n)
    for row in triangle:
        print(" ".join(map(str, row)))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='A DAG to generate Pascal\'s Triangle',
    schedule_interval='44 11 * * *',
    start_date=datetime(2024, 8, 23),
    catchup=False,
)

task = PythonOperator(
    task_id='print_pascals_triangle',
    python_callable=print_pascals_triangle,
    op_kwargs={'n': 10},
    dag=dag,
)