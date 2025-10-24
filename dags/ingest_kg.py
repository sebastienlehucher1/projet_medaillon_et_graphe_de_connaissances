from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from datetime import datetime


with DAG(
    "dag_exec_make_run_pipeline",
    start_date=datetime(2025, 10, 24),
    schedule=None,
    catchup=False,
    tags=['make', 'bash']
) as dag_exec_make_run_pipeline:    
    # Exécute la commande 'make run_pipeline'
    exec_make_run_pipeline = BashOperator(
        task_id='exec_make_run_pipeline',
        bash_command='make run_pipeline',        
    )