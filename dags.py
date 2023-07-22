# from airflow import DAG

# from airflow.operators.empty i
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

# from datetime import timedelta

# args = {
#     'owner': 'agung'}

# dag = DAG(
#     dag_id='dags_final_project',
#     default_args=args,
#     schedule_interval='0 0 * * *',
#     start_date=days_ago(1),
#     dagrun_timeout=timedelta(minutes=60),
#     tags=['finpro', 'digital-skola'])

# # identify start process
# start = DummyOperator(
#     task_id='start',
#     dag=dag)

# # services
# insert_to_datalake = BashOperator(
#     task_id='insert_data_to_datalake',
#     bash_command='python3 \home\aguungm89\airflow\dags\final_project\insert_data.py',
#     dag=dag)

# ingest_to_hadoop = BashOperator(
#     task_id='ingestion_to_datalake_hadoop',
#     bash_command='python3 \home\aguungm89\airflow\dags\final_project\ingestion_to_datalake.py',
#     dag=dag)

# transform_load_to_dwh = BashOperator(
#     task_id='transform_load_to_dwh',
#     bash_command='python3 \home\aguungm89\airflow\dags\final_project\transform_load_dwh.py',
#     dag=dag)

# datamart = BashOperator(
#     task_id='insert_to_datamart',
#     bash_command='python3 \home\aguungm89\airflow\dags\final_project\dwh_to_datamart.py',
#     dag=dag)


# # identify start process
# stop = DummyOperator(
#     task_id='stop',
#     dag=dag)

# start >> ingest_to_hadoop  >> transform_load_to_dwh >> transform_load_to_dwh >> datamart >> stop