from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
import scripts.download_datasets as download_datasets
import scripts.marc2json as marc2json
import scripts.create_stage_files as create_stage_files
import scripts.create_loans_files as create_loans_files

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "chsanch",
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "retries": 2,
    "start_date": datetime(2020, 1, 1),
}


datasets_folder = f"{os.path.abspath(os.getcwd())}/datasets"
marc_file = f"{datasets_folder}/catalog_marc"
catalog_output = f"{datasets_folder}/catalog.json"

dag = DAG(
    "capstone_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval=None,
    catchup=False,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

download_datasets_task = PythonOperator(
    task_id="download_datasets",
    python_callable=download_datasets.main,
    dag=dag,
    op_kwargs={"datasets_folder": datasets_folder},
)

create_catalog_json_task = PythonOperator(
    task_id="create_catalog_json",
    python_callable=marc2json.main,
    dag=dag,
    op_kwargs={"marc_file": marc_file, "catalog_output": catalog_output},
)

create_stage_files_task = PythonOperator(
    task_id="create_stage_files",
    python_callable=create_stage_files.main,
    dag=dag,
    op_kwargs={
        "json_file": catalog_output,
        "books_file": f"{datasets_folder}/books.json",
        "items_file": f"{datasets_folder}/items.json",
    },
)

create_loans_files_task = PythonOperator(
    task_id="create_loans_files",
    python_callable=create_loans_files.main,
    dag=dag,
    op_kwargs={"datasets_folder": datasets_folder,},
)


# stage_events_to_redshift = StageToRedshiftOperator(task_id="Stage_events", dag=dag)

# stage_songs_to_redshift = StageToRedshiftOperator(task_id="Stage_songs", dag=dag)

# load_songplays_table = LoadFactOperator(task_id="Load_songplays_fact_table", dag=dag)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id="Load_user_dim_table", dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id="Load_song_dim_table", dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id="Load_artist_dim_table", dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id="Load_time_dim_table", dag=dag
# )

# run_quality_checks = DataQualityOperator(task_id="Run_data_quality_checks", dag=dag)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_datasets_task

download_datasets_task >> create_catalog_json_task

create_catalog_json_task >> create_stage_files_task
create_catalog_json_task >> create_loans_files_task

create_loans_files_task >> end_operator
create_stage_files_task >> end_operator


# create_stage_files >> stage_events_to_redshift
# create_loans_files >> stage_events_to_redshift

# stage_events_to_redshift >> load_songplays_table


# load_songplays_table >> [
#     load_song_dimension_table,
#     load_user_dimension_table,
#     load_artist_dimension_table,
#     load_time_dimension_table,
# ] >> run_quality_checks >> end_operator
