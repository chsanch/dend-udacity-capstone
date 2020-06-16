from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator
from airflow.operators import (
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

loans_dataset_list = {
    "february-2018.csv": "https://datos.madrid.es/egob/catalogo/212700-80-bibliotecas-prestamos-historico.csv",
    "march-2018.csv": "https://datos.madrid.es/egob/catalogo/212700-82-bibliotecas-prestamos-historico.csv",
    "april-2018.csv": "https://datos.madrid.es/egob/catalogo/212700-84-bibliotecas-prestamos-historico.csv",
}

catalog_dataset_url = (
    "https://datos.madrid.es/egob/catalogo/200081-1-catalogo-bibliotecas.gz"
)

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
    op_kwargs={
        "datasets_folder": datasets_folder,
        "datasets_list": loans_dataset_list,
        "marc_file": marc_file,
        "catalog_url": catalog_dataset_url,
    },
)

create_catalog_json_task = PythonOperator(
    task_id="create_stage_catalog",
    python_callable=marc2json.main,
    dag=dag,
    op_kwargs={"marc_file": marc_file, "catalog_output": catalog_output},
)

create_stage_files_task = PythonOperator(
    task_id="create_stage_books_data",
    python_callable=create_stage_files.main,
    dag=dag,
    op_kwargs={
        "json_file": catalog_output,
        "books_file": f"{datasets_folder}/books.csv",
        "items_file": f"{datasets_folder}/items.csv",
    },
)

create_loans_files_task = PythonOperator(
    task_id="create_stage_loans_data",
    python_callable=create_loans_files.main,
    dag=dag,
    op_kwargs={
        "datasets_folder": datasets_folder,
        "datasets_list": list(loans_dataset_list),
    },
)

loans_to_database = LoadFactOperator(
    task_id="Loans_data_to_DB",
    dag=dag,
    db_conn="capstone_db",
    table="loans",
    multi=True,
    datasets_folder=datasets_folder,
    datasets_list=list(loans_dataset_list),
)

books_to_database = LoadFactOperator(
    task_id="Books_data_to_DB",
    dag=dag,
    data_path=f"{datasets_folder}/books.csv",
    table="books",
    db_conn="capstone_db",
    insert_type="csv",
    delimiter="|",
)

books_locations_to_database = LoadFactOperator(
    task_id="Books_locations_data_to_DB",
    dag=dag,
    data_path=f"{datasets_folder}/items.csv",
    table="books_locations",
    db_conn="capstone_db",
    insert_type="csv",
    delimiter="|",
)


# load_time_dimension_table = LoadDimensionOperator(
#     task_id="Load_time_dim_table", dag=dag
# )

# run_quality_checks = DataQualityOperator(task_id="Run_data_quality_checks", dag=dag)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_datasets_task

download_datasets_task >> create_catalog_json_task

create_catalog_json_task >> [
    create_stage_files_task,
    create_loans_files_task,
]


create_stage_files_task >> [
    books_to_database,
    books_locations_to_database,
] >> end_operator
create_loans_files_task >> loans_to_database >> end_operator
