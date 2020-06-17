from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"
    copy_from_csv = """
        COPY {}.{}
        FROM '{}'
        CSV
        DELIMITER '{}' 
    """

    copy_from_json = """
        COPY {}.{}
        FROM '{}'
        FORMAT JSON '{}' 
        COMPUPDATE OFF
    """

    @apply_defaults
    def __init__(
        self,
        schema="public",
        json_path="auto",
        table="",
        db_conn="",
        data_path="",
        insert_type="csv",
        delimiter=",",
        multi=False,
        datasets_folder="",
        datasets_list=[],
        *args,
        **kwargs,
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.schema = schema
        self.db_conn = db_conn
        self.data_path = data_path
        self.json_path = json_path
        self.insert_type = insert_type
        self.delimiter = delimiter
        self.multi = multi
        self.datasets_folder = datasets_folder
        self.datasets_list = datasets_list

    def run_sql(self, data_path):
        database = PostgresHook(postgres_conn_id=self.db_conn)
        self.log.info(f"Inserting data into {self.table}")
        if self.insert_type == "csv":
            formatted_sql = LoadFactOperator.copy_from_csv.format(
                self.schema, self.table, data_path, self.delimiter
            )
        else:
            formatted_sql = LoadFactOperator.copy_from_json.format(
                self.schema, self.table, data_path, self.json_path
            )
        database.run(formatted_sql)

    def multi_sql(self):
        for file in list(self.datasets_list):
            file_path = file.split(".")
            data_path = f"{self.datasets_folder}/{file_path[0]}_stage.csv"
            LoadFactOperator.run_sql(self, data_path)

    def execute(self, context):
        if self.multi is True:
            LoadFactOperator.multi_sql(self)
        else:
            LoadFactOperator.run_sql(self, self.data_path)
        
