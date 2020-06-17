from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"
    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(
        self,
        schema="public",
        table="",
        db_conn="",
        select_sql="",
        truncate=True,
        *args,
        **kwargs,
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.schema = schema
        self.db_conn = db_conn
        self.select_sql = select_sql
        self.truncate = truncate

    def execute(self, context):
        database = PostgresHook(postgres_conn_id=self.db_conn)
        if self.truncate is True:
            self.log.info("Clearing data from destination table")
            database.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Inserting data into {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table, self.select_sql
        )
        database.run(formatted_sql)
