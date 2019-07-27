from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 sql_query,
                 *args,
                 replace=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.replace = replace

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.replace:
            redshift.run('DELETE FROM {}'.format(self.table))
        redshift.run(
            'INSERT INTO {} {}'.format(self.table, self.sql_query)
        )
        logging.info("Load Table - {} complete.".format(self.table))
