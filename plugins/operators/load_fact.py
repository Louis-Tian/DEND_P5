from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 *args,
                 replace=True,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.replace = replace

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run('DELETE FROM {}'.format(self.table))
        if self.replace:
            redshift.run('DELETE FROM {}'.format(self.table))
        redshift.run(
            'INSERT INTO {} {}'.format(self.table, self.sql_query)
        )
        logging.info("Load Table - {} complete.".format(self.table))
