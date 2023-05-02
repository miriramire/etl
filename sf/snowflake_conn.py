import logging
import snowflake.connector

from models import Table
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

TEMPLATE = 'merge_template.sql.jinja'
META_TABLE = '_META_JOBS'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def jinja_zip(a, b):
    return zip(a, b)


def render_sql_template(arguments: dict) -> str:
    env = Environment(loader=FileSystemLoader('sf/sql/'))
    env.filters['zip'] = jinja_zip
    template = env.get_template(TEMPLATE)
    query = template.render(arguments)
    return query.split(";")


class SnowflakeConnector:
    
    def __init__(self, 
                 user: str, 
                 password: str, 
                 account: str,
                 warehouse: str, 
                 role: str,
                 database: str, 
                 schema: str):
        self._user = user
        self._password = password
        self._account = account
        self._warehouse = warehouse
        self._role = role
        self._database = database
        self._landing_db = 'LANDING_LAKEHOUSE'
        self._schema = schema
        self.connection = None
        self._start_time = datetime.now()
        self._initial_configuration()
    
    def connect(self):
        """Connect to Snowflake using the specified credentials."""
        try:
            logger.info("Connecting to snowflake")
            self.connection = snowflake.connector.connect(
                account=self._account,
                user=self._user,
                password=self._password,
                warehouse=self._warehouse,
                role=self._role
            )
            self.connection.autocommit(True)
            return self.connection
        except Exception as e:
            logger.error("Not possible to connect to Snowflake")
            raise e
    
    def execute_query(self, query):
        """Execute a SQL query and return the results."""
        if self.connection is None:
            self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def close(self):
        """Close the Snowflake connection."""
        if self.connection is not None:
            self.connection.close()
            
    def _initial_configuration(self) -> None:
        logger.info("Configuring Initial requirements")
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE WAREHOUSE {self._warehouse}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self._database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._database}.{self._schema}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self._landing_db}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._landing_db}.{self._schema}")
            self._grant_permission(cursor)

    def _grant_permission(self, cursor):
        cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {self._database}.{self._schema} to ROLE READ_ONLY")
        cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {self._landing_db}.{self._schema} to ROLE READ_ONLY")

    def _create_table(self, table: Table):
        query = f"""CREATE TABLE IF NOT EXISTS {self._database}.{self._schema}.{table.name} 
        ({", ".join([f"{col} {col_type}" 
                     for col, col_type in zip(table.columns, table.columns_type)])})"""
        self.execute_query(query)
        
    def _drop_table(self, database: str, table: Table):
        query = f"DROP TABLE IF EXISTS {database}.{self._schema}.{table.name}"
        self.execute_query(query)

    def _import_data(self, table: Table):
        arguments = {
            'landing': self._landing_db,
            'database': table.database,
            'schema': table.schema,
            'table': table.name,
            'columns': table.columns,
            'import_data': table.import_data,
            'merge_ids': table.primary_key
        }
        query = render_sql_template(arguments)[0].strip()
        logger.info(query)
        result = self.execute_query(query)
        end_time = datetime.now()
        logger.info(f"Number of rows inserted {result[0][0]} "
                    f"Number of rows updated {result[0][1]}")
        self._load_metadata(result, end_time, table)

    def _load_metadata(self, result: list, end_time: datetime, table: Table):
        data = (table.name, str(result[0][0]),
                str(result[0][1]), str(self._start_time), str(end_time))
        columns = ['table_name', 'number_of_rows_inserted',
                   'number_of_rows_updated', 'start_time',
                   'end_time']
        columns_type = ['VARCHAR', 'INTEGER', 'INTEGER',
                        'TIMESTAMP_NTZ', 'TIMESTAMP_NTZ']
        meta_jobs_table = Table(name=META_TABLE,
                                database=self._database,
                                schema=self._schema,
                                columns=columns,
                                columns_type=columns_type)
        self._create_table(meta_jobs_table)
        self._insert_multiple_data(meta_jobs_table, data)

    def _insert_multiple_data(self, table: Table, data: tuple):
        query = f"""INSERT INTO {self._database}.{self._schema}.{table.name} 
                    ({','.join(table.columns)})
                    VALUES ('{"','".join(data)}')"""
        self.execute_query(query)

    def import_to_table(self, table: Table):
        self._create_table(table)
        self._import_data(table)
        self._drop_table(self._landing_db, table)
