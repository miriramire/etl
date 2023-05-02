import logging

from models import Table

LANDING_DB = 'LANDING_LAKEHOUSE'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3Stage:
    def __init__(self,
                 s3_bucket: str,
                 aws_key_id: str = None,
                 aws_secret_key: str = None,
                 tabular: bool = False,
                 storage_integration: str = None,
                 file_format: str = "TYPE = JSON",
                 suffix: str = '.json',
                 pattern: str = None,
                 on_error: str = None,
                 ):
        self._s3_bucket = s3_bucket
        self._aws_key_id = aws_key_id
        self._aws_secret_key = aws_secret_key
        self._tabular = tabular
        self._storage_integration = storage_integration
        self._file_format = file_format
        self._suffix = suffix
        self._pattern = pattern
        self._on_error = on_error

    def create_stage(self, table: Table, cursor):
        logger.info("Creating Stage")
        credentials_access = ""
        if self._aws_key_id and self._aws_secret_key:
            credentials_access = f"CREDENTIALS=(AWS_KEY_ID='{self._aws_key_id}'" \
                                 f"AWS_SECRET_KEY='{self._aws_secret_key}')"
        if self._storage_integration:
            credentials_access = f"STORAGE_INTEGRATION = {self._storage_integration}"

        stage_creation = f""" 
            CREATE OR REPLACE TEMPORARY STAGE {table.name}_STAGE
            URL='{self._s3_bucket}'
            {credentials_access}
            FILE_FORMAT = ({self._file_format});
        """
        cursor.execute(f"USE DATABASE {LANDING_DB}")
        cursor.execute(stage_creation.strip())

    def load_to_lakehouse(self, table: Table, cursor):
        self._create_landing_table(table, cursor)
        self._load_data(table, cursor)

    def _create_landing_table(self, table: Table, cursor):
        if self._tabular:
            # To add CSV data
            pass
        else:
            cursor.execute(f"CREATE OR REPLACE TRANSIENT TABLE {LANDING_DB}.{table.schema}.{table.name}"
                           f"(FILE VARIANT);")

    def _load_data(self, table: Table, cursor):
        if self._pattern:
            pattern = self._pattern
        else:
            pattern = f".*\\\\{self._suffix}$"
        on_error = ""
        if self._on_error:
            on_error = self._on_error
        cursor.execute(f"COPY INTO {LANDING_DB}.{table.schema}.{table.name}"
                       f" FROM (SELECT * FROM @{table.name}_STAGE)"
                       f"PATTERN = '{pattern}'"
                       f"{on_error}")
