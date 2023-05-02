import argparse
import logging
import yaml

from models import Table
from sf.snowflake_conn import SnowflakeConnector
from sf.importer import S3Stage

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _parseargs():
    parser = argparse.ArgumentParser(description='Getting initial attributes')
    parser.add_argument('--user', required=True, help='Snowflake user')
    parser.add_argument('--password', required=True, help='Snowflake password')
    return parser


def _read_table(args):
    with open(f'./tables/{args.table_config}.yml') as file:
        tables = yaml.safe_load(file)
    table_config = tables.get('tables', {})
    return table_config.get(args.table, {})


def _get_columns(columns: dict):
    cols = []
    cols_type = []
    imp_data = []
    for col, val in columns.items():
        cols.append(col)
        cols_type.append(val.split('=')[1])
        imp_data.append(val.split('=')[0])
    return cols, cols_type, imp_data


def _snowflake_connection(args):
    sf_connector = SnowflakeConnector(
        user=args.user,
        password=args.password,
        account=args.account,
        warehouse=args.warehouse,
        role=args.role,
        database=args.database,
        schema=args.schema,
    )
    logger.info("Connected to Snowflake")
    return sf_connector


def import_job(sf_conn: SnowflakeConnector, stage: S3Stage, table: Table):
    with sf_conn.connect().cursor() as cursor:
        stage.create_stage(table, cursor)
        stage.load_to_lakehouse(table, cursor)
        sf_conn.import_to_table(table)


def import_s3_data(args):
    table_conf = _read_table(args)
    cols, cols_type, imp_data = _get_columns(table_conf['columns'])
    # TODO add aws keys and storage integration
    import_job(
        _snowflake_connection(args),
        S3Stage(
            s3_bucket=args.bucket,
            file_format=args.file_format,
            suffix='.json'
        ),
        Table(
            name=args.table,
            database=args.database,
            schema=args.schema,
            columns=cols,
            columns_type=cols_type,
            import_data=imp_data,
            primary_key=table_conf.get('primary_key')
        )
    )


def main():
    parser = argparse.ArgumentParser(description='Getting initial attributes')
    parser.add_argument('--user', required=True, help='Snowflake user')
    parser.add_argument('--password', required=True, help='Snowflake password')
    parser.add_argument('--account', required=True, help='Snowflake account')
    parser.add_argument('--warehouse', required=True, help='Snowflake warehouse')
    parser.add_argument('--role', required=False, default='SYSADMIN',
                        help='Snowflake warehouse')
    parser.add_argument('--file_format', required=True, help='File Format for the stage')
    parser.add_argument('--database', required=True, help='Destination database')
    parser.add_argument('--schema', required=True, help='Destination schema')
    parser.add_argument('--table', required=True, help='Table Name')
    parser.add_argument('--bucket', required=True, help='Bucket URL')
    parser.add_argument('--table_config', required=True, help='Bucket URL')
    args = parser.parse_args()
    
    logger.info("Starting the pipeline")
    import_s3_data(args)


if __name__ == '__main__':
    main()
