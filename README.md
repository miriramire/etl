# ELT

This is an ELT process, were we have the data located in the bucket s3://de-tech-assessment-2022, and we are going to import it into snowflake.

The link to access to snowflake is: https://ztuuoka-evb05477.snowflakecomputing.com/console/login

In this ELT, we will follow this process:

1. Connect to snowflake
2. Create the databases (Landing, Production)
3. Create a temporary stage to load the data
4. Create a transient table, were we will load the data
5. Create the production table
6. Transform the data and load it into the production table

In order to be able to accomplish this, we require the following packages:

| Package | Description |
| ---------------| --------------- |
| Jinja2 | To read a SQL template |
| parse | To read the initial parameters |
| snowflake-connector-python | To connect to snowflake |
| pyyaml | To read tables parameters |

To be able to run this ELT in Docker, it is needed to run the following commands:

1. `docker build -t etl:1.0 .`
2. `docker run --entrypoint python etl:1.0 main.py --user <username> --password <password> --account <account> --warehouse <warehouse> --file_format <file_format> --database <database> --schema <schema> --table <table> --bucket <bucket> --table_config <table_config>`

The parameters needed in order to be able to run the ETL are:

| Parameter | Description |
| ---------------| --------------- |
| `--user` | User to connect to snowflake |
| `--password` | Password to connect to snowflake |
| `--account` | Snowflake account |
| `--warehouse` | Snowflake warehouse |
| `--file_format` | File format to create the Stage |
| `--database` | Destination Database |
| `--schema` | Destination Schema |
| `--table` | Destination Table |
| `--bucket` | Bucket URL |
| `--table_config` | Initial configuration of the table |



