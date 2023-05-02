# SF (Snowflake) Folder

Here we manage the snowflake connection and everything related to the creation of the tables and import of the data.

Basically, we create an [stage](https://docs.snowflake.com/en/user-guide/data-load-considerations-stage) to be able to connect to the bucket, it could be S3 or a GCS Bucket, and then we load the data into a [transient table](https://docs.snowflake.com/en/user-guide/tables-temp-transient) that would hold the data while the connection is open. 
Then we load the data into a final table with the transformation required.

When the connection starts, it automatically creates the environment to work. It creates the database, landing database, and the schema in each database.

