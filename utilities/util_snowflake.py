"""Snowflake Utilites"""
from prefect import task, get_run_logger
from prefect_snowflake.database import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas


@task(name="Create Snowflake Connection")
def get_snowflake_connection(block_name):
    """Establish a Snowflake connection and return it as a context manager."""
    connector = SnowflakeConnector.load(block_name)
    connection = connector.get_connection()
    return connection


@task(name="Create Table in Snowflake")
def create_table(connection, database_name, schema_name, table_name, DDL):
    """Creates the required table as needed

    Args:
        connection (connection): Snowflake Connection
    """

    statement = (
        f"CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} ({DDL})"
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)


@task(name="Query Snowflake and Return Exsisting Values")
def get_existing_values(
    connection,
    database_name,
    schema_name,
    table_name,
    column_name,
    conditionals=None,
    return_list=True,
):
    """Queries snowflake to get a list of values from a single column
    or a dataframe if multiple columns are specified

    Args:
        connection (conn): snowflake connection
        database_name (String): target DB name
        schema_name (String): target Schema
        table_name (String): Target Table
        column_name (String): Column to Select and Return
        conditionals (String, optional): "LIMIT 10". Defaults to None.
        return_list (Bool), optional): If you want a list or a Dataframe

    Returns:
        list: Flat list of all values for iteration
    """
    #logger = get_run_logger()
    statement = f"SELECT {column_name} FROM {database_name}.{schema_name}.{table_name} {conditionals};"
    with connection.cursor() as cursor:
        cursor.execute(statement)
        df = cursor.fetch_pandas_all()

    if return_list:
        existing_values_list = df[column_name].tolist()
        return existing_values_list
    else:
        return df


@task(name="Write Dataframe to Snowflake")
def write_dataframe(connection, database_name, schema_name, table_name, filtered_df):
    """_summary_

    Args:
        connection (_type_): Snowflake Connection
        filtered_df (_type_): Deduped Dataframe
    """
    logger = get_run_logger()

    if len(filtered_df) != 0:
        logger.info(filtered_df)

        write_pandas(
            conn=connection,
            df=filtered_df,
            table_name=table_name,
            database=database_name,
            schema=schema_name,
        )

        logger.info("Data loaded to Snowflake")
    else:
        logger.info("No new records to log")
