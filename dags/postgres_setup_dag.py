"""
PostgreSQL Database and Table Setup DAG

This DAG performs initial setup of PostgreSQL database and tables for the netkeiba project.
It handles two main tasks:

1. Database Creation:
   - Checks if 'netkeiba' database exists
   - Creates the database if it doesn't exist
   - Uses PostgresHook with 'postgres_airflow' connection

2. Table Creation:
   - Creates 'race_ids' table if it doesn't exist
   - Table contains columns for race_id (primary key), kaisai_id, and kaisai_date
   - Uses 'postgres_netkeiba' connection

DAG Configuration:
    - Runs once (schedule_interval="@once")
    - Starts from January 1, 2021
    - No catchup
    - Includes 2 retries on failure
    - Tagged as "database"

Dependencies:
    check_database_exists >> create_race_ids_table
"""

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def check_database_exists():
    """
    Check if 'netkeiba' database exists in PostgreSQL and create it if not present.

    This function:
    1. Establishes a connection to PostgreSQL using PostgresHook
    2. Retrieves a list of existing databases
    3. Creates 'netkeiba' database if it doesn't exist

    Uses:
        - PostgresHook with connection ID 'postgres_airflow'
        - Autocommit mode for database creation

    Returns:
        None. Prints status message indicating whether database was created
        or already existed.

    Note:
        Requires appropriate PostgreSQL permissions to create databases.
    """

    hook = PostgresHook(postgres_conn_id="postgres_airflow")
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT datname FROM pg_database;")
    databases = [row[0] for row in cursor.fetchall()]

    if "netkeiba" not in databases:
        cursor.execute("CREATE DATABASE netkeiba;")
        print("Created database 'netkeiba'")
    else:
        print("Database 'netkeiba' already exists")

with DAG(
    "postgres_setup_dag",
    default_args={"retries": 2},
    description="Setup PostgreSQL database and tables",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@once",
    catchup=False,
    tags=["database"],
) as dag:
    dag.doc_md = __doc__

    check_db = PythonOperator(
        task_id="check_database_exists",
        python_callable=check_database_exists,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_race_ids_table",
        conn_id="postgres_netkeiba",
        sql="""
            CREATE TABLE IF NOT EXISTS race_ids (
                race_id BIGSERIAL PRIMARY KEY,
                kaisai_id SERIAL,
                kaisai_date SERIAL
            );
        """
    )

    check_db >> create_table
