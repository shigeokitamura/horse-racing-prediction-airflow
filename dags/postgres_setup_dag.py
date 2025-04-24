"""
PostgreSQL Database and Table Setup DAG

This DAG performs initial setup of PostgreSQL database and tables for the netkeiba project.
It handles two main tasks:

1. Database Creation:
   - Checks if 'netkeiba' database exists
   - Creates the database if it doesn't exist
   - Uses PostgresHook with 'postgres_airflow' connection

2. Table Creation:
   - Creates 'race_ids', 'race_infos', 'race_results', and 'starting_prices' table if it doesn't exist
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

import logging
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


logger = logging.getLogger(__name__)

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
        logger.info("Created database 'netkeiba'")
    else:
        logger.info("Database 'netkeiba' already exists")

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

    create_race_ids_table = SQLExecuteQueryOperator(
        task_id="create_race_ids_table",
        conn_id="postgres_netkeiba",
        sql="""
            CREATE TABLE IF NOT EXISTS race_ids (
                race_id BIGSERIAL PRIMARY KEY,
                kaisai_id SERIAL NOT NULL,
                kaisai_date SERIAL NOT NULL,
                crawled BOOLEAN DEFAULT false NOT NULL
            );
        """
    )

    create_race_infos_table = SQLExecuteQueryOperator(
        task_id="create_race_infos_table",
        conn_id="postgres_netkeiba",
        sql="""
            CREATE TABLE IF NOT EXISTS race_infos (
                race_id BIGSERIAL PRIMARY KEY,
                race_name VARCHAR(255) NOT NULL,
                race_grade VARCHAR(255),
                race_time VARCHAR(255) NOT NULL,
                course VARCHAR(255) NOT NULL,
                weather VARCHAR(255) NOT NULL,
                FOREIGN KEY (race_id) REFERENCES race_ids (race_id)
            );
            """
    )

    create_race_results_table = SQLExecuteQueryOperator(
        task_id="create_race_results_table",
        conn_id="postgres_netkeiba",
        sql="""
            CREATE TABLE IF NOT EXISTS race_results (
                result_id BIGSERIAL PRIMARY KEY,
                race_id BIGSERIAL NOT NULL,
                final_position SMALLSERIAL,
                bracket_number SMALLSERIAL,
                post_position SMALLSERIAL,
                horse_name VARCHAR(255),
                horse_id BIGSERIAL,
                age_and_sex VARCHAR(255),
                jockey_weight REAL,
                jockey_name VARCHAR(255),
                finish_time VARCHAR(255),
                margin VARCHAR(255),
                positions_at_bends VARCHAR(255),
                last_3_furlongs REAL,
                odds REAL,
                favorite SMALLSERIAL,
                horse_weight VARCHAR(255),
                trainer VARCHAR(255),
                owner VARCHAR(255),
                prize REAL,
                FOREIGN KEY (race_id) REFERENCES race_ids (race_id)
            );
            """
    )

    create_starting_prices_table = SQLExecuteQueryOperator(
        task_id="create_starting_prices_table",
        conn_id="postgres_netkeiba",
        sql="""
            CREATE TABLE IF NOT EXISTS starting_prices (
                payout_id BIGSERIAL PRIMARY KEY,
                race_id BIGSERIAL NOT NULL,
                bet_type VARCHAR(255),
                result VARCHAR(255),
                payout INTEGER,
                FOREIGN KEY (race_id) REFERENCES race_ids (race_id)
            );
            """
    )

    check_db >> create_race_ids_table >> create_race_infos_table >> create_race_results_table >> create_starting_prices_table
