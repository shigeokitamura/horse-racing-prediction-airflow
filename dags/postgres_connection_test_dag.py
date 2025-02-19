"""
PostgreSQL Connection Test DAG

This DAG tests the connection to a PostgreSQL database using Airflow's PostgresHook.
It verifies the connection by retrieving database version and connection information,
making it useful for validating database connectivity in an Airflow environment.

The DAG:
- Runs once and doesn't perform catchup
- Has 2 retries configured for error handling
- Uses the 'postgres_airflow' connection ID
"""

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


def test_postgres_connection():
    """
    Tests PostgreSQL database connection and retrieves connection information.

    This function:
    1. Establishes a connection to PostgreSQL using PostgresHook
    2. Retrieves and prints the database version
    3. Fetches and displays current connection details including:
       - Database name
       - Connected user
       - Server address
       - Server port

    Returns:
        str: Success message if connection test passes

    Raises:
        AirflowException: If connection fails or any database operations encounter errors

    Example response:
        PostgreSQL Version: PostgreSQL 13.18
        Current Database: ***
        Current User: ***
        Server Address: 172.20.0.2
        Server Port: 5432
    """

    try:
        hook = PostgresHook(postgres_conn_id="postgres_airflow")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]

        cursor.execute("""
            SELECT
                current_database() AS database,
                current_user AS user,
                inet_server_addr() AS server_address,
                inet_server_port() AS server_port;
        """)
        db_info = cursor.fetchone()

        print(f"PostgreSQL Version: {version}")
        print(f"Current Database: {db_info[0]}")
        print(f"Current User: {db_info[1]}")
        print(f"Server Address: {db_info[2]}")
        print(f"Server Port: {db_info[3]}")

        cursor.close()
        conn.close()

        return "Connection test successful!"

    except Exception as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        raise AirflowException(f"PostgreSQL connection test failed: {str(e)}")


with DAG(
    "postgres_connection_test_dag",
    default_args={"retries": 2},
    description="Test PostgreSQL connection",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@once",
    catchup=False,
    tags=["database"],
) as dag:
    dag.doc_md = __doc__

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
    )

    test_connection
