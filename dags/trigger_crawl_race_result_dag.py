"""
Trigger Crawl Race Result DAG

This DAG triggers the crawl_race_result_dag for each race ID that has not been crawled yet.

The DAG:
- Runs every 5 minutes
- Has 0 retries configured for error handling
- Uses the 'postgres_netkeiba' connection ID

The DAG performs the following tasks:
- Retrieves a list of race IDs that have not been crawled yet from the database
- Triggers the crawl_race_result_dag for each race ID in the list

"""

import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from airflow.api.common.trigger_dag import trigger_dag

logger = logging.getLogger(__name__)


@dag(
    dag_id="trigger_crawl_race_result_dag",
    default_args={"retries": 0},
    description="Trigger Crawl Race Result DAG",
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    doc_md=__doc__,
    tags=["trigger"],
)
def trigger_crawl_race_result() -> None:
    @task()
    def get_target_race_ids() -> list[int]:
        """
        Get race IDs that have not been crawled yet.

        This task queries the database to get race IDs that have not been crawled yet.
        It returns a list of race IDs. The list is sorted by race ID and limited to 10 elements.

        Returns:
            list[int]: A list of race IDs
        """

        hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
        conn = hook.get_conn()
        cursor = conn.cursor()

        query = "SELECT race_id FROM race_ids WHERE crawled = false ORDER BY race_id LIMIT 10"

        cursor.execute(query)
        race_ids = [int(row[0]) for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return race_ids

    @task()
    def create_trigger_tasks(race_ids: list[int]) -> None:
        """
        Trigger the crawl_race_result_dag for each race_id in the given list.

        This task triggers the crawl_race_result_dag for each race_id in the given list.
        It uses the trigger_dag function from the airflow.api.common.trigger_dag module.
        The dag_id is set to "crawl_race_result_dag", the run_id is set to a string
        that includes the dag_run.run_id and the race_id, and the conf is set to
        a dict with the race_id and dry_run set to True.

        Args:
            race_ids (list[int]): A list of race IDs

        Returns:
            None
        """
        context = get_current_context()
        dag_run = context["dag_run"]

        for race_id in race_ids:
            trigger_dag(
                dag_id="crawl_race_result_dag",
                run_id=f"triggered_by_{dag_run.run_id}_{race_id}",
                conf={"race_id": race_id, "dry_run": False},
                replace_microseconds=False,
            )
            logger.info(f"Triggered DAG for race_id: {race_id}")

    race_ids = get_target_race_ids()
    create_trigger_tasks(race_ids)


trigger_crawl_race_result()
