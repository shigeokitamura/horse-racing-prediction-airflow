"""
Crawl Past Race IDs DAG

This DAG crawls the race IDs of past races from netkeiba.com and saves them to the database.

The DAG:
- Runs once and doesn't perform catchup
- Has 2 retries configured for error handling
- Uses the 'postgres_netkeiba' connection ID

The DAG takes year and month as parameters and crawls the race IDs of past races
in the specified year and month. It first checks the parameters and then
requests the calendar list page and extracts the links to the race list pages.
After that, it requests each race list page and extracts the links to the race
result pages. Finally, it saves the race IDs to the database.

Example response:
    [
        {'race_id': 202301010101, 'kaisai_id': 20230101, 'kaisai_date': 20230101},
        {'race_id': 202301010102, 'kaisai_id': 20230101, 'kaisai_date': 20230101},
        {'race_id': 202301010103, 'kaisai_id': 20230101, 'kaisai_date': 20230101},
        ...
    ]
"""

import logging
from airflow import XComArg
import requests
import pendulum
from datetime import datetime
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


@dag(
    "crawl_past_race_ids_dag",
    concurrency=1,
    default_args={"retries": 2},
    description="Crawl past race_ids",
    schedule_interval="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    doc_md=__doc__,
    params={
        "year": Param(
            type="integer",
            default=2008,
            minimum=2008,
            maximum=datetime.now().year,
            description="Year (2008-current)",
        ),
        "month": Param(
            type="integer", default=1, minimum=1, maximum=12, description="Month (1-12)"
        ),
        "dry_run": Param(type="boolean", default=True, description="Dry run"),
    },
    tags=["crawl"],
)
def crawl_past_race_ids() -> None:
    @task()
    def check_params() -> dict[str, int]:
        """
        Check the year and month parameters and return them as a dict.

        Checks the value of the year and month parameters, and if they are valid,
        returns them as a dict. If they are invalid, raises an AirflowException.

        Args:
            None

        Returns:
            dict[str, int]: A dict containing the year and month as int.
        """

        context = get_current_context()
        year = context["dag_run"].conf.get("year")
        month = context["dag_run"].conf.get("month")

        if year and month:
            return {"year": int(year), "month": int(month)}
        else:
            raise AirflowException("Params are invalid.")

    @task()
    def get_calendar_list(target: XComArg) -> list[dict[str, int]]:
        """
        Get calendar list.

        Requests the calendar list page and extracts the links to the race list pages.
        Returns a list of dicts containing the kaisai_id and kaisai_date.

        Args:
            target (XComArg): XComArg containing the year and month.

        Returns:
            list[dict[str, int]]: List of dicts containing the kaisai_id and kaisai_date.
        """

        year: int = target["year"]
        month: int = target["month"]

        url = (
            f"https://en.netkeiba.com/race/race_calendar.html?year={year}&month={month}"
        )
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        race_calendar_lists = soup.find("div", class_="Race_Calendar_List")
        if type(race_calendar_lists) is not Tag:
            raise AirflowException("Could not find Race_Calendar_List.")

        links: ResultSet[Tag] = race_calendar_lists.find_all("a")

        logger.info(f"Found {len(links)} links.")

        results = []

        for link in links:
            href: str = link.get("href")
            logger.info(href)

            variables = href.split("?")[1].split("&")
            kaisai_id = int(variables[0].split("=")[1])
            kaisai_date = int(variables[1].split("=")[1])

            results.append(
                {
                    "kaisai_id": kaisai_id,
                    "kaisai_date": kaisai_date,
                }
            )

        return results

    @task_group(group_id="get_race_ids_and_save")
    def get_race_ids_and_save(target: XComArg) -> None:
        """
        Task group to get race IDs and save them to the database.

        Args:
            target (XComArg): XComArg containing the year and month.

        Returns:
            None
        """

        @task()
        def get_race_ids(target: dict[str, int]) -> dict[str, list[int] | int]:
            """
            Get race IDs.

            Requests the race list page and extracts the links to the race result pages.
            Returns a dict containing the race IDs and the kaisai_id and kaisai_date.

            Args:
                target (dict[str, int]): dict containing the year and month.

            Returns:
                dict[str, list[int] | int]: Dict containing the race IDs and the kaisai_id and kaisai_date.
            """

            kaisai_id: int = target["kaisai_id"]
            kaisai_date: int = target["kaisai_date"]

            url = f"https://en.netkeiba.com/race/race_list.html?kaisai_id={kaisai_id}&kaisai_date={kaisai_date}"

            response = requests.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            race_lists = soup.find("ul", class_="RaceList")

            if type(race_lists) is not Tag:
                raise AirflowException("Could not find Race_Calendar_List.")

            links: ResultSet[Tag] = race_lists.find_all(
                "div", class_="RaceList_Main_Box"
            )
            logger.info(f"Found {len(links)} links.")

            result: dict[str, list[int] | int] = {
                "race_ids": [],
                "kaisai_id": kaisai_id,
                "kaisai_date": kaisai_date,
            }

            for link in links:
                href: str = link.find("a").get("href")
                logger.info(href)

                race_id = int(href.split("?")[1].split("=")[1])
                result["race_ids"].append(race_id)

            return result

        @task()
        def save_race_ids(target: dict[str, list[int] | int]) -> None:
            """
            Save race IDs to the database.

            Args:
                target (dict[str, list[int] | int]): dict containing the race IDs and the kaisai_id and kaisai_date.

            Returns:
                None
            """

            context = get_current_context()
            dry_run: bool = context["dag_run"].conf.get("dry_run")

            race_ids: list[int] = target["race_ids"]
            kaisai_id: int = target["kaisai_id"]
            kaisai_date: int = target["kaisai_date"]

            hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
            conn = hook.get_conn()
            conn.autocommit = True
            cursor = conn.cursor()

            for race_id in race_ids:
                query = f"""
                    INSERT INTO
                        race_ids (race_id, kaisai_id, kaisai_date)
                    VALUES
                        ({race_id}, {kaisai_id}, {kaisai_date})
                    ON CONFLICT (race_id) DO NOTHING
                """

                logger.info(query)
                if dry_run:
                    logger.info("DRY RUN")
                else:
                    cursor.execute(query)

        save_race_ids(target=get_race_ids(target=target))

    target_month = check_params()
    calendars = get_calendar_list(target=target_month)
    get_race_ids_and_save.expand(target=calendars)


crawl_past_race_ids()
