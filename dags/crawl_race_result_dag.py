"""
Crawl Race Result DAG

This DAG crawls race results from netkeiba.com.

The DAG:
- Runs once and doesn't perform catchup
- Has 2 retries configured for error handling
- Uses the 'postgres_netkeiba' connection ID

The DAG takes race_id as a parameter and crawls the race result page.
It extracts the race results from the page and saves them to the database.

Example response:
    [
        {'race_id': 202301010101, 'final_position': 1, 'bracket_number': 1, 'post_position': 1, 'horse_name': '', 'horse_id': 123, 'age_and_sex': '3 ', 'jockey_weight': 54.0, 'jockey_name': ' ', 'finish_time': '1:38.6', 'margin': '0.1', 'positions_at_bends': '1-1-1-1', 'last_3_furlongs': 35.3, 'odds': 2.9, 'favorite': 1, 'horse_weight': '512(', 'trainer': ' ', 'owner': ' ', 'prize': 54000000},
        {'race_id': 202301010101, 'final_position': 2, 'bracket_number': 2, 'post_position': 2, 'horse_name': '', 'horse_id': 124, 'age_and_sex': '4 ', 'jockey_weight': 54.0, 'jockey_name': ' ', 'finish_time': '1:38.7', 'margin': '0.1', 'positions_at_bends': '2-2-2-2', 'last_3_furlongs': 35.4, 'odds': 3.1, 'favorite': 2, 'horse_weight': '515(', 'trainer': ' ', 'owner': ' ', 'prize': 21600000},
        {'race_id': 202301010101, 'final_position': 3, 'bracket_number': 3, 'post_position': 3, 'horse_name': '', 'horse_id': 125, 'age_and_sex': '3 ', 'jockey_weight': 54.0, 'jockey_name': ' ', 'finish_time': '1:39.1', 'margin': '0.4', 'positions_at_bends': '3-3-3-3', 'last_3_furlongs': 35.7, 'odds': 4.1, 'favorite': 3, 'horse_weight': '518(', 'trainer': ' ', 'owner': ' ', 'prize': 10800000},
        {'race_id': 202301010101, 'final_position': 4, 'bracket_number': 4, 'post_position': 4, 'horse_name': '', 'horse_id': 126, 'age_and_sex': '4 ', 'jockey_weight': 54.0, 'jockey_name': ' ', 'finish_time': '1:39.4', 'margin': '0.3', 'positions_at_bends': '4-4-4-4', 'last_3_furlongs': 36.0, 'odds': 5.8, 'favorite': 4, 'horse_weight': '513(', 'trainer': ' ', 'owner': ' ', 'prize': 5400000},
        ...
    ]
"""

import requests
import logging
import pendulum
import pandas as pd
import re
from io import StringIO
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.exceptions import AirflowException
from sqlalchemy.engine.base import Engine


logger = logging.getLogger(__name__)

@dag(
    dag_id="crawl_race_result_dag",
    concurrency=1,
    default_args={"retries": 0},
    description="Crawl race result",
    schedule_interval="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    params={
        "race_id": Param(
            type="integer",
            description="The race_id to crawl",
            default=200801010101,
        ),
        "dry_run": Param(type="boolean", default=True, description="Dry run"),
    },
    tags=["crawl"],
)

def crawl_race_result() -> None:
    @task()
    def check_already_crawled() -> int:
        """
        Check if the race result is already crawled.

        Check the 'crawled' field in the 'race_ids' table for the given race_id.
        If the field is true, raise an AirflowException.

        Args:
            None

        Returns:
            int: The race_id
        """

        context = get_current_context()
        race_id: int = context["dag_run"].conf.get("race_id")

        hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
        conn = hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()

        query = f"SELECT crawled FROM race_ids WHERE race_id = {race_id}"

        cursor.execute(query)
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        if bool(result[0]):
            raise AirflowException(f"Race result already crawled for race_id: {race_id}")

        return race_id

    @task(retries=2)
    def check_race_result_exists(race_id: int) -> dict[str, str]:
        """
        Check if the race result exists.

        Requests the race result page and checks if the race header, result table, and starting prices are present.
        If the race result is not found, raises an AirflowException.

        Args:
            race_id (int): The race_id to check.

        Returns:
            dict[str, str]: A dict containing the race_id, race header, result table, and starting prices.
        """

        url = f"https://en.netkeiba.com/db/race/{race_id}"

        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        race_header = soup.find("div", class_="RaceHeader_Value")
        result_table = soup.find("table", class_="ResultsByRaceDetail")
        starting_prices = soup.find("div", class_="Result_Pay_Back").find("table")

        if not isinstance(race_header, Tag) or not isinstance(result_table, Tag) or not isinstance(starting_prices, Tag):
            raise AirflowException(f"Race result not found for race_id: {race_id}")

        return {
            "race_id": race_id,
            "race_header": str(race_header),
            "result_table": str(result_table),
            "starting_prices": str(starting_prices),
        }

    @task()
    def crawl_race_header(race_id: int, race_header: str) -> dict[str, int | str | None]:
        """
        Crawl race header.

        Extracts the race name, grade, time, course, and weather from the race header.

        Args:
            race_id (int): The race_id to crawl.
            race_header (str): The race header HTML.

        Returns:
            dict[str, int | str | None]: A dict containing the race_id, race_name, race_grade, race_time, course, and weather.
        """

        race_header: Tag = BeautifulSoup(race_header, "html.parser")

        race_names: ResultSet[Tag] = race_header.find("h2", class_="RaceName").find_all("span")
        race_name: str = race_names[0].text
        race_grade: str | None = race_names[1].text if len(race_names) > 1 else None

        race_data: ResultSet[Tag] = race_header.find("div", class_="RaceData").find_all("span")
        race_time: str = race_data[0].text
        course: str = race_data[1].text
        weather: str = race_data[-1].text

        return {
            "race_id": race_id,
            "race_name": race_name,
            "race_grade": race_grade,
            "race_time": race_time,
            "course": course,
            "weather": weather,
        }

    @task()
    def crawl_result_table(race_id: int, result_table: str) -> pd.DataFrame:
        """
        Crawl result table.

        Parses the HTML content of a race result table to extract detailed race results
        and returns them as a pandas DataFrame. The DataFrame includes fields such as
        final position, bracket number, post position, horse name, age and sex, jockey weight,
        jockey name, finish time, margin, positions at bends, last 3 furlongs, odds,
        favorite, horse weight, trainer, owner, prize, and horse ID.

        Args:
            race_id (int): The race ID to which the result table belongs.
            result_table (str): The HTML content of the result table.

        Returns:
            pd.DataFrame: A DataFrame containing the parsed race results.

        Raises:
            AirflowException: If the result table is empty.
        """

        df = pd.read_html(StringIO(result_table))[0]

        if len(df) == 0:
            raise AirflowException(f"Result table not found for race_id: {race_id}")

        df.columns = [
            "final_position",
            "bracket_number",
            "post_position",
            "horse_name",
            "age_and_sex",
            "jockey_weight",
            "jockey_name",
            "finish_time",
            "margin",
            "positions_at_bends",
            "last_3_furlongs",
            "odds",
            "favorite",
            "horse_weight",
            "trainer",
            "owner",
            "prize",
        ]
        df["final_position"] = pd.to_numeric(df["final_position"], errors="coerce").astype(pd.Int64Dtype())

        result_table: Tag = BeautifulSoup(result_table, "html.parser")

        horses = result_table.find_all("a", class_ = "text_horse_name")
        horse_ids = []
        for horse in horses:
            horse_id = re.findall(r"\d+", horse.get("href"))[0]
            horse_ids.append(int(horse_id))
        df["horse_id"] = horse_ids

        df["race_id"] = [race_id] * len(df)
        df["result_id"] = df.apply(lambda x: f"{x['race_id']}{x['post_position']:02}", axis=1)
        df = df.dropna(subset=["final_position"])

        return df

    @task()
    def crawl_starting_prices(race_id: int, starting_prices: str) -> pd.DataFrame:
        """
        Crawl starting prices.

        Extracts the starting prices (odds) of a race from the race result page.

        Args:
            race_id (int): The race ID to which the starting prices belong.
            starting_prices (str): The HTML content of the starting prices table.

        Returns:
            pd.DataFrame: A DataFrame containing the parsed starting prices.

        Raises:
            AirflowException: If the starting prices table is empty.
        """

        starting_prices: Tag = BeautifulSoup(starting_prices, "html.parser")

        index = 1
        data = []
        tbodies: ResultSet[Tag] = starting_prices.find_all("tbody")
        for tbody in tbodies:
            bet_type: str = tbody.find("th").text.strip()

            trs: ResultSet[Tag] = tbody.find_all("tr")
            for tr in trs:
                result: str = tr.find("td", class_="Result").text.strip()
                payout: str = tr.find("td", class_="Payout").text.strip().replace("￥", "").replace(",", "")

                try:
                    payout = int(payout)
                except ValueError:
                    payout = None

                data.append([f"{race_id}{index:02}", race_id, bet_type, result, payout])
                index += 1

        return pd.DataFrame(data, columns=["payout_id", "race_id", "bet_type", "result", "payout"])

    @task()
    def check_crawled(race_info: dict[str, int | str | None], result_table: pd.DataFrame, starting_prices: pd.DataFrame) -> dict[str, dict[str, int | str | None] | pd.DataFrame]:
        """
        Check if race info, result table, and starting prices are crawled.

        Raises AirflowException if any of race info, result table, or starting prices are not found.

        Args:
            race_info (dict[str, int | str | None]): The race info to check.
            result_table (pd.DataFrame): The result table to check.
            starting_prices (pd.DataFrame): The starting prices to check.

        Returns:
            dict[str, dict[str, int | str | None] | pd.DataFrame]: A dict containing the race info, result table, and starting prices.
        """

        if not isinstance(race_info, dict):
            raise AirflowException("Race info not found")
        if len(result_table) == 0:
            raise AirflowException("Result table not found")
        if len(starting_prices) == 0:
            raise AirflowException("Starting prices not found")

        return {
            "race_info": race_info,
            "result_table": result_table,
            "starting_prices": starting_prices,
        }

    @task.skip_if(lambda context: context["dag_run"].conf.get("dry_run"))
    @task()
    def save_race_info(race_info: dict[str, int | str | None]) -> bool:
        """
        Save race info to the database.

        Inserts or updates race info in the race_infos table.

        Args:
            race_info (dict[str, int | str | None]): The race info to save.

        Returns:
            bool: True if the race info is saved successfully.

        Raises:
            AirflowException: If the race info is not a dict.
        """

        hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
        conn = hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()

        query = """
            INSERT INTO
                race_infos (race_id, race_name, race_grade, race_time, course, weather)
            VALUES
                (%(race_id)s, %(race_name)s, %(race_grade)s, %(race_time)s, %(course)s, %(weather)s)
            ON CONFLICT (race_id)
            DO UPDATE SET
                race_name = %(race_name)s,
                race_grade = %(race_grade)s,
                race_time = %(race_time)s,
                course = %(course)s,
                weather = %(weather)s
        """

        cursor.execute(query, race_info)

        cursor.close()
        conn.close()

        return True

    @task.skip_if(lambda context: context["dag_run"].conf.get("dry_run"))
    @task()
    def save_result_table(result_table: pd.DataFrame) -> bool:
        """
        Save the result table to the database.

        Appends the result table to the race_results table in the database.

        Args:
            result_table (pd.DataFrame): The result table to save.

        Returns:
            bool: True if the result table is saved successfully.
        """

        hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
        engine: Engine = hook.get_sqlalchemy_engine()
        result_table.to_sql("race_results", engine, if_exists="replace", index=False)

        return True

    @task.skip_if(lambda context: context["dag_run"].conf.get("dry_run"))
    @task()
    def save_starting_prices(starting_prices: pd.DataFrame) -> bool:
        """
        Save the starting prices to the database.

        Appends the starting prices to the starting_prices table in the database.

        Args:
            starting_prices (pd.DataFrame): The starting prices to save.

        Returns:
            bool: True if the starting prices are saved successfully.
        """

        hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
        engine: Engine = hook.get_sqlalchemy_engine()
        starting_prices.to_sql("starting_prices", engine, if_exists="replace", index=False)

        return True

    @task.skip_if(lambda context: context["dag_run"].conf.get("dry_run"))
    @task()
    def set_crawled(race_info: bool, result_table: bool, starting_prices: bool) -> None:
        """
        Set the crawled flag in the race_ids table to True for the given race_id.

        Args:
            race_info (bool): Whether the race info is crawled or not.
            result_table (bool): Whether the result table is crawled or not.
            starting_prices (bool): Whether the starting prices are crawled or not.

        Raises:
            AirflowException: If the race info, result table, or starting prices are not crawled.
        """

        if not race_info or not result_table or not starting_prices:
            raise AirflowException("Crawling failed")

        context = get_current_context()
        race_id: int = context["dag_run"].conf.get("race_id")

        query = f"UPDATE race_ids SET crawled = true WHERE race_id = {race_id}"

        hook = PostgresHook(postgres_conn_id="postgres_netkeiba")
        conn = hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(query)

        cursor.close()
        conn.close()

    race_id = check_already_crawled()
    race_result = check_race_result_exists(race_id)

    race_info = crawl_race_header(race_result["race_id"], race_result["race_header"])
    result_table = crawl_result_table(race_result["race_id"], race_result["result_table"])
    starting_prices = crawl_starting_prices(race_result["race_id"], race_result["starting_prices"])

    crawled_data = check_crawled(race_info, result_table, starting_prices)

    is_saved_race_info = save_race_info(crawled_data["race_info"])
    is_saved_result_table = save_result_table(crawled_data["result_table"])
    is_saved_starting_prices = save_starting_prices(crawled_data["starting_prices"])

    set_crawled(is_saved_race_info, is_saved_result_table, is_saved_starting_prices)

crawl_race_result()
