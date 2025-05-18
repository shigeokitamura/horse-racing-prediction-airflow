# horse-racing-prediction-airflow
Data pipeline to ETL data from netkeiba.com

![Image](https://github.com/user-attachments/assets/524e6e65-9d2b-403b-9a40-625f8795e99d)

## Initialize

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```
docker compose up airflow-init
```

## Run

```
docker compose up
```

## Connect to Database

```
docker exec -it horse-racing-prediction-airflow-postgres-1 /bin/bash
psql -U airflow
```

## How to Use
1. Unpause `postgres_setup_dag` and Trigger the dag. Tables are created in PostgreSQL.
2. Unpause dags tagged `crawl` (`crawl_past_race_ids_dag`, `crawl_race_card_dag`, `crawl_race_result_dag`).
3. Unpause dags tagged `trigger` (`trigger_crawl_past_race_ids_dag`, `trigger_crawl_race_result_dag`).
4. Wait until the crawling is finished.
