# horse-racing-prediction-airflow
Data pipeline to ETL data from netkeiba.com

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
