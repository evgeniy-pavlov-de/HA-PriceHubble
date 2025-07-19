# Company: PriceHubble
# Result: Rejected
---


# Repo Airflow DAG for Take-Home Exercise. Pavlov Evgeniy

## 1. Description
This project provides an Apache Airflow-based data pipeline to parse `.jsonl` file, filter data, and load the results into a DuckDB database. It's designed to be efficient and containerized, using Docker and Docker Compose for local development. The DAG (`duckdb__scraping__property`) automates data ingestion and transformation using Python, pyarrow, and DuckDB.

## 2. Prerequisites
1. Docker & Docker Compose installed.
2. (Optional) Python ≥ 3.8 if you plan to run parts of the pipeline outside Docker (use `requirements.txt`).

## 3. Docker Setup
In the project root (pythonProject/):
`cd <repo dir>`
```
DOCKER_COMPOSE_CONFIG_PATH=docker/docker-compose.yaml

docker compose -f ${DOCKER_COMPOSE_CONFIG_PATH} build
docker compose -f ${DOCKER_COMPOSE_CONFIG_PATH} stop
docker compose -f ${DOCKER_COMPOSE_CONFIG_PATH} up -d

docker image prune --force --filter label="com.docker.compose.project"=docker
```
These commands will:
- Build the custom Airflow image using the Dockerfile and install required Python packages.
- Start essential services including the Airflow webserver, scheduler, and metadata database.
- Mount your local `dags/` directory to the container so your DAGs are recognized.
- Ensure the logs and data folders are accessible and used by the DAGs.

## 4. Access Airflow UI
Once containers are running, visit:
📍 http://localhost:8080
Use default credentials:
   - Username: admin
   - Password: admin

## 5. Running DAG Locally
1. Go to the DAGs tab
2. Click ▶️ to trigger a manual run `duckdb__scraping__property`
3. View logs inside the UI
4. Output data (from DuckDB or scraping) will be stored in `dags/data/output/`

## 6. Example output and logs

| id       | scraping_date   | property_type   | municipality     |       price |   living_area |   price_per_square_meter |
|:---------|:----------------|:----------------|:-----------------|------------:|--------------:|-------------------------:|
| 0000a4fb | 2021-02-17      | apartment       | Solothurn        |    530000.0 |            84 |                  6309.52 |
| 000640ca | 2022-11-24      | apartment       | Volketswil       |   1573000.0 |           182 |                  8642.86 |
| 00093b3b | 2022-12-21      | house           | Meisterschwanden |   1190000.0 |           166 |                  7168.67 |
| 000c44d2 | 2023-08-07      | apartment       | Le Sentier       |    374000.0 |            62 |                  6032.26 |
| 002a329a | 2022-04-17      | house           | Roches           |    884000.0 |           589 |                  1500.85 |
| 00370535 | 2020-12-12      | apartment       | Hérémence        |    180000.0 |           143 |                  1258.74 |
| 00546d40 | 2022-05-23      | apartment       | Troistorrents    |    428000.0 |            97 |                  4412.37 |
| 005ab9e3 | 2021-02-09      | house           | Morens           |    895000.0 |           174 |                  5143.68 |
| 0064e0f8 | 2020-05-21      | house           | Muttenz          |   1730000.0 |           209 |                  8277.51 |
| 00842931 | 2020-03-08      | apartment       | Ruschein         |    560000.0 |           107 |                  5233.64 |

```
***   * /opt/airflow/logs/dag_id=duckdb__scraping__property/run_id=manual__2025-06-16T21:02:10.279482+00:00/task_id=parse_jsonl_and_write_to_duckdb_task/attempt=1.log
[2025-06-16, 21:02:11 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: duckdb__scraping__property.parse_jsonl_and_write_to_duckdb_task manual__2025-06-16T21:02:10.279482+00:00 [queued]>
[2025-06-16, 21:02:11 UTC] {taskinstance.py:1956} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: duckdb__scraping__property.parse_jsonl_and_write_to_duckdb_task manual__2025-06-16T21:02:10.279482+00:00 [queued]>
[2025-06-16, 21:02:11 UTC] {taskinstance.py:2170} INFO - Starting attempt 1 of 1
[2025-06-16, 21:02:11 UTC] {taskinstance.py:2191} INFO - Executing <Task(PythonOperator): parse_jsonl_and_write_to_duckdb_task> on 2025-06-16 21:02:10.279482+00:00
[2025-06-16, 21:02:11 UTC] {standard_task_runner.py:60} INFO - Started process 798 to run task
[2025-06-16, 21:02:11 UTC] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'duckdb__scraping__property', 'parse_jsonl_and_write_to_duckdb_task', 'manual__2025-06-16T21:02:10.279482+00:00', '--job-id', '44', '--raw', '--subdir', 'DAGS_FOLDER/duckdb__scraping__property.py', '--cfg-path', '/tmp/tmpsxwez4qc']
[2025-06-16, 21:02:11 UTC] {standard_task_runner.py:88} INFO - Job 44: Subtask parse_jsonl_and_write_to_duckdb_task
[2025-06-16, 21:02:11 UTC] {logging_mixin.py:188} WARNING - /home/***/.local/lib/python3.10/site-packages/***/settings.py:194 DeprecationWarning: The sql_alchemy_conn option in [core] has been moved to the sql_alchemy_conn option in [database] - the old setting has been used, but please update your config.
[2025-06-16, 21:02:11 UTC] {task_command.py:423} INFO - Running <TaskInstance: duckdb__scraping__property.parse_jsonl_and_write_to_duckdb_task manual__2025-06-16T21:02:10.279482+00:00 [running]> on host cd2024da3a1f
[2025-06-16, 21:02:11 UTC] {taskinstance.py:2480} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='duckdb__scraping__property' AIRFLOW_CTX_TASK_ID='parse_jsonl_and_write_to_duckdb_task' AIRFLOW_CTX_EXECUTION_DATE='2025-06-16T21:02:10.279482+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2025-06-16T21:02:10.279482+00:00'
[2025-06-16, 21:02:11 UTC] {duckdb__scraping__property.py:100} INFO - Reading data from file: /opt/airflow/dags/data/input/scraping_data.jsonl
[2025-06-16, 21:02:11 UTC] {duckdb__scraping__property.py:111} INFO - Total rows read from JSONL file: 207
[2025-06-16, 21:02:11 UTC] {duckdb__scraping__property.py:125} INFO - Total rows currently in 'properties' table before insertion: 0
[2025-06-16, 21:02:12 UTC] {duckdb__scraping__property.py:128} INFO - Rows inserted: 129
[2025-06-16, 21:02:12 UTC] {python.py:201} INFO - Done. Returned value was: None
[2025-06-16, 21:02:12 UTC] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=duckdb__scraping__property, task_id=parse_jsonl_and_write_to_duckdb_task, execution_date=20250616T210210, start_date=20250616T210211, end_date=20250616T210212
[2025-06-16, 21:02:12 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 0
[2025-06-16, 21:02:12 UTC] {taskinstance.py:3280} INFO - 0 downstream tasks scheduled from follow-on schedule check
```

## 6. Feedback

Hi Evgeniy,

Apologies for the delay in getting back to you, I was aligning with the team.

Happy to provide a more detailed feedback:

Thanks for taking the time to interview with us. You're clearly a capable engineer with strong documentation habits, clean code, and good use of tools like Docker and logging practices.

However, your experience doesn't fully align with the core tools we use (PySpark, dbt), and that showed in the technical task.

There were some issues with data modeling, Python structure, and SQL queries. Specifically, the SQL had a few problems: using a column alias in the same SELECT clause, filtering on a derived column in the WHERE clause, and a potential divide-by-zero or NULL when living_area is 0 or missing.

Overall, with our current needs and team setup, we've decided to move forward with candidates who are a closer match for our stack and ready to contribute right away.