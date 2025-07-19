"""
ETL real estate data.
Reads raw jsonl files, filters values, transforms columns and writes to final propeties table in DuckDB.

Comment for test case.
For data transformation, I see these basic options:
1. Pandas. Fast, easy to run into excessive memory consumption. Good for small datasets. Easy to do complex
    transformations, which is not valid in this case.
2. Low-level python and list handling. Easy to understand the essence of the code. No problems with non-obvious resource
    consumption like Pandas. May be slower.
3. Pyarrow. I like the code visually better than with pandas :). According to my tests it was 20% faster than pandas for
    both "a lot of small files" and "large files". Works well with duckdb (reusing efficient zero-copy filters:
    https://duckdb.org/2021/12/03/duck-arrow.html#benchmark-comparison).
4. Spark and other kinds of Python APIs for DuckDB. Can be fast, but need to test. Probably too mach for a test case.

The option 3 I chose have multiple syntax options to choose from. I chose to stop completely with sql query as it's easy
 to read (all in one place), easy to assume a query tree, easy to port to DBT-like configs. Generally convenient for
 small pipelines as it is fast to implement.
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging

import duckdb
import pyarrow as pa
import pyarrow.json
from airflow import DAG
from airflow.operators.python import PythonOperator

# Comment for test case.
# Write out all constants so that they are easy to find when opening the code.
# In the prod, the path pattern to the file (or directory) will be printed as a constant. The file retrieval itself will
#   be most likely a separate function.
RAW_DATA_DIR = "data/input/scraping_data.jsonl"
DUCKDB_FILE = "data/output/data.duckdb"

# Comment for test case.
# I don't use the schema as a dictionary and I don't check the existing schema for type matching via PRAGMA.
# Since in a typical scenario the receiving table never or very rarely changes.
# If the situation is the opposite, and you need active control over the table schema, then you need to write a separate
#   schema evolution module for this, which is excessive in this case.
CREATE_DUCKDB_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS properties (
    id TEXT NOT NULL,  -- Property ID
    scraping_date TEXT NOT NULL,  -- Date when the data was scraped
    property_type TEXT NOT NULL,  -- Type of property
    municipality TEXT NOT NULL,  -- Municipality of the property
    price DOUBLE NOT NULL,  -- Converted price in numeric format
    living_area DOUBLE NOT NULL,  -- Area of the property
    price_per_square_meter DOUBLE NOT NULL  -- Price per square meter
);
"""

# Comment for test case.
# Description of filtering method selection in the file header.
INSERT_DUCKDB_TABLE_QUERY = f"""
INSERT INTO properties
SELECT 
    id,
    scraping_date,
    property_type,
    municipality,
    TRY_CAST(REPLACE(SPLIT_PART(raw_price, '€', 1), ' ', '') AS DOUBLE) AS price,
    living_area,
    ROUND(
        price / living_area,
        2
    ) AS price_per_square_meter
FROM arrow_table
WHERE 
    property_type IN ('apartment', 'house') AND
    price_per_square_meter BETWEEN 500 AND 15000 AND
    scraping_date > '2020-03-05';
"""

# Comment for test case.
# In such a situation, I stick to strict type validation.
# The disadvantage is that when the structure of incoming data changes, the dag will drop.
# The plus is that this itself serves as a guarantee of correct processing and recording of values at later stages.
PA_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False, metadata={"description": "Unique ID of the property"}),
    pa.field("raw_price", pa.string(), metadata={"description": "Price info (e.g., “530 000€/mo.”)"}),
    pa.field("living_area", pa.float64(), metadata={"description": "The area of the property in square meters"}),
    pa.field("property_type", pa.string(), metadata={"description": "Type of property (e.g., house, studio)"}),
    pa.field("municipality", pa.string(), metadata={"description": "City or town where the property is located"}),
    pa.field("scraping_date", pa.string(), metadata={"description": "Date the data was scraped (YYYY-MM-DD)"})
])


def parse_jsonl_and_write_to_duckdb() -> None:
    """
    Parses JSONL data from a file using PyArrow with a predefined schema, then writes the processed data into a DuckDB
     database after filtering and transforming it according to business logic.
    """
    input_file_path = Path(__file__).parent.joinpath(RAW_DATA_DIR)
    output_file_path = Path(__file__).parent.joinpath(DUCKDB_FILE)

    with open(input_file_path, "rb") as f:
        logging.info("Reading data from file: %s", input_file_path)
        # Comment for test case.
        # Several approaches can be used for parsing jsonl:
        # 1. Read line-by-line using libraries for json files. RAM-safe, single thread.
        # 2. Iteration by line-by-line. Same as item. 1, memory utilization is even better.
        # 3. Reading the entire file into memory. Can be risky on small VMs if non-deterministic input file size. Fast.
        # 4. Multithreaded string reading. Very fast. Requires controlling the number of threads so as not to overflow
        #   memory. Over-engineering in that case.
        # I chose option 3 because there are no memory risks at the proposed file size. Also, the data format doesn't
        # look like it will be gigabyte+ single files (since the amount of real estate is limited).
        pa_table = pyarrow.json.read_json(f, parse_options=pyarrow.json.ParseOptions(explicit_schema=PA_SCHEMA))
        logging.info("Total rows read from JSONL file: %d", pa_table.num_rows)
    with duckdb.connect(output_file_path) as duckdb_conn:

        # Comment for test case.
        # In such an etl process, it may make sense to log the number of rows in the source file, the number of rows
        #   inserted into the final table.
        # This is more important if the table is written incrementally rather than being overwritten anew (assumed this
        #   from the job condition).
        # There could also be other metrics, including about the composition of the data.
        # Such logging requires additional calculations (e.g. pyarrow table reads with and without filters). Therefore,
        #   it makes sense to use it for important or moderately important processes.
        duckdb_conn.execute(CREATE_DUCKDB_TABLE_QUERY)
        duckdb_conn.register("arrow_table", pa_table)
        row_count_before_insert = duckdb_conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        logging.info("Total rows currently in 'properties' table before insertion: %d", row_count_before_insert)
        duckdb_conn.execute(INSERT_DUCKDB_TABLE_QUERY)
        row_count_after_insert = duckdb_conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        logging.info("Rows inserted: %d", row_count_after_insert - row_count_before_insert)


with DAG(
        # default random params
        dag_id="duckdb__scraping__property",
        start_date=datetime(2025, 6, 16),
        schedule_interval="@daily",
        catchup=False,
) as dag:

    # Comment for test case.
    # PythonOperator for convenience.
    # Doing the whole etl in one step, because at this size of task it would be over-engineering otherwise.
    parse_task = PythonOperator(
        task_id="parse_jsonl_and_write_to_duckdb_task",
        python_callable=parse_jsonl_and_write_to_duckdb,
    )
