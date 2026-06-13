import json
import os
import time
from datetime import date
from pathlib import Path

import dotenv
import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

dotenv.load_dotenv()

Polygon_API_KEY = os.getenv("POLYGON_API_KEY")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

LIMIT = 1000
SCHEMA_PATH = Path(__file__).parent / "schemas" / "stock_tickers_schema.json"


def load_table_schema(schema_path: Path = SCHEMA_PATH) -> dict:
    with open(schema_path, encoding="utf-8") as file:
        return json.load(file)


def _coerce_value(value, column: dict):
    snowflake_type = column["snowflake_type"]

    if value is None or (isinstance(value, str) and not value.strip()):
        if column.get("empty_string_to_null"):
            return None
        return column.get("default")

    if snowflake_type == "VARCHAR":
        return str(value).strip() or (
            None if column.get("empty_string_to_null") else str(value)
        )

    if snowflake_type == "BOOLEAN":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ("true", "1", "yes"):
                return True
            if normalized in ("false", "0", "no"):
                return False
        return None

    if snowflake_type == "TIMESTAMP_NTZ":
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.tz_convert("UTC").tz_localize(None)

    if snowflake_type.upper() == "DATE":
        if isinstance(value, date):
            return value
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()

    return value


def transform_ticker_record(
    record: dict, schema: dict, run_date: date | None = None
) -> dict | None:
    transformed = {}

    for column in schema["columns"]:
        generated = column.get("generated")
        if generated == "run_date":
            transformed[column["name"]] = run_date
            continue

        source_field = column["source_field"]
        raw_value = record.get(source_field, column.get("default"))

        if column.get("required") and (
            raw_value is None or (isinstance(raw_value, str) and not raw_value.strip())
        ):
            return None

        try:
            transformed[column["name"]] = _coerce_value(raw_value, column)
        except (TypeError, ValueError):
            return None

    return transformed


def transform_tickers_for_snowflake(
    raw_tickers: list[dict], schema_path: Path = SCHEMA_PATH
) -> pd.DataFrame:
    schema = load_table_schema(schema_path)
    run_date = date.today()
    transformed_rows = []
    skipped = 0

    for record in raw_tickers:
        row = transform_ticker_record(record, schema, run_date=run_date)
        if row is None:
            skipped += 1
            continue
        transformed_rows.append(row)

    if skipped:
        print(f"Skipped {skipped} invalid ticker records during ETL")

    if not transformed_rows:
        raise ValueError("ETL produced no valid rows to load into Snowflake")

    column_order = [column["name"] for column in schema["columns"]]
    return pd.DataFrame(transformed_rows, columns=column_order)


def upload_to_snowflake(tickers: list[dict]) -> None:
    schema = load_table_schema()
    table_name = schema["table"]["name"]
    df = transform_tickers_for_snowflake(tickers)

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    try:
        success, _, nrows, _ = write_pandas(
            conn,
            df,
            table_name,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
            use_logical_type=True,
        )
        if not success:
            raise RuntimeError("Failed to write tickers to Snowflake")
        print(f"Inserted {nrows} rows into {table_name} with DS={date.today().isoformat()}")
    finally:
        conn.close()


def run_stock_job():
    url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={Polygon_API_KEY}"
    response = requests.get(url)
    step = 1
    data = response.json()
    tickers = []
    for ticker in data["results"]:
        tickers.append(ticker)

    while "next_url" in data:
        url = data["next_url"] + f"&apikey={Polygon_API_KEY}"
        response = requests.get(url)
        if "error" in response.json().keys():
            print(response.json()["error"])
            time.sleep(60)
            continue
        data = response.json()
        tickers.extend([ticker for ticker in data["results"]])
        print(step)
        step += 1
    print(step)
    print(len(tickers))

    upload_to_snowflake(tickers)


if __name__ == "__main__":
    run_stock_job()
