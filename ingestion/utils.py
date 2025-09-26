from __future__ import annotations
import os
import uuid
from pathlib import Path
from typing import List, Tuple
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# --- Always load .env from the project root (one level up from ingestion/) ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)


def _require_env(keys: list[str]) -> None:
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        raise RuntimeError(
            f"Missing env vars: {', '.join(missing)}. "
            f"Ensure they are set in {PROJECT_ROOT / '.env'}"
        )


def get_conn():
    """
    Returns an open Snowflake connection.
    We do not force a database/schema here,
    because ensure_tables() will create/select them explicitly.
    """
    _require_env([
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ])
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        client_session_keep_alive=True,
    )
    return conn


def ensure_tables(conn):
    """
    Creates the warehouse, database, schemas, and required tables if they don't exist.
    Sets the current DB and schema context.
    """
    cur = conn.cursor()
    db = os.environ["SNOWFLAKE_DATABASE"]
    wh = os.environ["SNOWFLAKE_WAREHOUSE"]
    role = os.environ["SNOWFLAKE_ROLE"]
    try:
        # Role and warehouse
        cur.execute(f"USE ROLE {role}")
        cur.execute(
            f"CREATE WAREHOUSE IF NOT EXISTS {wh} "
            "WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE"
        )
        cur.execute(f"USE WAREHOUSE {wh}")

        # Database and schemas
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cur.execute(f"USE DATABASE {db}")
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")
        cur.execute("CREATE SCHEMA IF NOT EXISTS STAGING")
        cur.execute("CREATE SCHEMA IF NOT EXISTS MARTS")
        cur.execute("CREATE SCHEMA IF NOT EXISTS OPS")
        cur.execute("USE SCHEMA RAW")

        # Tables
        cur.execute("""
            create table if not exists RAW.CUSTOMERS (
              customer_id string primary key,
              full_name string,
              email string,
              phone string,
              city string,
              created_at timestamp_ntz,
              updated_at timestamp_ntz
            );
        """)
        cur.execute("""
            create table if not exists RAW.WORKERS (
              worker_id string primary key,
              worker_name string,
              worker_type string,
              city string,
              is_active boolean,
              created_at timestamp_ntz,
              updated_at timestamp_ntz
            );
        """)
        cur.execute("""
            create table if not exists RAW.BOOKINGS (
              booking_id string primary key,
              customer_id string,
              worker_id string,
              city string,
              channel string,
              status string,
              price number(10,2),
              requested_at timestamp_ntz,
              assigned_at timestamp_ntz,
              completed_at timestamp_ntz,
              canceled_at timestamp_ntz,
              updated_at timestamp_ntz
            );
        """)
        cur.execute("""
            create table if not exists RAW.WEATHER (
              city string,
              date date,
              temp_max float,
              temp_min float,
              precipitation float,
              windspeed_max float,
              updated_at timestamp_ntz,
              primary key (city, date)
            );
        """)
        cur.execute("""
            create table if not exists OPS.INGESTION_WATERMARKS (
              source_name string primary key,
              last_updated_at timestamp_ntz
            );
        """)
    finally:
        cur.close()


def _parse_table_identifier(identifier: str) -> Tuple[str | None, str | None, str]:
    """
    Accepts 'DB.SCHEMA.TABLE' or 'SCHEMA.TABLE' or 'TABLE'.
    Returns (database, schema, table_name)
    """
    parts = identifier.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 1:
        return None, None, parts[0]
    raise ValueError(f"Invalid table identifier: {identifier}")


def merge_upsert(
    conn,
    target_table: str,
    df: pd.DataFrame,
    key_columns: List[str],
    updated_col: str | None = "updated_at"
):
    """
    Upsert a DataFrame into Snowflake target_table using MERGE.

    - Idempotent: updates only when S.updated_at >= T.updated_at (if updated_col provided)
    - Accepts target_table as 'RAW.TABLE' or 'DB.SCHEMA.TABLE'
    """
    if df is None or df.empty:
        return

    df = df.copy()
    df.columns = [str(c).upper() for c in df.columns]

    env_db = os.environ["SNOWFLAKE_DATABASE"]
    db, schema, table = _parse_table_identifier(target_table)
    if schema is None:
        schema = "RAW"
    if db is None:
        db = env_db

    cur = conn.cursor()
    try:
        # Ensure context
        cur.execute(f"USE DATABASE {db}")
        cur.execute(f"USE SCHEMA {schema}")

        tmp_name = f"TMP_{table}_{uuid.uuid4().hex[:8].upper()}"

        # Create temp table based on target structure
        cur.execute(f"CREATE TEMPORARY TABLE {tmp_name} LIKE {db}.{schema}.{table}")

        # Load into temp table
        write_pandas(conn, df, tmp_name, schema=schema)

        # Build MERGE
        on_clause = " AND ".join([f"T.{k.upper()} = S.{k.upper()}" for k in key_columns])
        non_key_cols = [c for c in df.columns if c.upper() not in {k.upper() for k in key_columns}]
        set_clause = ", ".join([f"{c} = S.{c}" for c in non_key_cols]) if non_key_cols else ""
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"S.{c}" for c in df.columns])

        when_matched = ""
        if updated_col and updated_col.upper() in df.columns and set_clause:
            when_matched = (
                f"when matched and S.{updated_col.upper()} >= T.{updated_col.upper()} "
                f"then update set {set_clause}"
            )
        elif set_clause:
            when_matched = f"when matched then update set {set_clause}"

        merge_sql = f"""
            merge into {db}.{schema}.{table} as T
            using {db}.{schema}.{tmp_name} as S
            on {on_clause}
            {when_matched}
            when not matched then insert ({insert_cols}) values ({insert_vals});
        """
        cur.execute(merge_sql)
    finally:
        cur.close()
