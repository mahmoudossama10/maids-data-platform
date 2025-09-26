from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
from utils import get_conn, ensure_tables, merge_upsert


def load_table(df: pd.DataFrame, target: str, key_cols: list[str], updated_col: str = "updated_at"):
    conn = get_conn()
    try:
        merge_upsert(conn, target, df, key_columns=key_cols, updated_col=updated_col)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        data_dir = (Path(__file__).resolve().parent.parent / "data")
        customers_fp = data_dir / "customers.csv"
        workers_fp = data_dir / "workers.csv"
        bookings_fp = data_dir / "bookings.csv"

        if not customers_fp.exists() or not workers_fp.exists() or not bookings_fp.exists():
            print("CSV files not found. Run: python ingestion/generate_synthetic.py", file=sys.stderr)
            sys.exit(1)

        # Read CSVs with proper datetime parsing
        customers = pd.read_csv(customers_fp, parse_dates=["created_at", "updated_at"])
        workers = pd.read_csv(workers_fp, parse_dates=["created_at", "updated_at"])
        bookings = pd.read_csv(
            bookings_fp,
            parse_dates=["requested_at", "assigned_at", "completed_at", "canceled_at", "updated_at"]
        )

        # Ensure base tables exist
        conn = get_conn()
        ensure_tables(conn)
        conn.close()

        # Upsert into RAW
        print("Upserting RAW.CUSTOMERS ...")
        load_table(customers, "RAW.CUSTOMERS", key_cols=["customer_id"], updated_col="updated_at")

        print("Upserting RAW.WORKERS ...")
        load_table(workers, "RAW.WORKERS", key_cols=["worker_id"], updated_col="updated_at")

        print("Upserting RAW.BOOKINGS ...")
        load_table(bookings, "RAW.BOOKINGS", key_cols=["booking_id"], updated_col="updated_at")

        print("Loaded RAW tables successfully.")

    except Exception as e:
        print(f"Error loading CSVs: {e}", file=sys.stderr)
        sys.exit(2)
