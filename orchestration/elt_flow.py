import os
import sys
import subprocess
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task

BASE_DIR = Path(__file__).resolve().parents[1]

# Load Snowflake creds for child processes (dbt, scripts)
load_dotenv(BASE_DIR / ".env")


def run(cmd: list[str], cwd: Path | None = None, env: dict | None = None):
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=cwd, env=env)


@task(retries=2, retry_delay_seconds=60, timeout_seconds=600)
def generate_data():
    run([sys.executable, str(BASE_DIR / "ingestion" / "generate_synthetic.py")])


@task(retries=2, retry_delay_seconds=60, timeout_seconds=900)
def load_csvs():
    run([sys.executable, str(BASE_DIR / "ingestion" / "load_csvs.py")])


@task(retries=2, retry_delay_seconds=60, timeout_seconds=900)
def fetch_weather():
    run([sys.executable, str(BASE_DIR / "ingestion" / "fetch_weather.py")])


@task(retries=1, timeout_seconds=1800)
def dbt_run():
    # Use module entrypoint so it works reliably on Windows
    run([sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", str(BASE_DIR / "dbt_project")])


@task(retries=1, timeout_seconds=900)
def dbt_test():
    run([sys.executable, "-m", "dbt.cli.main", "test", "--project-dir", str(BASE_DIR / "dbt_project")])


@flow(name="elt_pipeline")
def elt_pipeline():
    generate_data()
    load_csvs()
    fetch_weather()
    dbt_run()
    dbt_test()


if __name__ == "__main__":
    elt_pipeline()
