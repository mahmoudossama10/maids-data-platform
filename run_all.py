import os, sys, subprocess
from pathlib import Path
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")

def run(cmd, cwd=None):
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=cwd)

if __name__ == "__main__":
    run([sys.executable, str(BASE / "ingestion" / "generate_synthetic.py")])
    run([sys.executable, str(BASE / "ingestion" / "load_csvs.py")])
    run([sys.executable, str(BASE / "ingestion" / "fetch_weather.py")])
    run([sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", str(BASE / "dbt_project")])
    run([sys.executable, "-m", "dbt.cli.main", "test", "--project-dir", str(BASE / "dbt_project")])
