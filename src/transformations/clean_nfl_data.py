import pandas as pd
from pathlib import Path

RAW_PATH = Path("/opt/airflow/data/raw/nfl_raw.json")
PROCESSED_PATH = Path("/opt/airflow/data/process/nfl_clean.csv")

def clean():
    df = pd.read_json(RAW_PATH)

    df.dropna(inplace=True)
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    if "passing_yards" in df:
        df["passing_yards"] = df["passing_yards"].astype(int)
    
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)

if __name__ == "__main__":
    clean()