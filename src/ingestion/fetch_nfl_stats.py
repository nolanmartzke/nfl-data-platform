import requests
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

API_URL = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/teams"

RAW_PATH = Path("/opt/airflow/data/raw/nfl_raw.csv")

def fetch_team_data():
    res = requests.get(API_URL)
    data = res.json()

    teams = []
    for team_ref in data["items"]:
        team_data = requests.get(team_ref["$ref"]).json()
        teams.append({
            "id": team_data["id"],
            "name": team_data["name"],
            "location": team_data["location"],
            "abbreviation": team_data["abbreviation"],
            "created_at": datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(teams)

    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_PATH, index=False)

    return df

if __name__ == "__main__":
    fetch_team_data()