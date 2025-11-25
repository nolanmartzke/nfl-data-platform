import pandas as pd
import psycopg2

def load():
    conn = psycopg2.connect(
        host="postgres",
        dbname="nfl",
        user="airflow",
        password="airflow",
        port=5432
    )
    cur = conn.cursor()

    df = pd.read_csv("/opt/airflow/data/processed/nfl_clean.csv")

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO nfl_stats (player, team, passing_yards)
            VALUES (%s, %s, %s)
            """,
            (row['player'], row['team'], row['passing_yards'])
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    load()