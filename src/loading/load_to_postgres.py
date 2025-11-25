import pandas as pd
import psycopg2

def load():
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cur = conn.cursor()

    df = pd.read_csv("/opt/airflow/data/processed/nfl_clean.csv")

    cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INT PRIMARY KEY,
                name TEXT,
                location TEXT,
                abbreviation TEXT
            )
            """
        )

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO teams (id, name, location, abbreviation)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (row['id'], row['name'], row['location'], row['abbreviation'])
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    load()