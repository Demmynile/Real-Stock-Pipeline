from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

@app.get("/events")
def get_events():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM events ORDER BY id DESC LIMIT 100")

    rows = cur.fetchall()

    conn.close()

    return {"data": rows}