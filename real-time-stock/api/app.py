from fastapi import FastAPI
import psycopg2

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        host="postgres",
        database="analytics",
        user="sparkuser",
        password="sparkpass"
    )

@app.get("/events")
def get_events():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM events ORDER BY id DESC LIMIT 100")

    rows = cur.fetchall()

    conn.close()

    return {"data": rows}