from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2
import os

spark = SparkSession.builder \
    .appName("KafkaSparkPipeline") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")) \
    .option("subscribe", "events") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING)")

def write_to_postgres(batch_df, batch_id):

    data = batch_df.collect()

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cur = conn.cursor()

    for row in data:
        cur.execute(
            "INSERT INTO events(data) VALUES(%s)",
            (row.value,)
        )

    conn.commit()
    conn.close()

query = value_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()