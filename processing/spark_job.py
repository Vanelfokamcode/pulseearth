import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, LongType,
    TimestampType
)

load_dotenv()

# Configuration
BROKER = os.getenv("BROKER", "localhost:9092")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_DB = os.getenv("CLICKHOUSE_DB", "pulseearth")
CH_USER = os.getenv("CLICKHOUSE_USER", "pulse")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "earth2026")
CHECKPOINT = "/tmp/pulseearth_checkpoints"

# ── Schémas ──────────────────────────────────────────────────────────────────

EARTHQUAKE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("mag", FloatType(), True),
    StructField("place", StringType(), True),
    StructField("time_ms", LongType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("depth_km", FloatType(), True),
    StructField("alert", StringType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("sig", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("mag_type", StringType(), True),
    StructField("net", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

WILDFIRE_SCHEMA = StructType([
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("brightness", FloatType(), True),
    StructField("frp", FloatType(), True),
    StructField("confidence", StringType(), True),
    StructField("acq_date", StringType(), True),
    StructField("acq_time", StringType(), True),
    StructField("satellite", StringType(), True),
    StructField("daynight", StringType(), True),
    StructField("scan", FloatType(), True),
    StructField("track", FloatType(), True),
    StructField("ingested_at", StringType(), True),
])

POLLUTION_SCHEMA = StructType([
    StructField("location_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("value", FloatType(), True),
    StructField("unit", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("country", StringType(), True),
    StructField("measured_at", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PulseEarth")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def read_topic(spark: SparkSession, topic: str, schema: StructType):
    """Lit un topic Redpanda et parse le JSON."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

def write_to_clickhouse(df, table: str, checkpoint_suffix: str):
    ch_url = f"http://{CH_HOST}:{CH_PORT}/"

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # On convertit le batch en liste de dictionnaires pour garder l'ordre des colonnes
        rows = batch_df.collect()
        columns = batch_df.columns
        col_list = ", ".join(columns)

        lines = []
        for row in rows:
            vals = []
            for col in columns:
                v = row[col]
                
                # 1. Gestion des NULLs (Important pour ClickHouse TabSeparated)
                if v is None:
                    vals.append(r"\N") 
                
                # 2. Formatage des Floats
                elif isinstance(v, float):
                    vals.append(f"{v:.6f}")
                
                # 3. Formatage des Timestamps / Datetime (Spark Timestamp -> String)
                # ClickHouse préfère "YYYY-MM-DD HH:MM:SS" sans le T ou le fuseau complexe
                elif hasattr(v, 'isoformat'): # Si c'est un objet datetime/timestamp
                    vals.append(v.strftime('%Y-%m-%d %H:%M:%S'))
                
                # 4. Nettoyage des chaînes de caractères
                else:
                    s = str(v).replace("\t", " ").replace("\n", " ").replace("\r", "")
                    vals.append(s)
            
            lines.append("\t".join(vals))

        tsv_data = "\n".join(lines) + "\n" # Toujours finir par un saut de ligne
        query = f"INSERT INTO {CH_DB}.{table} ({col_list}) FORMAT TabSeparated"

        import requests
        try:
            r = requests.post(
                ch_url,
                params={"query": query, "user": CH_USER, "password": CH_PASSWORD},
                data=tsv_data.encode("utf-8"),
                timeout=30,
            )
            if r.status_code != 200:
                print(f"❌ [ClickHouse ERROR] {table}: {r.text[:500]}")
            else:
                print(f"✅ [ClickHouse] {table}: {len(rows)} rows (batch {batch_id})")
        except Exception as e:
            print(f"❌ [Request Error] {e}")

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT}/{checkpoint_suffix}")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession démarrée")

    # ── Lecture des trois topics ─────────────────────────────────────────────
    df_eq = read_topic(spark, "earthquakes", EARTHQUAKE_SCHEMA)
    df_wf = read_topic(spark, "wildfires",   WILDFIRE_SCHEMA)
    df_pol = read_topic(spark, "pollution",   POLLUTION_SCHEMA)

    # ── Enrichissement earthquakes ───────────────────────────────────────────
    df_eq = df_eq.withColumn(
        "event_time",
        (F.col("time_ms") / 1000).cast(TimestampType())
    )

    # ── Enrichissement wildfires ─────────────────────────────────────────────
    df_wf = df_wf.withColumn(
        "event_time",
        F.to_timestamp(
            F.concat(F.col("acq_date"), F.lit(" "),
                     F.lpad(F.col("acq_time"), 4, "0")),
            "yyyy-MM-dd HHmm"
        )
    )

    # ── Enrichissement pollution + watermark ─────────────────────────────────
    df_pol = (
        df_pol
        .withColumn("event_time", F.to_timestamp("measured_at"))
        .withWatermark("event_time", "2 hours")
    )

    # ── Écriture dans ClickHouse ─────────────────────────────────────────────
    q_eq = write_to_clickhouse(df_eq,  "earthquakes", "eq")
    q_wf = write_to_clickhouse(df_wf,  "wildfires",   "wf")
    q_pol = write_to_clickhouse(df_pol, "pollution",    "pol")

    print("✅ 3 streams actifs → ClickHouse")
    print("   Ctrl+C pour arrêter")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()