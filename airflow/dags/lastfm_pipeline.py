import os
import requests
import json
import time
from datetime import datetime

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-spark_2.12:3.3.0 pyspark-shell'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-arm64'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum, count, avg, col, lit

# API config
API_KEY = os.environ.get('LASTFM_API_KEY')
BASE_URL = "http://ws.audioscrobbler.com/2.0/"

# Paths
BASE_PATH = "/opt/airflow/data/lastfm_pipeline"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

def get_spark():
    """
    Create and return a Delta-enabled SparkSession.
    Defined as a function so it can be called fresh within each Airflow task.
    """
    return SparkSession.builder \
        .appName("lastfm-pipeline") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()

# Schemas
track_schema = StructType([
    StructField("track_name", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("listeners", IntegerType(), True),
    StructField("chart_rank", IntegerType(), True),
    StructField("snapshot_date", StringType(), True),
    StructField("country", StringType(), True)
])

artist_tag_schema = StructType([
    StructField("artist_name", StringType(), True),
    StructField("tag_name", StringType(), True),
    StructField("tag_count", IntegerType(), True),
    StructField("snapshot_date", StringType(), True)
])

def get_top_tracks(country, limit=50):
    url = f"{BASE_URL}?method=geo.gettoptracks&country={country}&limit={limit}&api_key={API_KEY}&format=json"
    response = requests.get(url)
    data = response.json()
    return data

def get_artist_tags(artist_name, snapshot_date):
    url = f"{BASE_URL}?method=artist.gettoptags&artist={artist_name}&api_key={API_KEY}&format=json"
    response = requests.get(url)
    data = response.json()
    if "toptags" not in data:
        print(f"[INGEST] No tags found for {artist_name}")
        return []
    tags = data["toptags"]["tag"]
    parsed_tags = []
    for tag in tags:
        parsed_tags.append((
            artist_name,
            tag["name"],
            int(tag["count"]),
            snapshot_date
        ))
    return parsed_tags

def get_all_artist_tags(parsed_tracks, snapshot_date):
    artists = list(set([track[1] for track in parsed_tracks]))
    all_tags = []
    for artist in artists:
        print(f"[INGEST] Fetching tags for {artist}...")
        tags = get_artist_tags(artist, snapshot_date)
        all_tags.extend(tags)
        time.sleep(0.2)
    print(f"[INGEST] Fetched {len(all_tags)} tag records for {len(artists)} artists")
    return all_tags

def write_bronze(spark, raw_data, source):
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_json_string = json.dumps(raw_data)
    bronze_data = [(source, ingested_at, raw_json_string)]
    bronze_columns = ["source", "ingested_at", "raw_json"]
    bronze_df = spark.createDataFrame(bronze_data, bronze_columns)
    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .save(f"{BRONZE_PATH}/raw_api_responses")
    print(f"[BRONZE] Written {source} at {ingested_at}")

def parse_tracks(raw_data, snapshot_date, country):
    tracks = raw_data["tracks"]["track"]
    parsed_tracks = []
    for track in tracks:
        duration = int(track["duration"]) if track["duration"] else 0
        parsed_tracks.append((
            track["name"],
            track["artist"]["name"],
            duration,
            int(track["listeners"]),
            int(track["@attr"]["rank"]) + 1,
            snapshot_date,
            country
        ))
    print(f"[SILVER] Parsed {len(parsed_tracks)} tracks for {country}")
    return parsed_tracks

def write_silver_tracks(spark, parsed_tracks, country):
    snapshot_date = parsed_tracks[0][5]
    silver_path = f"{SILVER_PATH}/tracks"
    if os.path.exists(silver_path):
        existing = spark.read.format("delta").load(silver_path)
        already_loaded = existing.filter(
            (existing.snapshot_date == snapshot_date) &
            (existing.country == country)
        ).count()
        if already_loaded > 0:
            print(f"[SILVER] Tracks for {snapshot_date} {country} already exist - skipping")
            return
    silver_df = spark.createDataFrame(parsed_tracks, track_schema)
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .save(silver_path)
    print(f"[SILVER] Written {len(parsed_tracks)} tracks to silver")

def write_silver_tags(spark, all_tags, snapshot_date):
    silver_path = f"{SILVER_PATH}/artist_tags"
    if os.path.exists(silver_path):
        existing = spark.read.format("delta").load(silver_path)
        already_loaded = existing.filter(
            existing.snapshot_date == snapshot_date
        ).count()
        if already_loaded > 0:
            print(f"[SILVER] Tags for {snapshot_date} already exist - skipping")
            return
    tags_df = spark.createDataFrame(all_tags, artist_tag_schema)
    tags_df.write \
        .format("delta") \
        .mode("append") \
        .save(silver_path)
    print(f"[SILVER] Written {len(all_tags)} tag records to silver")

def build_gold_genre_summary(spark, snapshot_date):
    gold_path = f"{GOLD_PATH}/genre_summary"
    if os.path.exists(gold_path):
        existing = spark.read.format("delta").load(gold_path)
        already_loaded = existing.filter(
            existing.snapshot_date == snapshot_date
        ).count()
        if already_loaded > 0:
            print(f"[GOLD] Genre summary for {snapshot_date} already exists - skipping")
            return
    tracks = spark.read.format("delta").load(f"{SILVER_PATH}/tracks")
    tags = spark.read.format("delta").load(f"{SILVER_PATH}/artist_tags")
    tracks = tracks.filter(tracks.snapshot_date == snapshot_date)
    tags = tags.filter(tags.tag_count > 10)
    joined = tracks.join(tags, on="artist_name", how="left")
    gold_df = joined.groupBy("tag_name") \
        .agg(
            count("track_name").alias("track_count"),
            sum("listeners").alias("total_listeners"),
            avg("chart_rank").alias("avg_chart_rank")
        ) \
        .orderBy(col("total_listeners").desc()) \
        .withColumn("snapshot_date", lit(snapshot_date))
    gold_df.write \
        .format("delta") \
        .mode("append") \
        .save(gold_path)
    print(f"[GOLD] Genre summary written for {snapshot_date}")

# Task functions called by Airflow
def task_ingest_and_bronze(**context):
    spark = get_spark()
    snapshot_date = context['ds']  # Airflow passes the run date as 'ds'
    countries = ["united kingdom", "united states", "germany"]
    all_parsed_tracks = []
    for country in countries:
        raw_tracks = get_top_tracks(country)
        write_bronze(spark, raw_tracks, f"lastfm_geo_toptracks_{country.replace(' ', '_')}")
        parsed_tracks = parse_tracks(raw_tracks, snapshot_date, country)
        all_parsed_tracks.extend(parsed_tracks)
    # Push parsed tracks to XCom so next task can use them
    context['ti'].xcom_push(key='parsed_tracks', value=all_parsed_tracks)
    print(f"[PIPELINE] Ingest complete for {snapshot_date}")

def task_silver(**context):
    spark = get_spark()
    snapshot_date = context['ds']
    # Pull parsed tracks from previous task via XCom
    parsed_tracks = context['ti'].xcom_pull(
        task_ids='ingest_and_bronze',
        key='parsed_tracks'
    )
    all_tags = get_all_artist_tags(parsed_tracks, snapshot_date)
    # Write bronze for tags
    for artist_name in list(set([track[1] for track in parsed_tracks])):
        raw_tags = get_artist_tags(artist_name, snapshot_date)
        if raw_tags:
            write_bronze(spark, {"artist": artist_name, "tags": raw_tags},
                        f"lastfm_artist_tags_{artist_name.replace(' ', '_')}")
    write_silver_tracks(spark, parsed_tracks, "united kingdom")
    write_silver_tags(spark, all_tags, snapshot_date)
    print(f"[PIPELINE] Silver complete for {snapshot_date}")

def task_gold(**context):
    spark = get_spark()
    snapshot_date = context['ds']
    build_gold_genre_summary(spark, snapshot_date)
    print(f"[PIPELINE] Gold complete for {snapshot_date}")