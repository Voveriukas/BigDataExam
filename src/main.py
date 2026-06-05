import argparse
import math
import os
import sys
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, TimestampType,
)
from pyspark import StorageLevel
import time

def elapsed(start: float) -> str:
    s = int(time.time() - start)
    return f"{s//60}m {s%60}s"

from config import (
    CENTER_LAT, CENTER_LON, RADIUS_NM,
    START_DATE, END_DATE,
    COL_TIMESTAMP, COL_MMSI, COL_LAT, COL_LON,
    COL_SOG, COL_COG, COL_NAME, COL_SHIP_TYPE,
    COL_MOBILE_TYPE, COL_NAV_STATUS,
)
from noise_filter import clean_track_points, remove_sar_transponders, remove_stationary_vessels
from collision_detector import find_collision_candidates, find_closest_approach
from visualizer import build_top5_maps, write_report


# -- Spark session ------------------------------------------------------------

def create_spark(driver_memory: str = "8g") -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIS Collision Detector")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "50mb")
        # Reuse processes across tasks
        .config("spark.python.worker.reuse", "true")
        # Kryo serializer is faster than Java default
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Compress cached data
        .config("spark.rdd.compress", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation")
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation")
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation,G1 Concurrent GC")
        # Allow Spark to spill to disk when memory is low
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .getOrCreate()
    )


# -- Data loading -------------------------------------------------------------

def load_ais_data(spark: SparkSession, path: str) -> DataFrame:

    df = spark.read.option("header", "true").csv(path)

    df = df.withColumn("ts", F.to_timestamp(F.col(COL_TIMESTAMP), "dd/MM/yyyy HH:mm:ss"))
    df = df.withColumn("ts_unix", F.unix_timestamp("ts"))

    df = df.select(
        F.col(COL_MMSI).cast(LongType()).alias("mmsi"),
        F.col("ts"), F.col("ts_unix"),
        F.col(COL_LAT).cast(DoubleType()).alias("lat"),
        F.col(COL_LON).cast(DoubleType()).alias("lon"),
        F.col(COL_SOG).cast(DoubleType()).alias("sog"),
        F.col(COL_COG).cast(DoubleType()).alias("cog"),
        F.col(COL_NAME).alias("name"),
        F.col(COL_SHIP_TYPE).alias("ship_type"),
        F.col(COL_MOBILE_TYPE).alias("mobile_type"),
    )

    return df.filter(
        F.col("ts").isNotNull() &
        F.col("lat").isNotNull() &
        F.col("lon").isNotNull() &
        F.col("mmsi").isNotNull() &
        F.col("lat").between(-90.0, 90.0) &
        F.col("lon").between(-180.0, 180.0) &
        ~((F.col("lat") == 0.0) & (F.col("lon") == 0.0))
    )


# -- Geographic filter --------------------------------------------------------

def filter_geographic_area(df: DataFrame) -> DataFrame:
    from noise_filter import haversine_nm

    lat_margin = RADIUS_NM / 60.0
    lon_margin = RADIUS_NM / (60.0 * math.cos(math.radians(CENTER_LAT)))

    df = df.filter(
        F.col("lat").between(CENTER_LAT - lat_margin, CENTER_LAT + lat_margin) &
        F.col("lon").between(CENTER_LON - lon_margin, CENTER_LON + lon_margin)
    )

    return df.filter(
        haversine_nm(
            F.lit(CENTER_LAT), F.lit(CENTER_LON),
            F.col("lat"), F.col("lon"),
        ) <= RADIUS_NM
    )


# -- Vessel state filter ------------------------------------------------------

def filter_moving_vessels(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("mobile_type") == "Class A")
    return df


# -- Trajectory extraction ----------------------------------------------------

def extract_trajectory(
    df: DataFrame,
    mmsi: int,
    center_time_unix: int,
    window_minutes: int = 10,
) -> list[dict]:
    start_unix = center_time_unix - window_minutes * 60
    end_unix   = center_time_unix + window_minutes * 60

    rows = (
        df.filter(F.col("mmsi") == mmsi)
        .filter(F.col("ts_unix").between(start_unix, end_unix))
        .orderBy("ts_unix")
        .select("ts", "lat", "lon", "sog", "cog")
        .collect()
    )

    return [
        {
            "ts":  r["ts"].replace(tzinfo=timezone.utc),
            "lat": r["lat"],
            "lon": r["lon"],
            "sog": r["sog"],
            "cog": r["cog"],
        }
        for r in rows
    ]


# -- Main pipeline ------------------------------------------------------------

def run(data_path: str) -> None:
    spark = create_spark(
        driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger(
        "org.apache.hadoop.util.NativeCodeLoader"
    ).setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.ERROR)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t_start = time.time()

    # 1. Load
    print(f"[{elapsed(t_start)}] Loading AIS data from: {data_path}")
    df = load_ais_data(spark, data_path)
    print(f"[{elapsed(t_start)}] Data loaded")

    # 2-8. Filtering and cleaning
    print(f"[{elapsed(t_start)}] Applying filters (time, geographic, Class A, SAR)...")
    df = df.filter(
        F.col("ts").between(
            F.lit(START_DATE).cast(TimestampType()),
            F.lit(END_DATE + " 23:59:59").cast(TimestampType()),
        )
    )
    df = filter_geographic_area(df)
    df = filter_moving_vessels(df)
    df = remove_sar_transponders(df)

    print(f"[{elapsed(t_start)}] Cleaning track points (duplicates + GPS noise)...")
    df = clean_track_points(df)

    print(f"[{elapsed(t_start)}] Removing stationary vessels...")
    df_clean = remove_stationary_vessels(df)

    print(f"[{elapsed(t_start)}] Persisting clean data...")
    df_clean = df_clean.persist(StorageLevel.MEMORY_AND_DISK)
    clean_count = df_clean.count()
    print(f"[{elapsed(t_start)}] Clean data ready: {clean_count:,} pings")

    # 9. Find collision candidates
    print(f"[{elapsed(t_start)}] Building collision candidates (grid join + distance filter)...")
    candidates = find_collision_candidates(df_clean)

    print(f"[{elapsed(t_start)}] Persisting candidates...")
    candidates = candidates.persist(StorageLevel.MEMORY_AND_DISK)
    candidate_count = candidates.count()
    print(f"[{elapsed(t_start)}] Collision candidates found: {candidate_count:,}")

    if candidate_count == 0:
        print(f"[{elapsed(t_start)}] No candidates found. Exiting.")
        spark.stop()
        sys.exit(0)

    # 10. Score candidates and find winner
    print(f"[{elapsed(t_start)}] Scoring candidates (silence, post-event behavior)...")
    collision, scored_candidates = find_closest_approach(candidates, df_clean)
    print(f"[{elapsed(t_start)}] Scoring complete")
    # Show top 5 distinct vessel pairs
    from pyspark.sql import Window as W
    w_pair = W.partitionBy("mmsi_a", "mmsi_b").orderBy(F.col("collision_score").desc())
    top5 = (scored_candidates
            .withColumn("pair_rank", F.row_number().over(w_pair))
            .filter(F.col("pair_rank") == 1)
            .drop("pair_rank")
            .orderBy(F.col("collision_score").desc())
            .limit(5)
            .collect())
    print(f"[{elapsed(t_start)}] Collecting top 5 pairs...")
    print("Top 5 distinct vessel pairs by collision score:")
    for i, r in enumerate(top5, 1):
        print(f"  #{i} {r['name_a'] or r['mmsi_a']} x {r['name_b'] or r['mmsi_b']}"
              f"  score={r['collision_score']:.1f}  dist={r['distance_nm']*1852:.0f}m"
              f"  silence={r['silence_count']}  ts={r['ts_a']}")
    print()

    winner_mmsi_a = collision["mmsi_a"]
    winner_mmsi_b = collision["mmsi_b"]

    closest = (candidates
               .filter(
                   (F.col("mmsi_a") == winner_mmsi_a) &
                   (F.col("mmsi_b") == winner_mmsi_b)
               )
               .orderBy(F.col("distance_nm").asc())
               .first())

    collision_time = closest["ts_a"].replace(tzinfo=timezone.utc)
    collision_lat  = (closest["lat_a"] + closest["lat_b"]) / 2
    collision_lon  = (closest["lon_a"] + closest["lon_b"]) / 2
    collision_dist = closest["distance_nm"]

    print("\n" + "=" * 55)
    print("COLLISION DETECTED")
    print(f"  Vessel A: {collision['name_a'] or 'Unknown'} (MMSI {winner_mmsi_a})")
    print(f"  Vessel B: {collision['name_b'] or 'Unknown'} (MMSI {winner_mmsi_b})")
    print(f"  Time:     {collision_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Location: {collision_lat:.6f}N, {collision_lon:.6f}E")
    print(f"  Distance: {collision_dist * 1852:.1f} m")
    print("=" * 55 + "\n")

    map_paths = build_top5_maps(
        top5, df_clean, extract_trajectory, collision_time, OUTPUT_DIR
    )

    # 10. Write collision report for the winning pair
    report_path = write_report(
        winner_mmsi_a, winner_mmsi_b,
        collision["name_a"], collision["name_b"],
        collision_time, collision_lat, collision_lon,
        collision_dist,
        output_dir=OUTPUT_DIR,
    )

    candidates.unpersist()
    df_clean.unpersist()
    spark.stop()
    print(f"[{elapsed(t_start)}] Complete")


# -- Entry point --------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AIS Collision Detector")
    parser.add_argument(
        "--file",
        default=None,
    )
    args = parser.parse_args()

    DATA_DIR   = os.environ.get("DATA_DIR",   "/app/data")
    OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")

    data_path = args.file if args.file else DATA_DIR
    run(data_path)