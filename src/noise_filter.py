import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

from config import COL_MMSI, MAX_SPEED_KN, MIN_SOG_KN


# -- Native Haversine expression ----------------------------------------------
def haversine_nm(lat1, lon1, lat2, lon2):
    R    = F.lit(3440.065)
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlam = F.radians(lon2 - lon1)
    a    = (F.pow(F.sin(dphi / 2), 2) +
            F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(dlam / 2), 2))
    return 2 * R * F.asin(F.sqrt(F.greatest(F.lit(0.0), a)))


# -- Step 1: Duplicate removal and GPS noise removal ---------------

def clean_track_points(df: DataFrame) -> DataFrame:
    df = df.repartition(COL_MMSI).sortWithinPartitions(COL_MMSI, "ts_unix")

    w = Window.partitionBy(COL_MMSI).orderBy("ts_unix")

    df = df.withColumn("prev_lat",  F.lag("lat",     1).over(w))
    df = df.withColumn("prev_lon",  F.lag("lon",     1).over(w))
    df = df.withColumn("prev_unix", F.lag("ts_unix", 1).over(w))

    df = df.withColumn(
        "dist_prev",
        F.when(
            F.col("prev_lat").isNotNull(),
            haversine_nm(
                F.col("prev_lat"), F.col("prev_lon"),
                F.col("lat"),      F.col("lon"),
            )
        ).otherwise(F.lit(None))
    )

    w_dup = (Window
             .partitionBy(COL_MMSI, "ts_unix")
             .orderBy(F.col("dist_prev").asc_nulls_last()))
    df = df.withColumn("dup_rank", F.row_number().over(w_dup))
    df = df.filter(F.col("dup_rank") == 1).drop("dup_rank")

    df = df.withColumn(
        "dt_h",
        F.when(
            F.col("prev_unix").isNotNull(),
            (F.col("ts_unix") - F.col("prev_unix")) / 3600.0
        ).otherwise(None)
    )
    df = df.withColumn(
        "implied_speed",
        F.when(
            (F.col("dt_h") > 0) & F.col("dist_prev").isNotNull(),
            F.col("dist_prev") / F.col("dt_h")
        ).otherwise(None)
    )

    df = df.filter(
        F.col("implied_speed").isNull() |
        (F.col("implied_speed") <= MAX_SPEED_KN)
    )

    return df.drop("prev_lat", "prev_lon", "prev_unix",
                   "dt_h", "dist_prev", "implied_speed")


# -- Step 2: SAR transponder filter ------------------------------------------

def remove_sar_transponders(df: DataFrame) -> DataFrame:
    """
    MMSI prefix 111 = SAR aircraft and vessel transponders.
    """
    return df.filter(~(F.col(COL_MMSI).cast("string").startswith("111")))


# -- Step 3: Stationary vessel filter -----------------------------------------

def remove_stationary_vessels(df: DataFrame) -> DataFrame:
    median_sog = (
        df.groupBy(COL_MMSI)
        .agg(F.percentile_approx("sog", 0.5).alias("median_sog"))
    )
    moving = median_sog.filter(F.col("median_sog") >= MIN_SOG_KN)
    return df.join(
        F.broadcast(moving.select(COL_MMSI)),
        on=COL_MMSI,
        how="inner"
    )