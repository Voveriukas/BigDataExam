from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType

from config import COLLISION_DIST_NM, GRID_CELL_DEG, TIME_BUCKET_MINUTES, COL_MMSI
from noise_filter import haversine_nm


def add_grid_cells(df: DataFrame) -> DataFrame:
    df = df.withColumn("grid_lat", F.floor(F.col("lat") / F.lit(GRID_CELL_DEG)).cast("int"))
    df = df.withColumn("grid_lon", F.floor(F.col("lon") / F.lit(GRID_CELL_DEG)).cast("int"))
    df = df.withColumn(
        "time_bucket",
        (F.col("ts_unix") / F.lit(TIME_BUCKET_MINUTES * 60)).cast("long")
    )
    return df


def add_previous_position(df: DataFrame) -> DataFrame:
    w = Window.partitionBy(COL_MMSI).orderBy("ts_unix")
    df = df.withColumn("prev_lat",  F.lag("lat",     1).over(w))
    df = df.withColumn("prev_lon",  F.lag("lon",     1).over(w))
    df = df.withColumn("prev_unix", F.lag("ts_unix", 1).over(w))
    return df

def keep_top_candidates_per_pair(candidates: DataFrame, n: int = 3) -> DataFrame:
    w = (
        Window
        .partitionBy("mmsi_a", "mmsi_b")
        .orderBy(
            F.col("distance_nm").asc(),
            F.col("closing_speed_kn").desc(),
            F.col("ts_a").asc(),
        )
    )
    return (
        candidates
        .withColumn("pair_rank", F.row_number().over(w))
        .filter(F.col("pair_rank") <= n)
        .drop("pair_rank")
    )


def find_collision_candidates(df: DataFrame) -> DataFrame:
    
    df = df.filter(F.col("sog") > 0.5)
    df = add_grid_cells(df)
    df = add_previous_position(df)

    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    neighbors = spark.createDataFrame(
        [(dy, dx) for dy in [-1, 0, 1] for dx in [-1, 0, 1]],
        ["dy", "dx"]
    )

    a_expanded = (
        df.crossJoin(F.broadcast(neighbors))
        .withColumn("join_grid_lat", F.col("grid_lat") + F.col("dy"))
        .withColumn("join_grid_lon", F.col("grid_lon") + F.col("dx"))
        .drop("dy", "dx")
    ).alias("a")

    b = df.alias("b")

    candidates = a_expanded.join(b,
        (F.col("a.time_bucket")    == F.col("b.time_bucket")) &
        (F.col("a.join_grid_lat")  == F.col("b.grid_lat")) &
        (F.col("a.join_grid_lon")  == F.col("b.grid_lon")) &
        (F.col(f"a.{COL_MMSI}") < F.col(f"b.{COL_MMSI}")),
        how="inner"
    ).drop("join_grid_lat", "join_grid_lon")

    from noise_filter import haversine_nm as haversine_nm_native
    candidates = candidates.withColumn(
        "distance_nm",
        haversine_nm_native(F.col("a.lat"), F.col("a.lon"),
                            F.col("b.lat"), F.col("b.lon"))
    )

    candidates = candidates.filter(F.col("distance_nm") < COLLISION_DIST_NM)

    # Compute closing speed from vessel A perspective
    candidates = candidates.withColumn(
        "dt_hours",
        F.when(F.col("a.prev_unix").isNotNull(),
               (F.col("a.ts_unix") - F.col("a.prev_unix")) / 3600.0
        ).otherwise(None)
    )
    
    prev_distance = haversine_nm(
        F.col("a.prev_lat"), F.col("a.prev_lon"),
        F.col("b.prev_lat"), F.col("b.prev_lon")
    )

    curr_distance = haversine_nm(
        F.col("a.lat"), F.col("a.lon"),
        F.col("b.lat"), F.col("b.lon")
    )

    candidates = candidates.withColumn(
        "closing_speed_kn",
        F.when(
            (F.col("dt_hours") > 0) &
            prev_distance.isNotNull() &
            curr_distance.isNotNull(),
            (prev_distance - curr_distance) / F.col("dt_hours")
        )
    )

    # Keep only converging pairs
    candidates = candidates.filter(
        F.col("closing_speed_kn").isNotNull() &
        (F.col("closing_speed_kn") > 0.5)
    )

    # Count distinct vessels per grid cell per time bucket
    vessel_counts = (
        df.groupBy("grid_lat", "grid_lon", "time_bucket")
        .agg(F.countDistinct(COL_MMSI).alias("vessels_in_cell"))
    )

    # Attach scene density to each vessel in the pair
    candidates = candidates.join(
        vessel_counts.alias("vc_a"),
        (F.col("a.grid_lat") == F.col("vc_a.grid_lat")) &
        (F.col("a.grid_lon") == F.col("vc_a.grid_lon")) &
        (F.col("a.time_bucket") == F.col("vc_a.time_bucket")),
        how="left"
    ).withColumnRenamed("vessels_in_cell", "vessels_in_cell_a")

    candidates = candidates.join(
        vessel_counts.alias("vc_b"),
        (F.col("b.grid_lat") == F.col("vc_b.grid_lat")) &
        (F.col("b.grid_lon") == F.col("vc_b.grid_lon")) &
        (F.col("b.time_bucket") == F.col("vc_b.time_bucket")),
        how="left"
    ).withColumnRenamed("vessels_in_cell", "vessels_in_cell_b")

    candidates = candidates.select(
        F.col(f"a.{COL_MMSI}").alias("mmsi_a"),
        F.col(f"b.{COL_MMSI}").alias("mmsi_b"),
        F.col("a.name").alias("name_a"),
        F.col("b.name").alias("name_b"),
        F.col("a.ts").alias("ts_a"),
        F.col("b.ts").alias("ts_b"),
        F.col("a.lat").alias("lat_a"),
        F.col("a.lon").alias("lon_a"),
        F.col("b.lat").alias("lat_b"),
        F.col("b.lon").alias("lon_b"),
        F.col("a.sog").alias("sog_a"),
        F.col("b.sog").alias("sog_b"),
        F.col("distance_nm"),
        F.col("closing_speed_kn"),
        F.col("vessels_in_cell_a"),
        F.col("vessels_in_cell_b"),
    )

    return keep_top_candidates_per_pair(candidates, n=3)


def add_ais_silence_flag(candidates: DataFrame, df_raw: DataFrame) -> DataFrame:
    
    SILENCE_WINDOW_S = 120       # vessel must go silent within 2 min of event
    END_BUFFER_S     = 43200     # ignore vessels whose last ping is within 12 hours of dataset end

    # Global dataset end time
    dataset_end_unix = df_raw.agg(F.max("ts_unix")).collect()[0][0]

    # Last ping time per vessel
    last_ping = (df_raw
                 .groupBy(COL_MMSI)
                 .agg(F.max("ts_unix").alias("last_ping_unix")))

    # Only consider vessels whose last ping is well before the dataset ends
    # Vessels still transmitting near midnight are not "silent" - data just ends
    genuine_silence = last_ping.filter(
        F.col("last_ping_unix") < (dataset_end_unix - END_BUFFER_S)
    )

    # Join genuine silence info for vessel A and vessel B
    candidates = candidates.join(
        genuine_silence.withColumnRenamed(COL_MMSI, "mmsi_a")
                       .withColumnRenamed("last_ping_unix", "last_ping_a"),
        on="mmsi_a", how="left"
    )
    candidates = candidates.join(
        genuine_silence.withColumnRenamed(COL_MMSI, "mmsi_b")
                       .withColumnRenamed("last_ping_unix", "last_ping_b"),
        on="mmsi_b", how="left"
    )

    candidates = candidates.withColumn(
        "event_unix",
        F.unix_timestamp("ts_a")
    )

    # Time from event to each vessels last genuine ping
    candidates = candidates.withColumn(
        "silence_a",
        F.col("last_ping_a") - F.col("event_unix")
    )
    candidates = candidates.withColumn(
        "silence_b",
        F.col("last_ping_b") - F.col("event_unix")
    )

    def is_silent(col):
        return (
            F.col(col).isNotNull() &
            (F.col(col) >= 0) &
            (F.col(col) < SILENCE_WINDOW_S)
        )

    silent_a = is_silent("silence_a")
    silent_b = is_silent("silence_b")

    # Either vessel silent = collision indicator
    # Both vessels silent = strongest indicator
    candidates = candidates.withColumn("silent_a", silent_a)
    candidates = candidates.withColumn("silent_b", silent_b)

    # Count how many vessels went silent (0, 1, or 2)
    candidates = candidates.withColumn(
        "silence_count",
        F.col("silent_a").cast("int") + F.col("silent_b").cast("int")
    )

    # Minimum silence gap
    candidates = candidates.withColumn(
        "min_silence_s",
        F.least(
            F.when(silent_a, F.col("silence_a")).otherwise(F.lit(999999)),
            F.when(silent_b, F.col("silence_b")).otherwise(F.lit(999999))
        )
    )

    return candidates


def add_post_event_behavior(candidates: DataFrame, df_clean: DataFrame) -> DataFrame:
    WINDOW_S = 120

    def get_window_stats(cands_df: DataFrame, df: DataFrame, mmsi_col: str,
                         ts_col: str, suffix: str) -> DataFrame:

        cands = cands_df.select(
            F.col(mmsi_col).alias("mmsi"),
            F.unix_timestamp(F.col(ts_col)).alias("event_unix"),
        ).distinct()

        joined = df.alias("pings").join(
            cands.alias("evts"),
            F.col("pings.mmsi") == F.col("evts.mmsi"),
            how="inner"
        ).select(
            F.col("evts.mmsi").alias("_mmsi"),
            F.col("evts.event_unix").alias("_event_unix"),
            (F.col("pings.ts_unix") - F.col("evts.event_unix")).alias("offset_s"),
            F.col("pings.sog").alias("sog"),
            F.col("pings.cog").alias("cog"),
        )

        before = (joined
                  .filter((F.col("offset_s") >= -WINDOW_S) & (F.col("offset_s") < 0))
                  .groupBy("_mmsi", "_event_unix")
                  .agg(
                      F.mean("sog").alias(f"mean_sog_before_{suffix}"),
                      F.mean("cog").alias(f"mean_cog_before_{suffix}"),
                  ))

        after = (joined
                 .filter((F.col("offset_s") > 0) & (F.col("offset_s") <= WINDOW_S))
                 .groupBy("_mmsi", "_event_unix")
                 .agg(
                     F.mean("sog").alias(f"mean_sog_after_{suffix}"),
                     F.mean("cog").alias(f"mean_cog_after_{suffix}"),
                 ))

        return before, after

    before_a, after_a = get_window_stats(candidates, df_clean, "mmsi_a", "ts_a", "a")
    before_b, after_b = get_window_stats(candidates, df_clean, "mmsi_b", "ts_b", "b")

    event_unix_a = F.unix_timestamp(F.col("ts_a"))
    event_unix_b = F.unix_timestamp(F.col("ts_b"))

    candidates = candidates.join(
        before_a,
        (F.col("mmsi_a") == F.col("_mmsi")) & (event_unix_a == F.col("_event_unix")),
        how="left"
    ).drop("_mmsi", "_event_unix")

    candidates = candidates.join(
        after_a,
        (F.col("mmsi_a") == F.col("_mmsi")) & (event_unix_a == F.col("_event_unix")),
        how="left"
    ).drop("_mmsi", "_event_unix")

    candidates = candidates.join(
        before_b,
        (F.col("mmsi_b") == F.col("_mmsi")) & (event_unix_b == F.col("_event_unix")),
        how="left"
    ).drop("_mmsi", "_event_unix")

    candidates = candidates.join(
        after_b,
        (F.col("mmsi_b") == F.col("_mmsi")) & (event_unix_b == F.col("_event_unix")),
        how="left"
    ).drop("_mmsi", "_event_unix")

    # SOG drop: mean SOG before minus mean SOG after (positive = deceleration)
    candidates = candidates.withColumn(
        "sog_drop_a",
        F.coalesce(F.col("mean_sog_before_a"), F.lit(0.0)) -
        F.coalesce(F.col("mean_sog_after_a"),  F.lit(0.0))
    )
    candidates = candidates.withColumn(
        "sog_drop_b",
        F.coalesce(F.col("mean_sog_before_b"), F.lit(0.0)) -
        F.coalesce(F.col("mean_sog_after_b"),  F.lit(0.0))
    )
    # Combined SOG drop - max of the two vessels
    candidates = candidates.withColumn(
        "max_sog_drop",
        F.greatest(
            F.col("sog_drop_a"),
            F.col("sog_drop_b"),
            F.lit(0.0)
        )
    )

    # COG change: course change before vs after (0-180 degrees)
    def circular_diff(col_before, col_after):
        diff = F.abs(F.col(col_before) - F.col(col_after))
        return F.least(diff, F.lit(360.0) - diff)

    candidates = candidates.withColumn(
        "cog_change_a",
        F.when(
            F.col("mean_cog_before_a").isNotNull() & F.col("mean_cog_after_a").isNotNull(),
            circular_diff("mean_cog_before_a", "mean_cog_after_a")
        ).otherwise(F.lit(0.0))
    )
    candidates = candidates.withColumn(
        "cog_change_b",
        F.when(
            F.col("mean_cog_before_b").isNotNull() & F.col("mean_cog_after_b").isNotNull(),
            circular_diff("mean_cog_before_b", "mean_cog_after_b")
        ).otherwise(F.lit(0.0))
    )
    # Combined COG change - max of the two vessels
    candidates = candidates.withColumn(
        "max_cog_change",
        F.greatest(F.col("cog_change_a"), F.col("cog_change_b"), F.lit(0.0))
    )

    # Post-event divergence: difference between vessel A and B
    # post-event COGs. Higher = they move in more different directions.
    candidates = candidates.withColumn(
        "post_event_divergence",
        F.when(
            F.col("mean_cog_after_a").isNotNull() & F.col("mean_cog_after_b").isNotNull(),
            circular_diff("mean_cog_after_a", "mean_cog_after_b")
        ).otherwise(F.lit(0.0))
    )

    drop_cols = [
        "mean_sog_before_a", "mean_sog_after_a",
        "mean_sog_before_b", "mean_sog_after_b",
        "mean_cog_before_a", "mean_cog_after_a",
        "mean_cog_before_b", "mean_cog_after_b",
        "sog_drop_a", "sog_drop_b",
        "cog_change_a", "cog_change_b",
    ]
    return candidates.drop(*drop_cols)


def find_closest_approach(candidates: DataFrame, df_clean: DataFrame = None):
    
    # Add AIS silence information if clean data available
    if df_clean is not None:
        candidates = add_ais_silence_flag(candidates, df_clean)
        candidates = add_post_event_behavior(candidates, df_clean)
    else:
        candidates = candidates.withColumn("ais_silence",   F.lit(False))
        candidates = candidates.withColumn("silence_count", F.lit(0))
        candidates = candidates.withColumn("min_silence_s", F.lit(999999))
        candidates = candidates.withColumn("max_sog_drop",          F.lit(0.0))
        candidates = candidates.withColumn("max_cog_change",        F.lit(0.0))
        candidates = candidates.withColumn("post_event_divergence", F.lit(0.0))

    candidates = candidates.withColumn(
        "scene_density",
        (F.coalesce(F.col("vessels_in_cell_a"), F.lit(1)) +
         F.coalesce(F.col("vessels_in_cell_b"), F.lit(1))) / 2.0
    )

    #   silence_count   x40  - going dark is a strong signal but not absolute
    #   min_silence_s   x10  - faster silence = stronger (inverted: 120-s)
    #   max_sog_drop    x8   - deceleration is a reliable physical reaction
    #   max_cog_change  x0.3 - course change in degrees (large scale, lower weight)
    #   post_diverge    x0.2 - post-event divergence in degrees
    #   closing_speed   x3   - convergence speed before event
    #   scene_density        - subtract: busier scene = less likely collision
    #   distance             - subtract: closer = more likely
    candidates = candidates.withColumn(
        "max_cog_change",
        F.least(F.col("max_cog_change"), F.lit(180.0))
    ).withColumn(
        "post_event_divergence",
        F.least(F.col("post_event_divergence"), F.lit(180.0))
    ).withColumn(
        "max_sog_drop",
        F.least(F.col("max_sog_drop"), F.lit(20.0))
    ).withColumn(
        "closing_speed_kn",
        F.least(F.col("closing_speed_kn"), F.lit(30.0))
    )

    # -- Positive signals -----------------------------------------------------

    # Silence: the dominant signal. silence_count=1 must beat any
    # non-silent vessel regardless of behavioral signals.
    # Max behavioral score (capped): sog=20*8 + cog=180*0.2 + div=180*0.15
    #   + speed=30*3 = 160+36+27+90 = 313
    # So silence_count=1 base weight must exceed 313.
    # Set to 320 - any vessel that goes silent wins over all non-silent vessels.
    # Sublinear silence scoring:
    # 1 vessel going silent = 320 points (beats any non-silent vessel)
    # 2 vessels going silent = 350 points (only slightly more than 1)
    # Rationale: two fishing vessels entering port both go silent simultaneously
    # all the time. Making silence_count=2 worth 640 would make that pattern
    # dominate over a genuine single-vessel-sinking collision.
    silence_base = F.when(
        F.col("silence_count") >= 1, F.lit(320.0)
    ).otherwise(F.lit(0.0))
    silence_extra = F.when(
        F.col("silence_count") >= 2, F.lit(30.0)
    ).otherwise(F.lit(0.0))

    silence_score = (
        silence_base + silence_extra +
        F.when(
            F.col("min_silence_s") < 999999,
            (120.0 - F.least(F.col("min_silence_s"), F.lit(120.0))) / 120.0 * 20.0
        ).otherwise(F.lit(0.0))
    )

    # Behavioral reactions - used to rank within the same silence tier
    behavior_score = (
        F.col("max_sog_drop") * 8.0 +
        F.col("max_cog_change") * 0.4 +
        F.col("post_event_divergence") * 0.3 +
        F.col("closing_speed_kn") * 3.0
    )

    # -- Negative signals -----------------------------------------------------
    no_sog_reaction_penalty = F.when(
        F.col("max_sog_drop") < 1.0,
        (1.0 - F.col("max_sog_drop")) * 10.0
    ).otherwise(F.lit(0.0))

    distance_penalty = F.when(
        F.col("distance_nm") * 1852.0 > 100.0,
        (F.col("distance_nm") * 1852.0 - 100.0) / 100.0 * 2.0
    ).otherwise(F.lit(0.0))

    density_penalty = (F.col("scene_density") - 2.0) * 3.0

    # -- Final score ----------------------------------------------------------
    candidates = candidates.withColumn(
        "collision_score",
        silence_score
        + behavior_score
        - no_sog_reaction_penalty
        - distance_penalty
        - F.greatest(density_penalty, F.lit(0.0))
    )

    candidates = candidates.cache()
    result = candidates.orderBy(F.col("collision_score").desc()).first()
    return result, candidates