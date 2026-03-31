"""Analyze a leaderboard Delta table and print gameplay-dynamics reports to the terminal.

Entry point: ``analyze_leaderboard(input_path, spark)``

Five report sections are printed, each as an ASCII table:

  [1] Leaderboard overview     – meta stats (snapshots, date range, unique players)
  [2] Current standings        – top-10 from the latest snapshot
  [3] Top climbers             – players with most cumulative rank positions gained
  [4] Score velocity leaders   – players with highest total score gain
  [5] Rank volatility          – most unstable rank trajectories (highest std dev)
  [6] Daily score momentum     – avg total score gain per calendar day (trend over time)
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType

# Terminal width used to size section-header banners.
_BANNER_WIDTH = 70


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _banner(title: str) -> None:
    bar = "═" * _BANNER_WIDTH
    print(f"\n╔{bar}╗")
    print(f"║  {title:<{_BANNER_WIDTH - 2}}║")
    print(f"╚{bar}╝")


def _section(number: int, title: str) -> None:
    print(f"\n{'─' * _BANNER_WIDTH}")
    print(f"  [{number}]  {title}")
    print(f"{'─' * _BANNER_WIDTH}")


def _kv(label: str, value: object) -> str:
    return f"  {label:<28}{value}"


# ---------------------------------------------------------------------------
# Schema helper: captured_at may be TimestampType or Struct{seconds, nanos}
# ---------------------------------------------------------------------------


def _add_snapshot_ts(df: DataFrame) -> DataFrame:
    """Add a ``snapshot_ts`` TimestampType column, handling both Spark representations
    of google.protobuf.Timestamp (native TimestampType or Struct{seconds, nanos})."""
    field_type = df.schema["captured_at"].dataType
    if isinstance(field_type, StructType):
        return df.withColumn(
            "snapshot_ts",
            F.from_unixtime(F.col("captured_at.seconds")).cast("timestamp"),
        )
    if isinstance(field_type, TimestampType):
        return df.withColumn("snapshot_ts", F.col("captured_at"))
    # Fallback: treat as epoch seconds integer
    return df.withColumn("snapshot_ts", F.from_unixtime(F.col("captured_at")).cast("timestamp"))


# ---------------------------------------------------------------------------
# Main analysis entry point
# ---------------------------------------------------------------------------


def analyze_leaderboard(input_path: str | Path, spark: SparkSession) -> None:
    """Load a leaderboard Delta table and print gameplay-dynamics reports.

    Parameters
    ----------
    input_path:
        Path to the Delta table directory written by ``main.py --format delta``.
    spark:
        Active SparkSession (local or cluster).
    """
    input_path = str(input_path)

    # ── Load ──────────────────────────────────────────────────────────────
    raw = spark.read.format("delta").load(input_path)
    raw = _add_snapshot_ts(raw)
    raw.cache()

    # ── Explode entries into one row per (snapshot, player) ───────────────
    entries = (
        raw.select(
            "snapshot_id",
            "snapshot_ts",
            "leaderboard_name",
            "game_id",
            F.explode("entries").alias("e"),
        )
        .select(
            "snapshot_id",
            "snapshot_ts",
            "leaderboard_name",
            "game_id",
            F.col("e.rank").alias("rank"),
            F.col("e.player_id").alias("player_id"),
            F.col("e.player_name").alias("player_name"),
            F.col("e.score").alias("score"),
            F.col("e.previous_rank").alias("previous_rank"),
            F.col("e.rank_delta").alias("rank_delta"),
            F.col("e.score_delta").alias("score_delta"),
            F.col("e.hours_played").alias("hours_played"),
        )
    )
    entries.cache()

    # ── Section [1]: Overview ─────────────────────────────────────────────
    meta = raw.agg(
        F.count("snapshot_id").alias("total_snapshots"),
        F.min("snapshot_ts").alias("from_dt"),
        F.max("snapshot_ts").alias("to_dt"),
        F.first("leaderboard_name").alias("leaderboard_name"),
        F.first("game_id").alias("game_id"),
    ).collect()[0]

    unique_players = entries.select("player_id").distinct().count()
    total_entries = entries.count()
    days = (meta["to_dt"] - meta["from_dt"]).days + 1 if meta["from_dt"] and meta["to_dt"] else 0

    _banner(f"LEADERBOARD ANALYSIS  ·  {meta['leaderboard_name']}  (game: {meta['game_id']})")
    print(_kv("Period:", f"{meta['from_dt']}  →  {meta['to_dt']}"))
    print(_kv("Days simulated:", days))
    print(_kv("Hourly snapshots:", meta["total_snapshots"]))
    print(_kv("Unique players:", unique_players))
    print(_kv("Total entry rows:", f"{total_entries:,}"))

    # ── Section [2]: Current standings ───────────────────────────────────
    latest_ts = raw.agg(F.max("snapshot_ts")).collect()[0][0]
    _section(2, f"CURRENT STANDINGS  (snapshot: {latest_ts})")

    current = (
        entries.filter(F.col("snapshot_ts") == latest_ts)
        .orderBy("rank")
        .select(
            "rank",
            "player_name",
            F.format_number("score", 0).alias("score"),
            F.round("hours_played", 1).alias("hours_played"),
            F.when(F.col("rank_delta") > 0, F.concat(F.lit("+"), F.col("rank_delta").cast("string")))
            .when(F.col("rank_delta") < 0, F.col("rank_delta").cast("string"))
            .otherwise("—")
            .alias("rank_Δ"),
            F.format_number("score_delta", 0).alias("score_Δ"),
        )
        .limit(10)
    )
    current.show(truncate=False)

    # ── Section [3]: Top climbers ─────────────────────────────────────────
    _section(3, "TOP CLIMBERS  (greatest total rank positions gained)")

    climbers = (
        entries.groupBy("player_id", "player_name")
        .agg(
            F.sum("rank_delta").alias("total_rank_gain"),
            F.count("snapshot_id").alias("snapshots_on_board"),
            F.min("rank").alias("best_rank"),
        )
        .orderBy(F.desc("total_rank_gain"))
        .select(
            "player_name",
            F.col("total_rank_gain").alias("total_rank_gain"),
            F.col("best_rank").alias("best_rank"),
            F.col("snapshots_on_board").alias("snapshots_on_board"),
        )
        .limit(10)
    )
    climbers.show(truncate=False)

    # ── Section [4]: Score velocity leaders ──────────────────────────────
    _section(4, "SCORE VELOCITY LEADERS  (highest total score gain)")

    scorers = (
        entries.groupBy("player_id", "player_name")
        .agg(
            F.sum("score_delta").alias("total_score_gain"),
            F.round(F.avg("score_delta"), 0).alias("avg_per_snapshot"),
            F.max("score").alias("peak_score"),
            F.round(F.max("hours_played"), 1).alias("total_hours"),
        )
        .orderBy(F.desc("total_score_gain"))
        .select(
            "player_name",
            F.format_number("total_score_gain", 0).alias("total_score_gain"),
            F.format_number("avg_per_snapshot", 0).alias("avg_per_snapshot"),
            F.format_number("peak_score", 0).alias("peak_score"),
            F.col("total_hours").alias("total_hours"),
        )
        .limit(10)
    )
    scorers.show(truncate=False)

    # ── Section [5]: Rank volatility ──────────────────────────────────────
    _section(5, "RANK VOLATILITY  (most unstable rank trajectories)")

    volatility = (
        entries.groupBy("player_id", "player_name")
        .agg(
            F.round(F.stddev("rank"), 1).alias("rank_stddev"),
            F.round(F.avg("rank"), 1).alias("avg_rank"),
            F.min("rank").alias("best_rank"),
            F.max("rank").alias("worst_rank"),
            F.count("snapshot_id").alias("snapshots"),
        )
        .filter(F.col("rank_stddev").isNotNull())
        .orderBy(F.desc("rank_stddev"))
        .select(
            "player_name",
            F.col("rank_stddev").alias("rank_stddev"),
            F.col("avg_rank").alias("avg_rank"),
            F.col("best_rank").alias("best_rank"),
            F.col("worst_rank").alias("worst_rank"),
            F.col("snapshots").alias("snapshots"),
        )
        .limit(10)
    )
    volatility.show(truncate=False)

    # ── Section [6]: Daily score momentum ────────────────────────────────
    _section(6, "DAILY SCORE MOMENTUM  (total score gain per calendar day)")

    daily = (
        entries.withColumn("day", F.to_date("snapshot_ts"))
        .groupBy("day")
        .agg(
            F.format_number(F.sum("score_delta"), 0).alias("total_score_gained"),
            F.round(F.avg("score_delta"), 0).alias("avg_per_player_snapshot"),
            F.countDistinct("player_id").alias("active_players"),
            F.sum(F.when(F.col("previous_rank") == 0, 1).otherwise(0)).alias("new_entrants"),
        )
        .orderBy("day")
    )
    daily.show(truncate=False, n=365)

    # ── Teardown ──────────────────────────────────────────────────────────
    entries.unpersist()
    raw.unpersist()
    print(f"\n{'═' * _BANNER_WIDTH}")
    print("  Analysis complete.")
    print(f"{'═' * _BANNER_WIDTH}\n")
