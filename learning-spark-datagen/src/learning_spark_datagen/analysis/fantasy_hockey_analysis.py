"""Fantasy hockey analytics: six PySpark report sections over a FantasyRosterSnapshot Delta table."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import LongType


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _add_snapshot_ts(df):
    """Normalise captured_at to a usable timestamp column `snapshot_ts`.

    Delta tables written from protobuf may serialise Timestamp as either a
    struct {seconds: long, nanos: int} or a plain string / long.  We handle
    both so the analysis is robust regardless of how the table was written.
    """
    if "captured_at" not in df.columns:
        return df.withColumn("snapshot_ts", F.lit(None).cast("timestamp"))

    first_type = dict(df.dtypes).get("captured_at", "")
    if "struct" in first_type:
        return df.withColumn(
            "snapshot_ts",
            F.to_timestamp(F.col("captured_at.seconds").cast(LongType())),
        )
    return df.withColumn("snapshot_ts", F.to_timestamp("captured_at"))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def analyze_fantasy_rosters(input_path: str | Path, spark: SparkSession) -> None:
    """Load a fantasy roster Delta table and print six league-dynamics reports.

    Reports
    -------
    1. League overview     – league name, scoring period range, number of teams.
    2. Current standings   – team rankings for the most recent scoring week.
    3. Top fantasy scorers – players with the highest cumulative fantasy points.
    4. Position breakdown  – average weekly fantasy points by roster slot type.
    5. Weekly movers       – teams with the largest positive rank change week-over-week.
    6. Roster construction – slot-level contribution (active vs bench efficiency).
    """
    input_path = str(input_path)

    # ── Load ──────────────────────────────────────────────────────────────
    raw = spark.read.format("delta").load(input_path)
    raw = _add_snapshot_ts(raw)
    raw.cache()

    # ── Explode roster entries ─────────────────────────────────────────────
    entries = (
        raw.select(
            "snapshot_id",
            "snapshot_ts",
            "fantasy_team_id",
            "fantasy_team_name",
            "scoring_week",
            "rank",
            "total_fantasy_points",
            "rank_delta",
            F.explode("roster").alias("e"),
        )
        .select(
            "snapshot_id",
            "snapshot_ts",
            "fantasy_team_id",
            "fantasy_team_name",
            "scoring_week",
            "rank",
            "total_fantasy_points",
            "rank_delta",
            F.col("e.player_id").alias("player_id"),
            F.col("e.player_name").alias("player_name"),
            F.col("e.slot_type").alias("slot_type"),
            F.col("e.fantasy_points").alias("fantasy_points"),
            F.col("e.goals").alias("goals"),
            F.col("e.assists").alias("assists"),
            F.col("e.shots_on_goal").alias("shots_on_goal"),
            F.col("e.hits").alias("hits"),
            F.col("e.blocked_shots").alias("blocked_shots"),
            F.col("e.penalty_minutes").alias("penalty_minutes"),
            F.col("e.plus_minus").alias("plus_minus"),
            F.col("e.wins").alias("wins"),
            F.col("e.saves").alias("saves"),
            F.col("e.goals_against").alias("goals_against"),
            F.col("e.is_active").alias("is_active"),
        )
    )
    entries.cache()

    # ── [1] League overview ───────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SECTION 1 — League Overview")
    print("=" * 70)

    overview = raw.agg(
        F.countDistinct("fantasy_team_id").alias("total_teams"),
        F.countDistinct("scoring_week").alias("total_weeks"),
        F.min("scoring_week").alias("first_week"),
        F.max("scoring_week").alias("last_week"),
        F.countDistinct("snapshot_id").alias("total_snapshots"),
    )
    overview.show(truncate=False)

    # ── [2] Current standings ─────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SECTION 2 — Current Standings (Most Recent Week)")
    print("=" * 70)

    latest_week = raw.agg(F.max("scoring_week")).collect()[0][0]
    standings = (
        raw.filter(F.col("scoring_week") == latest_week)
        .select(
            "rank",
            "fantasy_team_name",
            "total_fantasy_points",
            "rank_delta",
            "scoring_week",
        )
        .orderBy("rank")
    )
    standings.show(truncate=False)

    # ── [3] Top fantasy scorers ───────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SECTION 3 — Top 20 Fantasy Scorers (Season Totals, Active Slots Only)")
    print("=" * 70)

    top_scorers = (
        entries.filter(F.col("is_active") == True)  # noqa: E712
        .groupBy("player_id", "player_name")
        .agg(
            F.round(F.sum("fantasy_points"), 2).alias("total_fp"),
            F.sum("goals").alias("G"),
            F.sum("assists").alias("A"),
            F.sum("shots_on_goal").alias("SOG"),
            F.sum("hits").alias("HIT"),
            F.sum("blocked_shots").alias("BLK"),
            F.sum("wins").alias("W"),
            F.sum("saves").alias("SV"),
        )
        .orderBy(F.desc("total_fp"))
        .limit(20)
    )
    top_scorers.show(truncate=False)

    # ── [4] Position breakdown ────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SECTION 4 — Average Weekly Fantasy Points by Slot Type")
    print("=" * 70)

    # Map numeric slot_type enum values to readable labels.
    slot_labels = F.when(F.col("slot_type") == 1, "CENTER") \
        .when(F.col("slot_type") == 2, "LEFT_WING") \
        .when(F.col("slot_type") == 3, "RIGHT_WING") \
        .when(F.col("slot_type") == 4, "DEFENSE") \
        .when(F.col("slot_type") == 5, "GOALIE") \
        .when(F.col("slot_type") == 6, "UTILITY") \
        .when(F.col("slot_type") == 7, "BENCH") \
        .otherwise("UNKNOWN")

    position_breakdown = (
        entries.withColumn("slot_label", slot_labels)
        .groupBy("slot_label")
        .agg(
            F.round(F.avg("fantasy_points"), 2).alias("avg_fp_per_week"),
            F.round(F.sum("fantasy_points"), 2).alias("total_fp"),
            F.count("player_id").alias("player_weeks"),
        )
        .orderBy(F.desc("avg_fp_per_week"))
    )
    position_breakdown.show(truncate=False)

    # ── [5] Weekly movers ─────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SECTION 5 — Top 10 Weekly Movers (Biggest Single-Week Rank Gains)")
    print("=" * 70)

    weekly_movers = (
        raw.filter(F.col("rank_delta") > 0)
        .select("fantasy_team_name", "scoring_week", "rank", "rank_delta", "total_fantasy_points")
        .orderBy(F.desc("rank_delta"), F.desc("total_fantasy_points"))
        .limit(10)
    )
    weekly_movers.show(truncate=False)

    # ── [6] Roster construction efficiency ────────────────────────────────
    print("\n" * 0 + "=" * 70)
    print("  SECTION 6 — Roster Construction: Active vs Bench Efficiency")
    print("=" * 70)

    roster_efficiency = (
        entries.groupBy("fantasy_team_name", "is_active")
        .agg(
            F.round(F.avg("fantasy_points"), 2).alias("avg_fp"),
            F.round(F.sum("fantasy_points"), 2).alias("total_fp"),
            F.count("player_id").alias("player_weeks"),
        )
        .withColumn("slot_class", F.when(F.col("is_active"), "ACTIVE").otherwise("BENCH"))
        .select("fantasy_team_name", "slot_class", "avg_fp", "total_fp", "player_weeks")
        .orderBy("fantasy_team_name", "slot_class")
    )
    roster_efficiency.show(40, truncate=False)

    entries.unpersist()
    raw.unpersist()
