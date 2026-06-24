"""
Análisis GDELT con Apache Spark.
Lee datos Parquet y escribe resultados pre-calculados en MongoDB.
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

from utils import (
    add_date_column,
    get_continent_expr,
    get_region_expr,
    normalize_actor_type,
    write_to_mongo,
)

PARQUET_PATH = os.getenv("PARQUET_PATH", "/data/parquet")
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:gdelt2026@mongodb:27017/gdelt_analytics?authSource=admin",
)
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GDELT-Analytics")
        .master(SPARK_MASTER)
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_parquet(spark: SparkSession, table: str):
    path = f"{PARQUET_PATH}/{table}"
    if not os.path.exists(path):
        print(f"[WARN] No hay datos en {path}")
        return None
    return spark.read.parquet(path)


def analysis_01_conflict_heatmap(events):
    """Mapa de calor: intensidad de conflictos por país por día (Goldstein)."""
    df = (
        events
        .filter(F.col("QuadClass").isin([3, 4]))
        .transform(add_date_column)
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .groupBy("event_date", "ActionGeo_CountryCode")
        .agg(
            F.avg("GoldsteinScale").alias("avg_goldstein"),
            F.count("*").alias("conflict_events"),
            F.sum(F.abs("GoldsteinScale")).alias("intensity_score"),
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country_code")
    )
    return df


def analysis_02_top_countries(events):
    """Top 10 países con más eventos noticiosos por día."""
    w = Window.partitionBy("event_date").orderBy(F.desc("event_count"))
    df = (
        events
        .transform(add_date_column)
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .groupBy("event_date", "ActionGeo_CountryCode")
        .agg(F.count("*").alias("event_count"))
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 10)
        .withColumnRenamed("ActionGeo_CountryCode", "country_code")
        .select("event_date", "country_code", "event_count", "rank")
    )
    return df


def analysis_03_tone_sources_correlation(events):
    """Correlación entre AvgTone y NumSources."""
    stats = events.select(
        F.corr("AvgTone", "NumSources").alias("correlation"),
        F.count("*").alias("sample_size"),
        F.avg("AvgTone").alias("avg_tone"),
        F.avg("NumSources").alias("avg_sources"),
    ).withColumn("analysis_date", F.current_date())
    return stats


def analysis_04_cameo_by_region(events):
    """Distribución de tipos de eventos CAMEO por región."""
    df = (
        events
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .withColumn("region", get_region_expr("ActionGeo_CountryCode"))
        .groupBy("region", "EventBaseCode", "EventRootCode")
        .agg(F.count("*").alias("event_count"))
        .orderBy("region", F.desc("event_count"))
    )
    return df


def analysis_05_actor_interaction_matrix(events):
    """Matriz de interacción entre tipos de actores."""
    df = (
        events
        .withColumn("actor1_type", normalize_actor_type("Actor1Type1Code"))
        .withColumn("actor2_type", normalize_actor_type("Actor2Type1Code"))
        .groupBy("actor1_type", "actor2_type")
        .agg(F.count("*").alias("frequency"))
        .orderBy(F.desc("frequency"))
    )
    return df


def analysis_06_media_coverage(events):
    """Países con mayor cobertura mediática (menciones/evento)."""
    df = (
        events
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .groupBy("ActionGeo_CountryCode")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("NumMentions").alias("total_mentions"),
            F.avg("NumMentions").alias("avg_mentions_per_event"),
        )
        .withColumn(
            "mentions_per_event_ratio",
            F.col("total_mentions") / F.col("total_events"),
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country_code")
        .orderBy(F.desc("mentions_per_event_ratio"))
    )
    return df


def analysis_07_sentiment_trend(events):
    """Tendencia de sentimiento por país (promedio móvil AvgTone)."""
    w = Window.partitionBy("ActionGeo_CountryCode").orderBy("event_date").rowsBetween(-6, 0)
    df = (
        events
        .transform(add_date_column)
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .groupBy("event_date", "ActionGeo_CountryCode")
        .agg(F.avg("AvgTone").alias("daily_avg_tone"))
        .withColumn("moving_avg_7d", F.avg("daily_avg_tone").over(w))
        .withColumnRenamed("ActionGeo_CountryCode", "country_code")
        .orderBy("country_code", "event_date")
    )
    return df


def analysis_08_conflict_pairs(events):
    """Pares de países en conflicto con mayor frecuencia."""
    df = (
        events
        .filter(F.col("QuadClass").isin([3, 4]))
        .filter(
            F.col("Actor1CountryCode").isNotNull()
            & F.col("Actor2CountryCode").isNotNull()
            & (F.col("Actor1CountryCode") != F.col("Actor2CountryCode"))
        )
        .withColumn(
            "country_pair",
            F.array_sort(F.array("Actor1CountryCode", "Actor2CountryCode")),
        )
        .groupBy(
            F.col("country_pair")[0].alias("country_1"),
            F.col("country_pair")[1].alias("country_2"),
        )
        .agg(F.count("*").alias("conflict_count"))
        .orderBy(F.desc("conflict_count"))
        .limit(50)
    )
    return df


def analysis_09_escalation(mentions, events):
    """Detección de escalada: aumento acelerado de menciones en 24h."""
    mentions_ts = mentions.withColumn(
        "mention_hour",
        F.substring(F.col("MentionTimeDate").cast("string"), 1, 10),
    )
    hourly = (
        mentions_ts
        .groupBy("GLOBALEVENTID", "mention_hour")
        .agg(F.count("*").alias("hourly_mentions"))
    )
    w = Window.partitionBy("GLOBALEVENTID").orderBy("mention_hour")
    growth = (
        hourly
        .withColumn("prev_mentions", F.lag("hourly_mentions", 1).over(w))
        .withColumn(
            "growth_rate",
            F.when(
                F.col("prev_mentions") > 0,
                (F.col("hourly_mentions") - F.col("prev_mentions")) / F.col("prev_mentions"),
            ).otherwise(F.lit(0.0)),
        )
        .filter(F.col("growth_rate") > 2.0)
        .filter(F.col("hourly_mentions") >= 10)
    )
    if events is not None:
        growth = growth.join(
            events.select("GLOBALEVENTID", "ActionGeo_CountryCode", "EventCode"),
            "GLOBALEVENTID",
            "left",
        )
    return growth.select(
        "GLOBALEVENTID", "mention_hour", "hourly_mentions",
        "growth_rate", "ActionGeo_CountryCode", "EventCode",
    )


def analysis_10_religion_clusters(events):
    """Agrupamiento de conflictos por religión y región."""
    df = (
        events
        .filter(F.col("QuadClass").isin([3, 4]))
        .filter(
            F.col("Actor1Religion1Code").isNotNull()
            | F.col("Actor2Religion1Code").isNotNull()
        )
        .withColumn("region", get_region_expr("ActionGeo_CountryCode"))
        .withColumn(
            "religion",
            F.coalesce("Actor1Religion1Code", "Actor2Religion1Code"),
        )
        .groupBy("region", "religion")
        .agg(F.count("*").alias("conflict_count"))
        .orderBy("region", F.desc("conflict_count"))
    )
    return df


def analysis_11_gkg_themes(gkg):
    """Principales temas GKG por continente por año."""
    df = (
        gkg
        .filter(F.col("V2Themes").isNotNull())
        .withColumn("year", F.substring(F.col("DATE").cast("string"), 1, 4))
        .withColumn("theme", F.explode(F.split("V2Themes", ";")))
        .withColumn("theme_name", F.split("theme", ",")[0])
        .withColumn(
            "continent",
            F.when(
                F.col("Locations").contains(",US,")
                | F.col("Locations").contains(",CA,"),
                F.lit("North America"),
            )
            .when(F.col("Locations").contains(",BR,"), F.lit("South America"))
            .when(
                F.col("Locations").contains(",GB,")
                | F.col("Locations").contains(",DE,")
                | F.col("Locations").contains(",FR,"),
                F.lit("Europe"),
            )
            .when(
                F.col("Locations").contains(",CN,")
                | F.col("Locations").contains(",IN,")
                | F.col("Locations").contains(",JP,"),
                F.lit("Asia"),
            )
            .when(
                F.col("Locations").contains(",NG,")
                | F.col("Locations").contains(",ZA,")
                | F.col("Locations").contains(",EG,"),
                F.lit("Africa"),
            )
            .otherwise(F.lit("Other")),
        )
        .groupBy("continent", "year", "theme_name")
        .agg(F.count("*").alias("theme_count"))
    )
    w = Window.partitionBy("continent", "year").orderBy(F.desc("theme_count"))
    return (
        df.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 20)
    )


def analysis_12_top_organizations(gkg):
    """Organizaciones más mencionadas globalmente por día."""
    df = (
        gkg
        .filter(F.col("V2Organizations").isNotNull())
        .withColumn(
            "event_date",
            F.to_date(F.substring(F.col("DATE").cast("string"), 1, 8), "yyyyMMdd"),
        )
        .withColumn("org", F.explode(F.split("V2Organizations", ";")))
        .withColumn("org_name", F.split("org", ",")[0])
        .filter(F.length("org_name") > 2)
        .groupBy("event_date", "org_name")
        .agg(F.count("*").alias("mention_count"))
    )
    w = Window.partitionBy("event_date").orderBy(F.desc("mention_count"))
    return (
        df.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 20)
    )


def analysis_13_tone_lag(events):
    """Análisis de rezago: tono mediático hoy vs conflictos mañana."""
    daily = (
        events
        .transform(add_date_column)
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .groupBy("event_date", "ActionGeo_CountryCode")
        .agg(
            F.avg("AvgTone").alias("avg_tone"),
            F.sum(
                F.when(F.col("QuadClass").isin([3, 4]), 1).otherwise(0)
            ).alias("conflict_count"),
        )
    )
    w = Window.partitionBy("ActionGeo_CountryCode").orderBy("event_date")
    lagged = (
        daily
        .withColumn("next_day_conflicts", F.lead("conflict_count", 1).over(w))
        .withColumn("tone_lag_1d", F.col("avg_tone"))
        .filter(F.col("next_day_conflicts").isNotNull())
    )
    correlation = lagged.groupBy("ActionGeo_CountryCode").agg(
        F.corr("tone_lag_1d", "next_day_conflicts").alias("lag_correlation"),
        F.count("*").alias("observations"),
    ).withColumnRenamed("ActionGeo_CountryCode", "country_code")
    return correlation


def analysis_14_diplomatic_graph(events):
    """Grafo: interacciones diplomáticas vs conflictos entre países."""
    df = (
        events
        .filter(
            F.col("Actor1CountryCode").isNotNull()
            & F.col("Actor2CountryCode").isNotNull()
            & (F.col("Actor1CountryCode") != F.col("Actor2CountryCode"))
        )
        .withColumn(
            "interaction_type",
            F.when(F.col("QuadClass").isin([1, 2]), F.lit("diplomatic"))
            .when(F.col("QuadClass").isin([3, 4]), F.lit("conflict"))
            .otherwise(F.lit("neutral")),
        )
        .withColumn(
            "country_pair",
            F.array_sort(F.array("Actor1CountryCode", "Actor2CountryCode")),
        )
        .groupBy(
            F.col("country_pair")[0].alias("country_1"),
            F.col("country_pair")[1].alias("country_2"),
            "interaction_type",
        )
        .agg(F.count("*").alias("interaction_count"))
        .orderBy(F.desc("interaction_count"))
    )
    return df


def analysis_15_source_diversity(mentions, events):
    """Índice de diversidad de fuentes por país."""
    event_country = events.select(
        "GLOBALEVENTID", F.col("ActionGeo_CountryCode").alias("country_code"),
    ).filter(F.col("country_code").isNotNull())

    diversity = (
        mentions
        .join(event_country, "GLOBALEVENTID")
        .groupBy("country_code")
        .agg(
            F.countDistinct("MentionSourceName").alias("unique_sources"),
            F.count("*").alias("total_mentions"),
            F.countDistinct("GLOBALEVENTID").alias("unique_events"),
        )
        .withColumn(
            "diversity_index",
            F.col("unique_sources") / F.col("unique_events"),
        )
        .orderBy(F.desc("diversity_index"))
    )
    return diversity


def analysis_16_ethnic_conflicts(events):
    """Frecuencia de conflictos por etnia de actores."""
    df = (
        events
        .filter(F.col("QuadClass").isin([3, 4]))
        .withColumn(
            "ethnicity",
            F.coalesce("Actor1EthnicCode", "Actor2EthnicCode"),
        )
        .filter(F.col("ethnicity").isNotNull())
        .groupBy("ethnicity")
        .agg(F.count("*").alias("conflict_count"))
        .orderBy(F.desc("conflict_count"))
        .limit(30)
    )
    return df


def analysis_17_breaking_news(mentions):
    """Noticias de última hora: 0 a >100 menciones en menos de 1 hora."""
    hourly = (
        mentions
        .withColumn(
            "mention_hour",
            F.substring(F.col("MentionTimeDate").cast("string"), 1, 10),
        )
        .groupBy("GLOBALEVENTID", "mention_hour")
        .agg(F.count("*").alias("hourly_mentions"))
    )
    w = Window.partitionBy("GLOBALEVENTID").orderBy("mention_hour")
    breaking = (
        hourly
        .withColumn("prev", F.lag("hourly_mentions", 1).over(w))
        .filter(
            (F.coalesce(F.col("prev"), F.lit(0)) == 0)
            & (F.col("hourly_mentions") > 100)
        )
        .select(
            "GLOBALEVENTID", "mention_hour", "hourly_mentions",
            F.lit(True).alias("is_breaking_news"),
        )
    )
    return breaking


def analysis_18_quadclass_timeline(events):
    """[Extra 1] Evolución temporal de QuadClass por región."""
    df = (
        events
        .transform(add_date_column)
        .withColumn("region", get_region_expr("ActionGeo_CountryCode"))
        .groupBy("event_date", "region", "QuadClass")
        .agg(F.count("*").alias("event_count"))
        .orderBy("event_date", "region", "QuadClass")
    )
    return df


def analysis_19_hourly_density(events):
    """[Extra 2] Densidad de eventos por hora del día (UTC)."""
    df = (
        events
        .withColumn(
            "hour_utc",
            F.substring(F.col("DATEADDED").cast("string"), 9, 2).cast(IntegerType()),
        )
        .groupBy("hour_utc")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("AvgTone").alias("avg_tone"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
        )
        .orderBy("hour_utc")
    )
    return df


ANALYSES = [
    ("conflict_heatmap", analysis_01_conflict_heatmap, ["events"]),
    ("top_countries_events", analysis_02_top_countries, ["events"]),
    ("tone_sources_correlation", analysis_03_tone_sources_correlation, ["events"]),
    ("cameo_by_region", analysis_04_cameo_by_region, ["events"]),
    ("actor_interaction_matrix", analysis_05_actor_interaction_matrix, ["events"]),
    ("media_coverage", analysis_06_media_coverage, ["events"]),
    ("sentiment_trend", analysis_07_sentiment_trend, ["events"]),
    ("conflict_country_pairs", analysis_08_conflict_pairs, ["events"]),
    ("escalation_events", analysis_09_escalation, ["mentions", "events"]),
    ("religion_conflict_clusters", analysis_10_religion_clusters, ["events"]),
    ("gkg_themes_by_continent", analysis_11_gkg_themes, ["gkg"]),
    ("top_organizations", analysis_12_top_organizations, ["gkg"]),
    ("tone_lag_analysis", analysis_13_tone_lag, ["events"]),
    ("diplomatic_conflict_graph", analysis_14_diplomatic_graph, ["events"]),
    ("source_diversity_index", analysis_15_source_diversity, ["mentions", "events"]),
    ("ethnic_conflict_frequency", analysis_16_ethnic_conflicts, ["events"]),
    ("breaking_news", analysis_17_breaking_news, ["mentions"]),
    ("quadclass_timeline", analysis_18_quadclass_timeline, ["events"]),
    ("hourly_event_density", analysis_19_hourly_density, ["events"]),
]


def run_all_analyses():
    print(f"[{datetime.utcnow()}] Iniciando análisis GDELT")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    data = {}
    for table in ("events", "mentions", "gkg"):
        data[table] = load_parquet(spark, table)

    if data["events"] is None:
        print("[ERROR] No hay datos de eventos. Ejecute el loader primero.")
        spark.stop()
        sys.exit(1)

    data["events"].cache()
    if data["mentions"] is not None:
        data["mentions"].cache()
    if data["gkg"] is not None:
        data["gkg"].cache()

    metadata = spark.createDataFrame(
        [(datetime.utcnow().isoformat(), "completed")],
        ["last_run", "status"],
    )
    write_to_mongo(metadata, "pipeline_metadata", MONGO_URI)

    for name, func, required in ANALYSES:
        try:
            print(f"  → Ejecutando: {name}")
            args = []
            for req in required:
                if data.get(req) is None:
                    print(f"    [SKIP] Falta tabla {req}")
                    break
                args.append(data[req])
            else:
                result = func(*args)
                if result is not None and result.count() > 0:
                    write_to_mongo(result, name, MONGO_URI)
                    print(f"    [OK] {name}: {result.count()} registros")
                else:
                    print(f"    [WARN] {name}: sin resultados")
        except Exception as exc:
            print(f"    [ERROR] {name}: {exc}")

    spark.stop()
    print(f"[{datetime.utcnow()}] Análisis completado")


if __name__ == "__main__":
    run_all_analyses()
