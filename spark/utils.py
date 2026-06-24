"""Utilidades compartidas para análisis GDELT."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Mapeo FIPS10-4 (2 chars) → región mundial
FIPS_TO_REGION = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "GT": "North America", "HN": "North America", "NI": "North America",
    "CR": "North America", "PA": "North America", "CU": "North America",
    "BR": "South America", "AR": "South America", "CO": "South America",
    "PE": "South America", "CL": "South America", "VE": "South America",
    "EC": "South America", "BO": "South America", "PY": "South America",
    "UY": "South America", "GY": "South America", "SR": "South America",
    "GB": "Europe", "DE": "Europe", "FR": "Europe", "IT": "Europe",
    "ES": "Europe", "PT": "Europe", "NL": "Europe", "BE": "Europe",
    "PL": "Europe", "SE": "Europe", "NO": "Europe", "FI": "Europe",
    "DK": "Europe", "IE": "Europe", "AT": "Europe", "CH": "Europe",
    "GR": "Europe", "RO": "Europe", "CZ": "Europe", "HU": "Europe",
    "UA": "Europe", "RU": "Europe",
    "CN": "Asia", "JP": "Asia", "KR": "Asia", "IN": "Asia",
    "PK": "Asia", "BD": "Asia", "ID": "Asia", "PH": "Asia",
    "VN": "Asia", "TH": "Asia", "MY": "Asia", "SG": "Asia",
    "TW": "Asia", "AF": "Asia", "IR": "Asia", "IQ": "Asia",
    "SA": "Asia", "AE": "Asia", "IL": "Asia", "TR": "Asia",
    "EG": "Africa", "NG": "Africa", "ZA": "Africa", "KE": "Africa",
    "ET": "Africa", "GH": "Africa", "TZ": "Africa", "UG": "Africa",
    "DZ": "Africa", "MA": "Africa", "TN": "Africa", "LY": "Africa",
    "SD": "Africa", "CM": "Africa", "CI": "Africa", "SN": "Africa",
    "AU": "Oceania", "NZ": "Oceania", "FJ": "Oceania",
}

FIPS_TO_CONTINENT = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "BR": "South America", "AR": "South America", "CO": "South America",
    "GB": "Europe", "DE": "Europe", "FR": "Europe", "IT": "Europe",
    "ES": "Europe", "RU": "Europe", "UA": "Europe",
    "CN": "Asia", "JP": "Asia", "IN": "Asia", "KR": "Asia",
    "EG": "Africa", "NG": "Africa", "ZA": "Africa", "KE": "Africa",
    "AU": "Oceania", "NZ": "Oceania",
}

# CAMEO actor type codes → categoría legible
ACTOR_TYPE_MAP = {
    "GOV": "Government", "MIL": "Military", "REB": "Rebel",
    "OPP": "Opposition", "PTY": "Political Party", "JUD": "Judiciary",
    "BUS": "Business", "EDU": "Education", "MED": "Media",
    "REL": "Religious", "NGO": "NGO", "IGO": "IGO",
    "CVL": "Civilian", "REF": "Refugee", "CRM": "Criminal",
    "DEV": "Development", "ELI": "Elite", "LAB": "Labor",
    "SPY": "Intelligence", "UNK": "Unknown",
}

REGION_EXPR = None


def get_region_expr(col_name: str):
    """Expresión Spark para mapear código país → región."""
    expr = F.lit("Other")
    for fips, region in FIPS_TO_REGION.items():
        expr = F.when(F.col(col_name) == fips, F.lit(region)).otherwise(expr)
    return expr


def get_continent_expr(col_name: str):
    expr = F.lit("Other")
    for fips, cont in FIPS_TO_CONTINENT.items():
        expr = F.when(F.col(col_name) == fips, F.lit(cont)).otherwise(expr)
    return expr


def normalize_actor_type(col_name: str):
    """Normaliza tipo de actor CAMEO a categoría."""
    expr = F.lit("Other")
    for code, label in ACTOR_TYPE_MAP.items():
        expr = F.when(F.col(col_name).startswith(code), F.lit(label)).otherwise(expr)
    return expr


def add_date_column(df: DataFrame, sql_date_col: str = "SQLDATE") -> DataFrame:
    return df.withColumn(
        "event_date",
        F.to_date(F.col(sql_date_col).cast("string"), "yyyyMMdd"),
    )


def write_to_mongo(df: DataFrame, collection: str, mongo_uri: str, mode: str = "overwrite"):
    """Escribe DataFrame a MongoDB."""
    (
        df.write
        .format("mongodb")
        .mode(mode)
        .option("connection.uri", mongo_uri)
        .option("database", "gdelt_analytics")
        .option("collection", collection)
        .save()
    )
