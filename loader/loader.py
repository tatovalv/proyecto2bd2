"""
Loader GDELT 2.0: descarga archivos cada 15 minutos y los convierte a Parquet.
Fuentes: Events, Mentions, GKG
"""

import io
import json
import logging
import os
import shutil
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import schedule

from schemas import EVENTS_COLUMNS, EVENTS_NUMERIC, GKG_COLUMNS, MENTIONS_COLUMNS, MENTIONS_NUMERIC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PARQUET_PATH = Path(os.getenv("PARQUET_PATH", "/data/parquet"))
RAW_RETENTION_HOURS = int(os.getenv("RAW_RETENTION_HOURS", "1"))
GDELT_BASE_URL = os.getenv("GDELT_BASE_URL", "http://data.gdeltproject.org/gdeltv2")
STATE_FILE = PARQUET_PATH / ".loader_state.json"

FILE_TYPES = {
    "events": ".export.CSV.zip",
    "mentions": ".mentions.CSV.zip",
    "gkg": ".gkg.csv.zip",
}


def ensure_dirs():
    for sub in ("events", "mentions", "gkg", "raw"):
        (PARQUET_PATH / sub).mkdir(parents=True, exist_ok=True)


def load_state() -> set:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return set(json.load(f).get("downloaded", []))
    return set()


def save_state(downloaded: set):
    with open(STATE_FILE, "w") as f:
        json.dump({"downloaded": sorted(downloaded)[-500:]}, f)


def round_to_15min(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def get_timestamps_to_fetch(count: int = 4) -> list[str]:
    """Genera timestamps de los últimos N intervalos de 15 min (UTC)."""
    now = datetime.now(timezone.utc)
    current = round_to_15min(now) - timedelta(minutes=15)
    return [
        (current - timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M%S")
        for i in range(count)
    ]


def download_file(url: str) -> bytes | None:
    try:
        resp = requests.get(url, timeout=120)
        if resp.status_code == 404:
            logger.warning("Archivo no disponible: %s", url)
            return None
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as exc:
        logger.error("Error descargando %s: %s", url, exc)
        return None


def parse_csv_content(content: bytes, columns: list[str], numeric_cols: set) -> pd.DataFrame:
    text = content.decode("utf-8", errors="replace")
    df = pd.read_csv(
        io.StringIO(text),
        sep="\t",
        header=None,
        names=columns,
        dtype=str,
        on_bad_lines="skip",
        low_memory=False,
    )
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def save_parquet(df: pd.DataFrame, table: str, timestamp: str):
    out_dir = PARQUET_PATH / table / f"ts={timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.parquet"
    df.to_parquet(out_file, index=False, engine="pyarrow")
    logger.info("Guardado %s (%d filas)", out_file, len(df))


def process_timestamp(ts: str, downloaded: set) -> bool:
    key_prefix = f"{ts}"
    if any(f"{key_prefix}_{t}" in downloaded for t in FILE_TYPES):
        logger.info("Timestamp %s ya procesado, omitiendo", ts)
        return False

    any_success = False
    for table, suffix in FILE_TYPES.items():
        key = f"{key_prefix}_{table}"
        if key in downloaded:
            continue

        url = f"{GDELT_BASE_URL}/{ts}{suffix}"
        logger.info("Descargando %s", url)
        raw = download_file(url)
        if raw is None:
            continue

        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                csv_name = zf.namelist()[0]
                csv_content = zf.read(csv_name)

            if table == "events":
                df = parse_csv_content(csv_content, EVENTS_COLUMNS, EVENTS_NUMERIC)
            elif table == "mentions":
                df = parse_csv_content(csv_content, MENTIONS_COLUMNS, MENTIONS_NUMERIC)
            else:
                df = parse_csv_content(csv_content, GKG_COLUMNS, set())

            df["load_timestamp"] = ts
            save_parquet(df, table, ts)
            downloaded.add(key)
            any_success = True
        except Exception as exc:
            logger.error("Error procesando %s/%s: %s", ts, table, exc)

    save_state(downloaded)
    return any_success


def cleanup_old_raw():
    """Elimina particiones Parquet más antiguas que RAW_RETENTION_HOURS."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=RAW_RETENTION_HOURS)
    for table in ("events", "mentions", "gkg"):
        table_dir = PARQUET_PATH / table
        if not table_dir.exists():
            continue
        for part in table_dir.iterdir():
            if not part.is_dir() or not part.name.startswith("ts="):
                continue
            ts_str = part.name.replace("ts=", "")
            try:
                ts_dt = datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                if ts_dt < cutoff:
                    shutil.rmtree(part)
                    logger.info("Eliminada partición antigua: %s", part)
            except ValueError:
                continue


def run_loader():
    logger.info("Iniciando ciclo de carga GDELT")
    ensure_dirs()
    downloaded = load_state()
    timestamps = get_timestamps_to_fetch(count=4)

    for ts in timestamps:
        process_timestamp(ts, downloaded)

    cleanup_old_raw()
    logger.info("Ciclo de carga completado")


def main():
    import sys
    ensure_dirs()
    logger.info("Loader GDELT iniciado. Parquet path: %s", PARQUET_PATH)

    if "--once" in sys.argv:
        run_loader()
        return

    run_loader()
    schedule.every(15).minutes.do(run_loader)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
