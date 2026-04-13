import math
import os
import json
import re
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client
from postgrest.exceptions import APIError

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SECRET_KEY"]
ZENTRA_TOKEN = os.environ["ZENTRA_API_TOKEN"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DEVICE_SN = "z6-30197"
SERVER = "https://zentracloud.eu"
TABLE_NAME = "Zentra_SiteC"

BATCH_SIZE = 500


# ── How far back to backfill ──────────────────────────────────────────────────
BACKFILL_START = datetime(2026, 2, 1, tzinfo=timezone.utc)  # adjust as needed
CHUNK_DAYS     = 7   # fetch N days at a time to avoid API timeouts


# ── Zentra API ────────────────────────────────────────────────────────────────

def get_with_credentials(tok, uri, **kwargs):
    token = tok if tok.lower().startswith("token") else f"Token {tok}"
    return requests.get(uri, headers={"Authorization": token}, **kwargs)


def parse_df_payload(payload):
    parsed = json.loads(payload) if isinstance(payload, str) else payload
    return pd.DataFrame(**parsed)


def fetch_chunk(sn: str, start: datetime, end: datetime) -> pd.DataFrame:
    url    = f"{SERVER}/api/v4/get_readings/"
    params = {
        "device_sn": sn,
        "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date":   end.strftime("%Y-%m-%d %H:%M:%S"),
        "output_format": "df",
        "per_page": 1000,
        "page_num": 1,
        "sort_by": "asc",
        "location": True,
    }
    while True:
        res = get_with_credentials(ZENTRA_TOKEN, url, params=params, timeout=60)
        if res.ok:
            payload = res.json()
            if "data" not in payload or payload["data"] in (None, "", {}, []):
                return pd.DataFrame()
            return parse_df_payload(payload["data"])
        if "Exceeded request limit" in res.text:
            print("  Rate limit hit, waiting 60 s...")
            time.sleep(60)
            continue
        raise Exception(f"API error: {res.text}")


# ── Transform ─────────────────────────────────────────────────────────────────

def normalize_col(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")


def pivot_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["timestamp_utc", "measurement", "value"]].copy()
    df["measurement"] = df["measurement"].map(normalize_col)
    df["value"]       = pd.to_numeric(df["value"], errors="coerce")

    wide = df.pivot_table(
        index="timestamp_utc",
        columns="measurement",
        values="value",
        aggfunc="first",
    ).reset_index()

    wide.columns.name = None
    wide["device_sn"]   = DEVICE_SN
    wide["measured_at"] = pd.to_datetime(
        wide["timestamp_utc"], unit="s", utc=True
    ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    wide["ingested_at"] = datetime.now(timezone.utc).isoformat()

    return wide


# ── Write ─────────────────────────────────────────────────────────────────────

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


_dropped_cols: set[str] = set()


def write_wide(wide: pd.DataFrame) -> int:
    # Drop any columns we've already learned the table doesn't have
    if _dropped_cols:
        wide = wide.drop(columns=[c for c in _dropped_cols if c in wide.columns])

    records = [
        {k: (None if (v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))) else v)
         for k, v in row.items()}
        for row in wide.to_dict(orient="records")
    ]
    for batch in chunked(records, BATCH_SIZE):
        while True:
            try:
                supabase.table(TABLE_NAME).upsert(
                    batch, on_conflict="device_sn,timestamp_utc"
                ).execute()
                break
            except APIError as e:
                # PGRST204: column not found in schema cache
                if e.code == "PGRST204":
                    m = re.search(r"'([^']+)' column", e.message)
                    if m:
                        col = m.group(1)
                        print(f"  (column '{col}' not in table — dropping it)")
                        _dropped_cols.add(col)
                        batch = [{k: v for k, v in row.items() if k != col} for row in batch]
                        continue
                raise
    return len(records)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    end_date   = datetime.now(timezone.utc)
    chunk_start = BACKFILL_START
    total_written = 0

    # Build list of chunks upfront so we can show progress
    chunks = []
    while chunk_start < end_date:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_date)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    print(f"Backfilling {len(chunks)} chunks of {CHUNK_DAYS}d "
          f"from {BACKFILL_START.date()} → {end_date.date()}\n")

    for i, (start, end) in enumerate(chunks, 1):
        label = f"[{i}/{len(chunks)}] {start.date()} → {end.date()}"
        print(f"{label} — fetching...", end=" ", flush=True)

        df = fetch_chunk(DEVICE_SN, start, end)

        if df.empty:
            print("no data, skipping")
            continue

        wide    = pivot_to_wide(df)
        written = write_wide(wide)
        total_written += written
        print(f"{written} rows written (total: {total_written})")

        time.sleep(1)  # be polite to the API

    print(f"\nDone. Total rows written: {total_written}")


if __name__ == "__main__":
    main()
