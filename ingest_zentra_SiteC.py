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
LOOKBACK_HOURS = 6

# ── Zentra API ────────────────────────────────────────────────────────────────

def get_with_credentials(tok, uri, **kwargs):
    token = tok if tok.lower().startswith("token") else f"Token {tok}"
    return requests.get(uri, headers={"Authorization": token}, **kwargs)


def parse_df_payload(payload):
    parsed = json.loads(payload) if isinstance(payload, str) else payload
    return pd.DataFrame(**parsed)


def fetch_chunk(sn: str, start: datetime, end: datetime) -> pd.DataFrame:
    url = f"{SERVER}/api/v4/get_readings/"
    params = {
        "device_sn": sn,
        "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end.strftime("%Y-%m-%d %H:%M:%S"),
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
            print("Rate limit hit, waiting 60 s...")
            time.sleep(60)
            continue
        raise Exception(f"API error: {res.text}")


# ── Transform ─────────────────────────────────────────────────────────────────

def normalize_col(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")


def pivot_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["timestamp_utc", "measurement", "value"]].copy()
    df["measurement"] = df["measurement"].map(normalize_col)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    wide = df.pivot_table(
        index="timestamp_utc",
        columns="measurement",
        values="value",
        aggfunc="first",
    ).reset_index()

    wide.columns.name = None
    wide["device_sn"] = DEVICE_SN
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
    if _dropped_cols:
        wide = wide.drop(columns=[c for c in _dropped_cols if c in wide.columns])

    records = [
        {
            k: (
                None
                if (
                    v is None
                    or (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
                )
                else v
            )
            for k, v in row.items()
        }
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
                if e.code == "PGRST204":
                    m = re.search(r"'([^']+)' column", e.message)
                    if m:
                        col = m.group(1)
                        print(f"Column '{col}' not in table — dropping it")
                        _dropped_cols.add(col)
                        batch = [{k: v for k, v in row.items() if k != col} for row in batch]
                        continue
                raise
    return len(records)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(hours=LOOKBACK_HOURS)

    print(f"Fetching last {LOOKBACK_HOURS} hours: {start_date} → {end_date}")

    df = fetch_chunk(DEVICE_SN, start_date, end_date)

    if df.empty:
        print("No data, skipping")
        return

    wide = pivot_to_wide(df)
    written = write_wide(wide)

    print(f"Done. Rows written: {written}")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
