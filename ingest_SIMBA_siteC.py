import os
import csv
import subprocess
from datetime import datetime, timedelta
from supabase import create_client


def download_csv_with_curl(
    url: str,
    output_file: str,
    username: str,
    password: str,
    cl: str,
    unit: str,
    tbl: str,
    sdt: str = None,
    edt: str = None,
) -> str:
    params = [f"cl={cl}", f"unit={unit}", f"tbl={tbl}"]
    if sdt:
        params.append(f"sdt={sdt.replace(' ', '%20')}")
    if edt:
        params.append(f"edt={edt.replace(' ', '%20')}")

    full_url = f"{url}?{'&'.join(params)}"

    cmd = [
        "curl",
        "-L",
        "-u", f"{username}:{password}",
        full_url,
        "-o", output_file,
    ]

    print("Downloading from:", full_url)
    subprocess.run(cmd, check=True)
    return output_file


def parse_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def chunked(items, size=500):
    for i in range(0, len(items), size):
        yield items[i:i + size]

def dedupe_payload(records, key_fields=("deployment_id", "time_stamp"), ts_key="measured_at"):
    """Deduplicate on the upsert conflict key, keeping the newest row by measured_at.
 
    Postgres rejects an ON CONFLICT DO UPDATE that touches the same key twice in
    one batch, so duplicates must be removed before upserting. Logs how many were
    dropped so structural duplication at the source stays visible.
    """
    seen = {}
    dropped = 0
    for r in records:
        k = tuple(r.get(f) for f in key_fields)
        if k not in seen or (r.get(ts_key) or "") > (seen[k].get(ts_key) or ""):
            if k in seen:
                dropped += 1
            seen[k] = r
        else:
            dropped += 1
    print(f"Dedupe: {len(records)} -> {len(seen)} rows ({dropped} duplicates removed)")
    return list(seen.values())

def main():
    simba_url = "https://simba.sams-enterprise.com/data/include/archive.php"
    username = os.environ["SIMBA_USERNAME"].strip()
    password = os.environ["SIMBA_PASSWORD"].strip()

    supabase_url = os.environ["SUPABASE_URL"].strip()
    supabase_key = os.environ["SUPABASE_KEY"].strip()

    print("Has SUPABASE_URL:", bool(supabase_url))
    print("Has SUPABASE_KEY:", bool(supabase_key))
    print("SIMBA_USERNAME length:", len(username))
    print("SIMBA_PASSWORD length:", len(password))

    if not username:
        raise ValueError("SIMBA_USERNAME is empty")
    if not password:
        raise ValueError("SIMBA_PASSWORD is empty")

    supabase = create_client(supabase_url, supabase_key)

    cl = "refl"
    unit = "ar0103"
    tbl = "tdp"

    
    sdt_str = '2026-02-13 16:00:00'
    edt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
 
    output_file = "/tmp/simba.csv"
 
    download_csv_with_curl(
        url=simba_url,
        output_file=output_file,
        username=username,
        password=password,
        cl=cl,
        unit=unit,
        tbl=tbl,
        sdt=sdt_str,
        edt=edt_str,
    )
 
    with open(output_file, "r", encoding="utf-8", errors="replace") as f:
        preview = f.read(500)
        print("File preview:")
        print(preview)
 
    if "Incorrect Username or Password" in preview:
        raise ValueError(
            "SIMBA authentication failed. Check SIMBA_USERNAME and SIMBA_PASSWORD secrets."
        )
 
    rows = parse_csv(output_file)
 
    print("CSV row count:", len(rows))
    if not rows:
        raise ValueError(f"No rows returned from SIMBA for {sdt_str} to {edt_str}")
 
    print("CSV columns:", list(rows[0].keys()))
    print("First row sample:", rows[0])
 
    ingested_at = datetime.now(timezone.utc).isoformat()
 
    payload_rows = []
    for row in rows:
        payload_rows.append({
            "deployment_id": "SiteA",
            "time_stamp": row.get("MOMSN"),
            "measured_at": row.get("Send Time"),
            "filename": os.path.basename(output_file),
            "raw_payload": row,
            "ingested_at": ingested_at,
        })
 
    print("Prepared rows:", len(payload_rows))
 
    payload_rows = dedupe_payload(payload_rows)
 
    if not payload_rows:
        raise ValueError("No rows left after deduplication")
 
    print("First payload row:", payload_rows[0])
 
    for i, batch in enumerate(chunked(payload_rows, 500), start=1):
        result = (
            supabase
            .table("SIMBA_SiteA")
            .upsert(batch, on_conflict="deployment_id,time_stamp")
            .execute()
        )
        print(f"Upserted batch {i}, size {len(batch)}")
        print(result)
 
    print(f"{len(payload_rows)} rows loaded into Supabase.")
 
 
if __name__ == "__main__":
    main()
