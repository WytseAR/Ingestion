import os
import requests
from datetime import datetime, timezone
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SECRET_KEY"]
API_KEY = os.environ["CRYOSPHERE_API_KEY"]
DEPLOYMENT_ID = "e21fb253-349f-4072-a371-7ca47693cb2f"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

url = f"https://api.cryosphereinnovation.com/public/deployment/data/{DEPLOYMENT_ID}"

http_response = requests.get(
    url,
    headers={"Authorization": f"Bearer {API_KEY}"},
    timeout=60,
)
http_response.raise_for_status()

data = http_response.json()

rows = []
for item in data:
    ts = item.get("time_stamp")
    if ts is None:
        continue

    measured_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    rows.append({
        "deployment_id": DEPLOYMENT_ID,
        "external_id": item.get("_id"),
        "time_stamp": ts,
        "measured_at": measured_at,
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "air_temp": item.get("air_temp"),
        "air_pressure": item.get("air_pressure"),
        "bottom_distance": item.get("bottom_distance"),
        "water_temp": item.get("water_temp"),
        "surface_distance": item.get("surface_distance"),
        "incident": item.get("incident"),
        "reflected": item.get("reflected"),
        "battery_voltage": item.get("battery_voltage"),
        "gps_satellites": item.get("gps_satellites"),
        "iridium_signal": item.get("iridium_signal"),
        "iridium_retries": item.get("iridium_retries"),
        "filename": item.get("filename"),
        "raw_payload": item,
    })

if rows:
    supabase.table("SIMB3_SiteB").upsert(
        rows,
        on_conflict="deployment_id,time_stamp"
    ).execute()

print(f"{len(rows)} rows verwerkt")
