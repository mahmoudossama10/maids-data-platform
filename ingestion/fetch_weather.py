import pandas as pd
import requests
from datetime import date, timedelta, datetime
from utils import get_conn, ensure_tables, merge_upsert

CITY_COORDS = {
    "Dubai": {"lat": 25.276987, "lon": 55.296249, "tz": "Asia/Dubai"},
    "Abu Dhabi": {"lat": 24.453884, "lon": 54.377344, "tz": "Asia/Dubai"},
    "Sharjah": {"lat": 25.346255, "lon": 55.421060, "tz": "Asia/Dubai"},
}


def fetch_city_daily(city: str, lat: float, lon: float, tz: str, start: date, end: date) -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/era5"
    daily_params = "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": daily_params,
        "timezone": tz,
    }

    r = requests.get(url, params=params, timeout=30)
    try:
        r.raise_for_status()
    except Exception:
        print(f"Request failed for {city}: {r.url}")
        raise

    j = r.json()
    daily = j.get("daily")
    if not daily or "time" not in daily:
        return pd.DataFrame()

    # Build DataFrame from the 'daily' dict
    df = pd.DataFrame(daily)

    # Convert time → date
    df["date"] = pd.to_datetime(df["time"], errors="coerce").dt.date

    # Rename columns to match our schema
    df = df.rename(
        columns={
            "temperature_2m_max": "temp_max",
            "temperature_2m_min": "temp_min",
            "precipitation_sum": "precipitation",
            "windspeed_10m_max": "windspeed_max",
        }
    )

    # Keep relevant columns
    df = df[["date", "temp_max", "temp_min", "precipitation", "windspeed_max"]]

    # Add metadata
    df["city"] = city
    df["updated_at"] = datetime.utcnow()

    # Final column order
    return df[[
        "city",
        "date",
        "temp_max",
        "temp_min",
        "precipitation",
        "windspeed_max",
        "updated_at",
    ]]


if __name__ == "__main__":
    # ERA5 archive usually lags a few days → cap end_date to 7 days ago
    end = date.today() - timedelta(days=7)
    start = end - timedelta(days=200)
    if start >= end:
        start = end - timedelta(days=60)

    frames = []
    for city, meta in CITY_COORDS.items():
        try:
            df = fetch_city_daily(city, meta["lat"], meta["lon"], meta["tz"], start, end)
            if not df.empty:
                frames.append(df)
                print(f"Fetched {len(df)} rows for {city}")
            else:
                print(f"No data for {city}")
        except Exception as e:
            print(f"Error fetching {city}: {e}")

    if not frames:
        print("No weather data fetched. Exiting.")
        raise SystemExit(1)

    all_weather = pd.concat(frames, ignore_index=True)

    # Ensure base tables exist
    conn = get_conn()
    try:
        ensure_tables(conn)
    finally:
        conn.close()

    # Upsert into RAW.WEATHER
    conn = get_conn()
    try:
        merge_upsert(
            conn,
            "RAW.WEATHER",
            all_weather,
            key_columns=["city", "date"],
            updated_col="updated_at",
        )
        print(f"Loaded RAW.WEATHER ({len(all_weather)} rows in this run)")
    finally:
        conn.close()
