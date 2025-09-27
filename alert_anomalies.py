from dotenv import load_dotenv
import os
import json
import snowflake.connector
import urllib.request

load_dotenv()

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="MARTS",
        client_session_keep_alive=True,
    )

def post_slack(webhook_url, text):
    data = {"text": text}
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status

if __name__ == "__main__":
    lookback_days = int(os.environ.get("ANOMALY_LOOKBACK_DAYS", "2"))
    slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
    db = os.environ["SNOWFLAKE_DATABASE"]
    
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Optional: sanity check the table exists
        cur.execute(f"show tables like 'ANOMALIES_DAILY' in schema {db}.STAGING_MARTS")

        cur.execute(f"""
          select date, city, bookings_total, zscore
          from {db}.STAGING_MARTS.ANOMALIES_DAILY
          where is_anomaly = true
            and date >= dateadd('day', -{lookback_days}, current_date())
          order by date desc, abs(zscore) desc
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        msg = f"No anomalies in the last {lookback_days} day(s)."
        print(msg)
        if slack_webhook:
            post_slack(slack_webhook, msg)
    else:
        lines = ["Anomalies detected:"]
        for d, city, total, z in rows:
            lines.append(f"- {d} | {city} | bookings={total} | z={z:.2f}")
        text = "\n".join(lines)
        print(text)
        if slack_webhook:
            post_slack(slack_webhook, text)
