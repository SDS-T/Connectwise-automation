```python
import requests
import base64
import pandas as pd
import os
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================================
# ENV VARIABLES
# ================================
cw_base_url = os.getenv("CW_BASE_URL")
public_key = os.getenv("CW_PUBLIC_KEY")
private_key = os.getenv("CW_PRIVATE_KEY")
clientid = os.getenv("CW_CLIENT_ID")

# ================================
# AUTH
# ================================
credentials = base64.b64encode(
    f"acs+{public_key}:{private_key}".encode()
).decode()

headers = {
    "Authorization": f"Basic {credentials}",
    "Accept": "application/vnd.connectwise.com+json; version=2022.1",
    "clientid": clientid
}

# ================================
# SESSION
# ================================
def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

# ================================
# LAST RUN
# ================================
def get_last_run_time():
    if os.path.exists("last_run.txt"):
        with open("last_run.txt", "r") as f:
            return f.read().strip(), False
    return None, True

def save_last_run_time():
    with open("last_run.txt", "w") as f:
        f.write(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

# ================================
# FETCH
# ================================
def fetch_tickets(last_run, is_first_run):
    session = create_session()
    all_data = []
    page = 1
    page_size = 1000

    while True:
        if is_first_run:
            endpoint = f"{cw_base_url}/service/tickets?page={page}&pagesize={page_size}"
        else:
            endpoint = (
                f"{cw_base_url}/service/tickets?"
                f"conditions=dateEntered > [{last_run}]"
                f"&page={page}&pagesize={page_size}"
            )

        res = session.get(endpoint, headers=headers)

        if res.status_code != 200:
            print(res.text)
            break

        data = res.json()
        if not data:
            break

        all_data.extend(data)

        if len(data) < page_size:
            break

        page += 1

    return all_data

# ================================
# 🔥 FIX FUNCTION (CORE LOGIC)
# ================================
def fix_datetime_column(series):
    series = series.astype(str)

    # Identify ISO rows (have timezone info)
    iso_mask = series.str.contains(r'Z|\\+\\d{2}:\\d{2}', na=False)

    # Parse ISO → UTC → IST
    iso_parsed = pd.to_datetime(series[iso_mask], utc=True, errors='coerce')
    iso_parsed = iso_parsed.dt.tz_convert('Asia/Kolkata')

    # Parse non-ISO → assume IST
    non_iso_parsed = pd.to_datetime(
        series[~iso_mask],
        format='%m-%d-%Y %H:%M',
        errors='coerce'
    ).dt.tz_localize('Asia/Kolkata')

    # Combine back
    final = pd.concat([iso_parsed, non_iso_parsed]).sort_index()

    # Format EXACT like ConnectWise UI
    return final.dt.strftime('%m-%d-%Y %H:%M')

# ================================
# MAIN
# ================================
last_run_time, is_first_run = get_last_run_time()
tickets = fetch_tickets(last_run_time, is_first_run)

csv_path = "tickets.csv"

if tickets:
    df = pd.json_normalize(tickets)

    if '_info.dateEntered' in df.columns:
        df['_info.dateEntered'] = fix_datetime_column(df['_info.dateEntered'])

    # Merge old CSV safely
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)

        if '_info.dateEntered' in existing_df.columns:
            existing_df['_info.dateEntered'] = fix_datetime_column(existing_df['_info.dateEntered'])

        df = pd.concat([existing_df, df], ignore_index=True)
        df.drop_duplicates(subset=["id"], keep="last", inplace=True)

    df.to_csv(csv_path, index=False)
    save_last_run_time()

    print("✅ CSV FIXED — All timestamps now match ConnectWise UI (IST)")

else:
    print("No data fetched")
```
