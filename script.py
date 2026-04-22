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
                f"conditions=dateEntered > '{last_run}'"
                f"&page={page}&pagesize={page_size}"
            )

        res = session.get(endpoint, headers=headers)

        if res.status_code != 200:
            print("API Error:", res.status_code, res.text)
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
# 🔥 STRICT DATETIME PARSER
# ================================
def strict_datetime_parser(series):
    parsed_dates = []
    failed_rows = []

    for idx, val in series.items():
        try:
            raw = str(val).strip()

            if raw == "" or raw.lower() == "nan":
                parsed_dates.append("")
                failed_rows.append((idx, raw, "EMPTY"))
                continue

            # ISO / UTC format
            if "T" in raw or "+" in raw or "Z" in raw:
                dt = pd.to_datetime(raw, utc=True, errors='coerce')

                if pd.isna(dt):
                    parsed_dates.append(raw)
                    failed_rows.append((idx, raw, "ISO_PARSE_FAIL"))
                    continue

                dt = dt.tz_convert('Asia/Kolkata')
                parsed_dates.append(dt.strftime('%m-%d-%Y %H:%M'))
                continue

            # MM-DD-YYYY HH:MM
            try:
                dt = pd.to_datetime(raw, format='%m-%d-%Y %H:%M', errors='raise')
                dt = dt.tz_localize('Asia/Kolkata')
                parsed_dates.append(dt.strftime('%m-%d-%Y %H:%M'))
                continue
            except:
                pass

            # Fallback
            dt = pd.to_datetime(raw, errors='coerce')

            if pd.isna(dt):
                parsed_dates.append(raw)
                failed_rows.append((idx, raw, "UNKNOWN_FORMAT"))
                continue

            dt = dt.tz_localize('UTC').tz_convert('Asia/Kolkata')
            parsed_dates.append(dt.strftime('%m-%d-%Y %H:%M'))

        except Exception as e:
            parsed_dates.append(str(val))
            failed_rows.append((idx, val, str(e)))

    # Save debug file
    if failed_rows:
        debug_df = pd.DataFrame(failed_rows, columns=["index", "raw_value", "error"])
        debug_df.to_csv("datetime_parse_errors.csv", index=False)
        print(f"⚠️ {len(failed_rows)} rows had parsing issues (see datetime_parse_errors.csv)")

    return pd.Series(parsed_dates)

# ================================
# MAIN
# ================================
last_run_time, is_first_run = get_last_run_time()

tickets = fetch_tickets(last_run_time, is_first_run)

csv_path = "tickets.csv"

if tickets:
    df = pd.json_normalize(tickets)

    # Apply strict parser
    if '_info.dateEntered' in df.columns:
        df['_info.dateEntered'] = strict_datetime_parser(df['_info.dateEntered'])

    # Merge existing CSV
    if os.path.exists(csv_path):
        try:
            existing_df = pd.read_csv(csv_path)

            if '_info.dateEntered' in existing_df.columns:
                existing_df['_info.dateEntered'] = strict_datetime_parser(existing_df['_info.dateEntered'])

            df = pd.concat([existing_df, df], ignore_index=True)
            df.drop_duplicates(subset=["id"], keep="last", inplace=True)

        except Exception as e:
            print("Merge warning:", e)

    df.to_csv(csv_path, index=False)
    save_last_run_time()

    print("✅ SUCCESS: CSV updated with correct IST timestamps")

else:
    print("No data fetched")
```
