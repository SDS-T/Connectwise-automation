import requests
import base64
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
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
# SESSION WITH RETRY
# ================================
def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

# ================================
# GET LAST RUN TIME
# ================================
def get_last_run_time():
    if os.path.exists("last_run.txt"):
        with open("last_run.txt", "r") as f:
            return f.read().strip()
    else:
        return "2026-01-01T00:00:00Z"   # FIRST RUN

# ================================
# SAVE LAST RUN TIME
# ================================
def save_last_run_time():
    with open("last_run.txt", "w") as f:
        f.write(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

# ================================
# FETCH DATA
# ================================
def fetch_tickets(last_run):
    session = create_session()
    all_data = []
    page = 1
    page_size = 1000

    boards = ["$ACS HelpDesk (MS)", "$ACS Implementation (MS)", "$ACS Procurement"]
    owners = ["Prashant Tayade", "Shivaji Gupta"]

    board_filter = " OR ".join([f"board/name='{b}'" for b in boards])
    owner_filter = " OR ".join([f"owner/name='{o}'" for o in owners])

    while True:
        print(f"Fetching page {page}...")

        endpoint = (
            f"{cw_base_url}/service/tickets?"
            f"conditions=(({board_filter}) AND ({owner_filter}) AND dateEntered >= '{last_run}')"
            f"&page={page}&pagesize={page_size}"
        )

        response = session.get(endpoint, headers=headers)

        if response.status_code != 200:
            print("API Error:", response.text)
            break

        data = response.json()
        print(f"Records fetched: {len(data)}")

        if not data:
            break

        all_data.extend(data)

        if len(data) < page_size:
            break

        page += 1

    return all_data

# ================================
# MAIN EXECUTION
# ================================
last_run_time = get_last_run_time()
print("Last run time:", last_run_time)

tickets = fetch_tickets(last_run_time)
print("Total new records:", len(tickets))

csv_path = "tickets.csv"

if tickets:
    df = pd.json_normalize(tickets)

    # Convert time
    if "_info.dateEntered" in df.columns:
        df["_info.dateEntered"] = pd.to_datetime(df["_info.dateEntered"], utc=True)
        df["_info.dateEntered"] = df["_info.dateEntered"].dt.tz_convert("US/Eastern")

    # Append to existing
    if os.path.exists(csv_path):
        try:
            existing_df = pd.read_csv(csv_path)
            df = pd.concat([existing_df, df], ignore_index=True)
            df.drop_duplicates(subset=["id"], inplace=True)
        except pd.errors.EmptyDataError:
            print("Empty CSV, writing new file")

    df.to_csv(csv_path, index=False)
    print("CSV updated")

    # Save last run only if success
    save_last_run_time()

else:
    print("No new data")
