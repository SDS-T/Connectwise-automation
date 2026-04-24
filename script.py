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
# SESSION WITH RETRY
# ================================
def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

# ================================
# READ START DATE
# ================================
def get_start_date():
    if os.path.exists("start_date.txt"):
        with open("start_date.txt", "r") as f:
            return f.read().strip()
    else:
        # Default fallback
        return "2026-01-01T00:00:00Z"

# ================================
# FETCH DATA (FULL LOAD)
# ================================
def fetch_tickets(start_date):
    session = create_session()
    all_data = []
    page = 1
    page_size = 1000

    boards = [
        "$ACS HelpDesk (MS)", "$ACS Implementation (MS)", "$ACS Implementation (PS)",
        "$ACS Procurement", "$ACS Quotes", "$ACS Recurring (MS)", "$ACS Sales",
        "$ACS Threat Detection", "$ACS Triage", "$Backup (MS)",
        "$Inspiroz Helpdesk (MS)", "$Inspiroz Implementation (MS)", "$Inspiroz Implementation (PS)",
        "$Inspiroz Procurement", "$Inspiroz Quotes", "$Inspiroz Recurring (MS)",
        "$Inspiroz Sales", "$Inspiroz Threat Detection", "$Inspiroz Triage",
        "$Internal", "$Re-Opened", "$RMM Alerts (MS)", "$TAM"
    ]

    owners = [
        "Prashant Tayade", "Shivaji Gupta", "Siddhesh Tawde",
        "Nagendra Rao", "Laxman Vengurlekar", "Devram Washivale",
        "Vikas Bhadvalkar", "Vinith Devraj", "Shubham Mishra",
        "Shreyash Ghadage", "Indra Singha", "Gaurav Dalvi"
    ]

    board_filter = " OR ".join([f"board/name='{b}'" for b in boards])
    owner_filter = " OR ".join([f"owner/name='{o}'" for o in owners])

    while True:
        print(f"\nFetching page {page}...")

        endpoint = (
            f"{cw_base_url}/service/tickets?"
            f"conditions=(({board_filter}) AND ({owner_filter}) AND dateEntered >= '{start_date}')"
            f"&page={page}&pagesize={page_size}&orderBy=dateEntered asc"
        )

        response = session.get(endpoint, headers=headers)

        if response.status_code != 200:
            print(f"API Error {response.status_code}: {response.text}")
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
start_date = get_start_date()
print("Fetching data from:", start_date)

tickets = fetch_tickets(start_date)
print("Total records fetched:", len(tickets))

csv_path = "tickets.csv"

if tickets:
    df = pd.json_normalize(tickets)

    # ✅ Convert dateEntered to UTC (keep consistent)
    if "_info.dateEntered" in df.columns:
        df["_info.dateEntered"] = pd.to_datetime(df["_info.dateEntered"], utc=True)

    # ❗ CRITICAL: overwrite file every time
    try:
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV overwritten successfully with {len(df)} records")
    except Exception as e:
        print(f"❌ ERROR writing CSV: {e}")
        exit(1)

else:
    print("No data fetched")
