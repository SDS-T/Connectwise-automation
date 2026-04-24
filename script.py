import requests
import base64
import pandas as pd
import os
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================================
# ENV VARIABLES (GitHub Secrets)
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
# LAST RUN LOGIC
# ================================
def get_last_run_time():
    if os.path.exists("last_run.txt"):
        with open("last_run.txt", "r") as f:
            return f.read().strip(), False
    else:
        return None, True   # First run

def save_last_run_time():
    with open("last_run.txt", "w") as f:
        f.write(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

# ================================
# FETCH DATA
# ================================
def fetch_tickets(last_run, is_first_run):
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

        if is_first_run:
            endpoint = (
                f"{cw_base_url}/service/tickets?"
                f"conditions=(({board_filter}) AND ({owner_filter}))"
                f"&page={page}&pagesize={page_size}&orderBy=dateEntered asc"
            )
        else:
            endpoint = (
                f"{cw_base_url}/service/tickets?"
                f"conditions=(({board_filter}) AND ({owner_filter}) AND dateEntered >= '{last_run}')"
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
last_run_time, is_first_run = get_last_run_time()

print("Last run:", last_run_time)
print("First run:", is_first_run)

tickets = fetch_tickets(last_run_time, is_first_run)
print("Total records fetched:", len(tickets))

csv_path = "tickets.csv"

if tickets:
    df = pd.json_normalize(tickets)

    # ✅ Append + dedupe (FIXED)
    if os.path.exists(csv_path):
        try:
            existing_df = pd.read_csv(csv_path)
            df = pd.concat([existing_df, df], ignore_index=True)
            df.drop_duplicates(subset=["id"], inplace=True)
            print("Merged with existing CSV")
        except Exception as e:
            print(f"WARNING: Could not merge existing CSV: {e}")
            print("Proceeding with fresh data only")

    # ✅ Safe CSV write (CRITICAL FIX)
    try:
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV updated successfully with {len(df)} total records")

        # Save last run ONLY after success
        save_last_run_time()

    except Exception as e:
        print(f"❌ ERROR writing CSV: {e}")
        exit(1)

else:
    print("No new data fetched")


