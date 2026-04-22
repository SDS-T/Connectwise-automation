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
# FETCH DATA (FULL LOAD ONLY)
# ================================
def fetch_tickets():
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
            f"conditions=(({board_filter}) AND ({owner_filter}))"
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
# MAIN
# ================================
print(" Starting full refresh...")

tickets = fetch_tickets()
print("Total records fetched:", len(tickets))

csv_path = "tickets.csv"

if tickets:
    df = pd.json_normalize(tickets)

    # Keep raw values EXACTLY as API (no datetime conversion)
    df = df.astype(str)

    # ================================
    # DELETE OLD FILE (FULL REFRESH)
    # ================================
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print(" Old CSV deleted")

    # ================================
    # SAVE NEW FILE
    # ================================
    try:
        df.to_csv(csv_path, index=False)
        print(f" New CSV created with {len(df)} records")
    except Exception as e:
        print(f" ERROR writing CSV: {e}")
        exit(1)

else:
    print("No data fetched")
```
