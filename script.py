import requests
import base64
import pandas as pd
import os
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
 
# ================================
# ENV VARIABLES (from GitHub Secrets)
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
    "clientid": clientid,
    "Connection": "keep-alive"
}
 
# ================================
# SESSION
# ================================
def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
 
# ================================
# TIME (LAST 30 MINUTES)
# ================================
lastrun_dt = datetime.utcnow() - timedelta(minutes=30)
lastrun_api = lastrun_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
 
print("Fetching data from:", lastrun_api)
 
# ================================
# FETCH DATA
# ================================
def all_open_tkts():
    session = create_session()
    total_open_tkt = []
    page = 1
    page_size = 1000
    boards = [
        "$ACS HelpDesk (MS)",
        "$ACS Implementation (MS)",
        "$ACS Implementation (PS)",
        "$ACS Procurement",
        "$ACS Quotes",
        "$ACS Recurring (MS)",
        "$ACS Sales",
        "$ACS Threat Detection",
        "$ACS Triage",
        "$Backup (MS)",
        "$Inspiroz Helpdesk (MS)",
        "$Inspiroz Implementation (MS)",
        "$Inspiroz Implementation (PS)",
        "$Inspiroz Procurement",
        "$Inspiroz Quotes",
        "$Inspiroz Recurring (MS)",
        "$Inspiroz Sales",
        "$Inspiroz Threat Detection",
        "$Inspiroz Triage",
        "$Internal",
        "$Re-Opened",
        "$RMM Alerts (MS)",
        "$TAM"
    ]
    owners = [
        "Prashant Tayade",
        "Shivaji Gupta",
        "Siddhesh Tawde",
        "Nagendra Rao",
        "Laxman Vengurlekar",
        "Devram Washivale",
        "Vikas Bhadvalkar",
        "Vinith Devraj",
        "Shubham Mishra",
        "Shreyash Ghadage",
        "Indra Singha",
        "Gaurav Dalvi"
    ]

    board_filter = " OR ".join([f"board/name='{b}'" for b in boards])
    owner_filter = " OR ".join([f"owner/name='{o}'" for o in owners])
    while True:
        print(f"\nFetching page {page}...")
 
        # endpoint = f"{cw_base_url}/service/tickets?conditions=(({board_filter}) AND dateEntered >= '{lastrun_api}')&page={page}&pagesize={page_size}&orderBy=dateEntered asc"
        endpoint = f"{cw_base_url}/service/tickets?conditions=((({board_filter}) AND ({owner_filter})) AND dateEntered >= '{lastrun_api}')&page={page}&pagesize={page_size}&orderBy=dateEntered asc"
        response = session.get(endpoint, headers=headers)
 
        if response.status_code != 200:
            print(f"API Error {response.status_code}: {response.text}")
            break
 
        data = response.json()
 
        if not isinstance(data, list):
            print("Unexpected response:", data)
            break
 
        count = len(data)
        print(f"Page {page} returned {count} records")
 
        total_open_tkt.extend(data)
 
        if count < page_size:
            break
 
        page += 1
 
    return total_open_tkt
 
# ================================
# EXECUTION
# ================================
output = all_open_tkts()
print(f"\nTotal records: {len(output)}")
 
# ================================
# SAVE (APPEND MODE)
# ================================
csv_path = "tickets.csv"
 
if len(output) > 0:
    df = pd.json_normalize(output)
 
    # Timezone conversion
    df['_info.dateEntered'] = pd.to_datetime(df['_info.dateEntered'], utc=True)
    df['_info.dateEntered'] = df['_info.dateEntered'].dt.tz_convert('US/Eastern')
 
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)
 
        df = pd.concat([existing_df, df])
        df = df.drop_duplicates(subset=['id'])
 
    df.to_csv(csv_path, index=False)
    print("CSV updated successfully")
 
else:
    print("No new data")
