"""Quick Senate eFD API health check."""
import requests
import re
from datetime import datetime

def check():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    })

    # Step 1: GET landing page
    resp = s.get("https://efdsearch.senate.gov/search/", timeout=30)
    csrf = s.cookies.get("csrftoken", "")

    # Step 2: Accept terms
    if "prohibition_agreement" in resp.text:
        s.post(
            "https://efdsearch.senate.gov/search/",
            data={"prohibition_agreement": "1", "csrfmiddlewaretoken": csrf},
            headers={"Referer": "https://efdsearch.senate.gov/search/"},
            timeout=30,
        )
        csrf = s.cookies.get("csrftoken", csrf)

    # Step 3: Try search API
    resp = s.post(
        "https://efdsearch.senate.gov/search/report/data/",
        data={
            "start": "0", "length": "5",
            "report_types": "[11]", "filer_types": "[]",
            "submitted_start_date": "01/01/2026",
            "submitted_end_date": "03/15/2026",
            "candidate_state": "", "senator_state": "",
            "office_id": "", "first_name": "", "last_name": "",
        },
        headers={
            "Referer": "https://efdsearch.senate.gov/search/home/",
            "X-CSRFToken": csrf,
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=30,
    )

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    if resp.status_code == 200 and "recordsTotal" in resp.text:
        data = resp.json()
        print(f"[{now}] ✅ Senate eFD API is UP — {data.get('recordsTotal', '?')} records found")
        return True
    elif "Maintenance" in resp.text:
        print(f"[{now}] ❌ Senate eFD API still under maintenance (HTTP {resp.status_code})")
        return False
    else:
        print(f"[{now}] ⚠️  Senate eFD API returned HTTP {resp.status_code}, not maintenance — possible parsing issue")
        print(f"    Content-Type: {resp.headers.get('content-type', '?')}")
        print(f"    Body preview: {resp.text[:200]}")
        return False

if __name__ == "__main__":
    check()
