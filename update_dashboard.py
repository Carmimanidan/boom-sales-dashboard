#!/usr/bin/env python3
"""
Boom Sales Dashboard — HubSpot Auto-Updater
Pulls meetings, companies, deals & owners from HubSpot and generates dashboard HTML.
Run manually or via macOS LaunchAgent for daily updates.
"""

import json, os, re, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
ENV_PATH = SCRIPT_DIR / ".env"
OUTPUT_PATH = SCRIPT_DIR / "index.html"

def load_token():
    with open(ENV_PATH) as f:
        for line in f:
            if line.startswith("HUBSPOT_TOKEN="):
                return line.strip().split("=", 1)[1]
    raise RuntimeError("HUBSPOT_TOKEN not found in .env")

TOKEN = load_token()
BASE = "https://api.hubapi.com"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Sales pipeline & stage mapping
SALES_PIPELINE = "93413737"
DEAL_STAGES = {
    "1075455291": "Discovery Completed",
    "1075460493": "Negotiation",
    "171811493": "Attack List",
    "1275009440": "Contract Sent",
    "1108564665": "Closed Won",
    "216501682": "Closed Lost",
    "1236301393": "Churn / Inactive",
}

OWNER_CACHE = {}


# ── API Helpers ─────────────────────────────────────────────────────────
def api_get(path, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# ── Data Fetchers ───────────────────────────────────────────────────────
def fetch_owners():
    owners = {}
    after = None
    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after
        data = api_get("/crm/v3/owners/", params)
        for o in data.get("results", []):
            name = f"{o.get('firstName', '')} {o.get('lastName', '')}".strip()
            if name:
                owners[o["id"]] = name
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return owners


def search_meetings(start_ts):
    """Search for sales meetings (Discovery + Demo) from start_ts onwards."""
    meetings = []
    for query in ["Boom Discovery", "Boom Demo", "Discovery Meeting"]:
        after = 0
        while True:
            body = {
                "query": query,
                "filterGroups": [{"filters": [
                    {"propertyName": "hs_meeting_start_time", "operator": "GTE", "value": str(start_ts)}
                ]}],
                "properties": [
                    "hs_meeting_title", "hs_meeting_start_time", "hs_meeting_end_time",
                    "hs_meeting_outcome", "hubspot_owner_id"
                ],
                "associations": ["companies"],
                "sorts": [{"propertyName": "hs_meeting_start_time", "direction": "ASCENDING"}],
                "limit": 100,
                "after": after,
            }
            data = api_post("/crm/v3/objects/meetings/search", body)
            for m in data.get("results", []):
                meetings.append(m)
            paging = data.get("paging", {}).get("next", {})
            after = paging.get("after")
            if not after:
                break
    # Dedupe by ID
    seen = set()
    unique = []
    for m in meetings:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique.append(m)
    return unique


def fetch_companies():
    """Fetch companies with discovery_meeting_stage set."""
    companies = []
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "discovery_meeting_stage", "operator": "HAS_PROPERTY"}
        ]}],
        "properties": [
            "name", "listings_count", "current_pms__company_", "country",
            "lifecyclestage", "hubspot_owner_id", "discovery_meeting_stage",
            "completed_meetings_counter", "domain", "listing_count_bucket",
            "company_location__country_"
        ],
        "sorts": [{"propertyName": "notes_last_updated", "direction": "DESCENDING"}],
        "limit": 100,
        "after": 0,
    }
    while True:
        data = api_post("/crm/v3/objects/companies/search", body)
        for c in data.get("results", []):
            companies.append(c)
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
        body["after"] = int(after)
    return companies


def fetch_deals():
    """Fetch deals created in 2026 from the main sales pipeline."""
    deals = []
    jan1 = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "createdate", "operator": "GTE", "value": str(jan1)},
            {"propertyName": "pipeline", "operator": "EQ", "value": SALES_PIPELINE},
        ]}],
        "properties": [
            "dealname", "dealstage", "pipeline", "amount",
            "closedate", "hubspot_owner_id", "createdate"
        ],
        "associations": ["companies"],
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "limit": 100,
        "after": 0,
    }
    while True:
        data = api_post("/crm/v3/objects/deals/search", body)
        for d in data.get("results", []):
            deals.append(d)
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
        body["after"] = int(after)
    return deals


# ── Transform ───────────────────────────────────────────────────────────
def transform_meetings(raw):
    result = []
    for m in raw:
        p = m.get("properties", {})
        title = p.get("hs_meeting_title") or ""
        start = p.get("hs_meeting_start_time", "")
        mtype = "demo" if "demo" in title.lower() else "discovery"
        assoc = m.get("associations", {}).get("companies", {}).get("results", [])
        cid = assoc[0]["id"] if assoc else None
        result.append({
            "id": m["id"],
            "date": start[:10] if start else None,
            "owner": p.get("hubspot_owner_id"),
            "outcome": p.get("hs_meeting_outcome") or None,
            "title": title,
            "type": mtype,
            "cid": cid,
        })
    result.sort(key=lambda x: x["date"] or "")
    # Dedupe by title+date+owner — keep the one with the most definitive outcome
    outcome_rank = {"COMPLETED": 5, "NO_SHOW": 4, "RESCHEDULED": 3, "CANCELED": 2, "SCHEDULED": 1}
    seen = {}
    for m in result:
        key = (m["date"], m["title"], m["owner"])
        rank = outcome_rank.get(m["outcome"] or "", 0)
        if key not in seen or rank > seen[key][1]:
            seen[key] = (m, rank)
    return [v[0] for v in sorted(seen.values(), key=lambda v: v[0]["date"] or "")]


def transform_companies(raw):
    result = []
    for c in raw:
        p = c.get("properties", {})
        name = p.get("name")
        if not name or name.lower() in ("test", "j"):
            continue
        lc = p.get("listings_count")
        result.append({
            "id": c["id"],
            "name": name,
            "listings": int(lc) if lc and lc.isdigit() else None,
            "pms": p.get("current_pms__company_"),
            "country": p.get("country") or p.get("company_location__country_"),
            "stage": p.get("discovery_meeting_stage"),
            "lifecycle": p.get("lifecyclestage"),
            "owner": p.get("hubspot_owner_id"),
            "domain": p.get("domain"),
        })
    return result


def transform_deals(raw):
    result = []
    for d in raw:
        p = d.get("properties", {})
        name = p.get("dealname", "")
        if name.lower() in ("rfghgrfd", "sdfghj"):
            continue
        amt = p.get("amount")
        cd = p.get("closedate")
        assoc = d.get("associations", {}).get("companies", {}).get("results", [])
        cid = assoc[0]["id"] if assoc else None
        result.append({
            "id": d["id"],
            "name": name,
            "amount": float(amt) if amt else 0,
            "stage": p.get("dealstage"),
            "owner": p.get("hubspot_owner_id"),
            "created": (p.get("createdate") or "")[:10],
            "closed": cd[:10] if cd else None,
            "cid": cid,
        })
    return result


# ── HTML Data Injection ──────────────────────────────────────────────────
# Instead of regenerating the entire HTML (which falls out of sync with
# manual UI changes), we read the existing index.html and replace only
# the data block between the ═══════ markers.

DATA_START = "// ═══════ DATA (auto-generated from HubSpot) ═══════"
DATA_END   = "// ═══════════════════════════════════════════════════"


def inject_data(meetings, companies, deals, owners, updated_at):
    """Read index.html, replace the data block, update the timestamp."""
    html = OUTPUT_PATH.read_text(encoding="utf-8")

    # Build new data block
    data_block = "\n".join([
        DATA_START,
        f"const OWNERS = {json.dumps(owners)};",
        f"const MEETINGS = {json.dumps(meetings)};",
        f"const COMPANIES = {json.dumps(companies)};",
        f"const DEALS = {json.dumps(deals)};",
        f"const DEAL_STAGES = {json.dumps(DEAL_STAGES)};",
        DATA_END,
    ])

    # Replace between markers
    pattern = re.escape(DATA_START) + r".*?" + re.escape(DATA_END)
    html = re.sub(pattern, data_block, html, flags=re.DOTALL)

    # Update timestamp in nav
    html = re.sub(
        r'(Updated:\s*)[^<]+',
        rf'\g<1>{updated_at}',
        html,
        count=1,
    )

    return html


# ── Main ────────────────────────────────────────────────────────────────
def main():
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] Boom Dashboard — pulling from HubSpot...")

    jan1_2026 = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

    print("  → Fetching owners...")
    raw_owners = fetch_owners()

    print("  → Fetching meetings...")
    raw_meetings = search_meetings(jan1_2026)
    print(f"    Found {len(raw_meetings)} sales meetings")

    print("  → Fetching companies...")
    raw_companies = fetch_companies()
    print(f"    Found {len(raw_companies)} companies with discovery stage")

    print("  → Fetching deals...")
    raw_deals = fetch_deals()
    print(f"    Found {len(raw_deals)} deals")

    meetings = transform_meetings(raw_meetings)
    companies = transform_companies(raw_companies)
    deals = transform_deals(raw_deals)
    owners = {k: v for k, v in raw_owners.items() if v.strip()}

    updated_at = datetime.now().strftime("%b %d, %Y %H:%M")
    html = inject_data(meetings, companies, deals, owners, updated_at)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"  ✓ Dashboard saved to {OUTPUT_PATH}")
    print(f"  ✓ Done! {len(meetings)} meetings · {len(companies)} companies · {len(deals)} deals")


if __name__ == "__main__":
    main()
