#!/usr/bin/env python3
"""
Boom Sales Dashboard — HubSpot Auto-Updater
Pulls meetings, companies, deals & owners from HubSpot and generates dashboard HTML.
Run manually or via macOS LaunchAgent for daily updates.
"""

import json, os, re, time, urllib.request, urllib.parse, urllib.error
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
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def _request_with_retry(req):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"    ⚠ API request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError("Unreachable")


def api_get(path, params=None):
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(url, headers=HEADERS)
    return _request_with_retry(req)


def api_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, headers=HEADERS, method="POST")
    return _request_with_retry(req)


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

    # Fetch company associations separately (search API doesn't reliably return them)
    print(f"    Fetching company associations for {len(unique)} meetings...")
    batch_size = 30
    for i in range(0, len(unique), batch_size):
        batch = unique[i:i + batch_size]
        ids = [{"id": m["id"]} for m in batch]
        body = {"inputs": ids}
        try:
            data = api_post("/crm/v4/associations/meetings/companies/batch/read", body)
            for r in data.get("results", []):
                mid = r.get("from", {}).get("id")
                to_list = r.get("to", [])
                if mid and to_list:
                    # Inject into the meeting's associations
                    for m in batch:
                        if m["id"] == mid:
                            m.setdefault("associations", {})["companies"] = {
                                "results": [{"id": t["toObjectId"]} for t in to_list]
                            }
                            break
        except Exception as e:
            print(f"    ⚠ Association batch failed: {e}")

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
    jan1 = int(datetime(2025, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)
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

    # Fetch company associations separately (search API doesn't reliably return them)
    print(f"    Fetching company associations for {len(deals)} deals...")
    batch_size = 30
    for i in range(0, len(deals), batch_size):
        batch = deals[i:i + batch_size]
        ids = [{"id": d["id"]} for d in batch]
        try:
            data = api_post("/crm/v4/associations/deals/companies/batch/read", {"inputs": ids})
            for r in data.get("results", []):
                did = r.get("from", {}).get("id")
                to_list = r.get("to", [])
                if did and to_list:
                    for d in batch:
                        if d["id"] == did:
                            d.setdefault("associations", {})["companies"] = {
                                "results": [{"id": t["toObjectId"]} for t in to_list]
                            }
                            break
        except Exception as e:
            print(f"    ⚠ Deal association batch failed: {e}")

    return deals


# ── Transform ───────────────────────────────────────────────────────────
# Companies to always exclude (post-sale but lifecycle not updated in HubSpot)
EXCLUDE_CIDS = {"36624870933"}  # Simple Life Rentals

def transform_meetings(raw, customer_cids=None):
    """Filter and transform meetings. Excludes non-sales titles and customer companies."""
    customer_cids = customer_cids or set()
    customer_cids = customer_cids | EXCLUDE_CIDS
    result = []
    for m in raw:
        p = m.get("properties", {})
        title = p.get("hs_meeting_title") or ""
        # Only keep sales meetings (Discovery / Demo in title)
        t_lower = title.lower()
        if "discovery" not in t_lower and "demo" not in t_lower:
            continue
        assoc = m.get("associations", {}).get("companies", {}).get("results", [])
        cid = assoc[0]["id"] if assoc else None
        # Exclude meetings with companies that are already customers (post-sale)
        if cid and str(cid) in customer_cids:
            continue
        start = p.get("hs_meeting_start_time", "")
        mtype = "demo" if "demo" in t_lower else "discovery"
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


# ── SDR Leads ──────────────────────────────────────────────────────────
SDR_PIPELINES = {
    "875086945": "Email Outbound",
    "882509401": "LinkedIn Outbound",
    "lead-pipeline-id": "Website Forms",
    "879627862": "Webinar",
    "879819597": "Conference",
}

SDR_STAGES = {
    # Email Outbound
    "1311814179": "New", "1311814180": "Attempting", "1311814181": "Connected",
    "1311814182": "Meeting Booked", "1313960619": "Qualified", "1311814183": "Disqualified",
    # LinkedIn Outbound
    "1326338614": "New", "1326338615": "Attempting", "1326338616": "Connected",
    "1326338617": "Meeting Booked", "1326338618": "Qualified", "1326338619": "Disqualified",
    # Website Forms
    "new-stage-id": "New", "attempting-stage-id": "Attempting", "connected-stage-id": "Connected",
    "1329066352": "Meeting Booked", "qualified-stage-id": "Qualified", "unqualified-stage-id": "Disqualified",
    # Webinar
    "1320256781": "Signed Up", "1320256780": "Attended", "1320255564": "Attempting",
    "1320255565": "Connected", "1320255566": "Meeting Booked", "1325789260": "Qualified", "1320255567": "Disqualified",
    # Conference
    "1320256319": "New", "1320256320": "Attempting", "1320256321": "Connected",
    "1320256322": "Meeting Booked", "1324914298": "Qualified", "1320256323": "Disqualified",
}

# Normalized stage order for funnel
STAGE_ORDER = ["New", "Signed Up", "Attended", "Attempting", "Connected", "Meeting Booked", "Qualified", "Disqualified"]


def fetch_sdr_leads():
    """Fetch all leads from the SDR pipelines."""
    leads = []
    after = 0
    while True:
        body = {
            "limit": 100,
            "after": after,
            "properties": [
                "hs_lead_name", "hs_pipeline", "hs_pipeline_stage",
                "hs_lead_status", "createdate", "hs_lastmodifieddate",
                "conference_name", "webinar_name", "hs_primary_contact_id"
            ],
        }
        data = api_post("/crm/v3/objects/leads/search", body)
        for r in data.get("results", []):
            leads.append(r)
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return leads


def fetch_contact_activity(leads):
    """Batch-read contact activity data for leads that have a primary contact."""
    contact_map = {}  # contact_id -> activity dict
    contact_ids = []
    for l in leads:
        cid = l.get("properties", {}).get("hs_primary_contact_id")
        if cid:
            contact_ids.append(str(int(float(cid))))

    if not contact_ids:
        return contact_map

    batch_size = 100
    for i in range(0, len(contact_ids), batch_size):
        batch = contact_ids[i:i + batch_size]
        try:
            data = api_post("/crm/v3/objects/contacts/batch/read", {
                "inputs": [{"id": cid} for cid in batch],
                "properties": [
                    "hs_email_last_send_date", "hs_email_last_reply_date",
                    "hs_email_last_open_date", "hs_email_sends_since_last_engagement",
                    "hs_sales_email_last_replied", "hs_email_optout",
                ],
            })
            for r in data.get("results", []):
                cp = r.get("properties", {})
                contact_map[r["id"]] = {
                    "emailed": bool(cp.get("hs_email_last_send_date")),
                    "replied": bool(cp.get("hs_email_last_reply_date") or cp.get("hs_sales_email_last_replied")),
                    "opened": bool(cp.get("hs_email_last_open_date")),
                    "lastSend": (cp.get("hs_email_last_send_date") or "")[:10],
                    "lastReply": (cp.get("hs_email_last_reply_date") or cp.get("hs_sales_email_last_replied") or "")[:10],
                    "optedOut": cp.get("hs_email_optout") == "true",
                }
        except Exception as e:
            print(f"    ⚠ Contact activity batch failed: {e}")

    return contact_map


def transform_sdr_leads(raw, contact_activity=None):
    contact_activity = contact_activity or {}
    result = []
    for l in raw:
        p = l.get("properties", {})
        pipeline = p.get("hs_pipeline") or ""
        if pipeline not in SDR_PIPELINES:
            continue
        stage_id = p.get("hs_pipeline_stage") or ""
        cid = p.get("hs_primary_contact_id")
        cid_str = str(int(float(cid))) if cid else None
        activity = contact_activity.get(cid_str, {})
        result.append({
            "id": l["id"],
            "name": p.get("hs_lead_name") or "",
            "pipeline": pipeline,
            "stage": SDR_STAGES.get(stage_id, stage_id),
            "stageId": stage_id,
            "created": (p.get("createdate") or "")[:10],
            "modified": (p.get("hs_lastmodifieddate") or "")[:10],
            "conference": p.get("conference_name"),
            "webinar": p.get("webinar_name"),
            "emailed": activity.get("emailed", False),
            "replied": activity.get("replied", False),
            "opened": activity.get("opened", False),
            "lastSend": activity.get("lastSend", ""),
            "lastReply": activity.get("lastReply", ""),
            "optedOut": activity.get("optedOut", False),
        })
    return result


# ── HTML Data Injection ──────────────────────────────────────────────────
# Instead of regenerating the entire HTML (which falls out of sync with
# manual UI changes), we read the existing index.html and replace only
# the data block between the ═══════ markers.

DATA_START = "// ═══════ DATA (auto-generated from HubSpot) ═══════"
DATA_END   = "// ═══════════════════════════════════════════════════"


def inject_data(meetings, companies, deals, owners, sdr_leads, updated_at):
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
        f"const SDR_LEADS = {json.dumps(sdr_leads)};",
        f"const SDR_PIPELINES = {json.dumps(SDR_PIPELINES)};",
        DATA_END,
    ])

    # Replace between markers (use lambda to avoid backslash interpretation in replacement)
    pattern = re.escape(DATA_START) + r".*?" + re.escape(DATA_END)
    html = re.sub(pattern, lambda _: data_block, html, flags=re.DOTALL)

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

    jul1_2025 = int(datetime(2025, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)

    print("  → Fetching owners...")
    raw_owners = fetch_owners()

    print("  → Fetching meetings...")
    raw_meetings = search_meetings(jul1_2025)
    print(f"    Found {len(raw_meetings)} sales meetings")

    print("  → Fetching companies...")
    raw_companies = fetch_companies()
    print(f"    Found {len(raw_companies)} companies with discovery stage")

    print("  → Fetching deals...")
    raw_deals = fetch_deals()
    print(f"    Found {len(raw_deals)} deals")

    # Build set of customer company IDs to exclude from sales meetings
    # Start with companies from our dataset
    customer_cids = set()
    for c in raw_companies:
        p = c.get("properties", {})
        if p.get("lifecyclestage") == "customer":
            customer_cids.add(str(c["id"]))

    # Also check lifecycle for all company IDs linked to meetings (some aren't in raw_companies)
    meeting_cids = set()
    for m in raw_meetings:
        assoc = m.get("associations", {}).get("companies", {}).get("results", [])
        for a in assoc:
            cid = str(a.get("id", ""))
            if cid and cid not in customer_cids:
                meeting_cids.add(cid)
    # Remove ones we already know about
    meeting_cids -= customer_cids
    if meeting_cids:
        print(f"    Checking lifecycle for {len(meeting_cids)} additional meeting-linked companies...")
        batch_size = 100
        check_list = list(meeting_cids)
        for i in range(0, len(check_list), batch_size):
            batch = check_list[i:i + batch_size]
            try:
                data = api_post("/crm/v3/objects/companies/batch/read", {
                    "inputs": [{"id": cid} for cid in batch],
                    "properties": ["lifecyclestage"],
                })
                for r in data.get("results", []):
                    if r.get("properties", {}).get("lifecyclestage") == "customer":
                        customer_cids.add(str(r["id"]))
            except Exception as e:
                print(f"    ⚠ Lifecycle check failed: {e}")

    print(f"    Found {len(customer_cids)} customer companies to exclude from meetings")

    meetings = transform_meetings(raw_meetings, customer_cids)
    companies = transform_companies(raw_companies)
    deals = transform_deals(raw_deals)
    owners = {k: v for k, v in raw_owners.items() if v.strip()}

    print("  → Fetching SDR leads...")
    raw_sdr = fetch_sdr_leads()
    print(f"    Fetching contact activity for {len(raw_sdr)} leads...")
    contact_activity = fetch_contact_activity(raw_sdr)
    sdr_leads = transform_sdr_leads(raw_sdr, contact_activity)
    emailed = sum(1 for l in sdr_leads if l.get("emailed"))
    replied = sum(1 for l in sdr_leads if l.get("replied"))
    print(f"    Found {len(sdr_leads)} SDR leads ({emailed} emailed, {replied} replied)")

    updated_at = datetime.now().strftime("%b %d, %Y %H:%M")
    html = inject_data(meetings, companies, deals, owners, sdr_leads, updated_at)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"  ✓ Dashboard saved to {OUTPUT_PATH}")
    print(f"  ✓ Done! {len(meetings)} meetings · {len(companies)} companies · {len(deals)} deals · {len(sdr_leads)} SDR leads")


if __name__ == "__main__":
    main()
