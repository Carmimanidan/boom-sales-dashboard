#!/usr/bin/env python3
"""
Boom Sales Dashboard — HubSpot Auto-Updater
Pulls meetings, companies, deals & owners from HubSpot and generates dashboard HTML.
Run manually or via macOS LaunchAgent for daily updates.
"""

import json, os, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
ENV_PATH = SCRIPT_DIR / ".env"
OUTPUT_PATH = SCRIPT_DIR / "dashboard.html"

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
        result.append({
            "id": m["id"],
            "date": start[:10] if start else None,
            "owner": p.get("hubspot_owner_id"),
            "outcome": p.get("hs_meeting_outcome") or None,
            "title": title,
            "type": mtype,
        })
    result.sort(key=lambda x: x["date"] or "")
    return result


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
        result.append({
            "id": d["id"],
            "name": name,
            "amount": float(amt) if amt else 0,
            "stage": p.get("dealstage"),
            "owner": p.get("hubspot_owner_id"),
            "created": (p.get("createdate") or "")[:10],
            "closed": cd[:10] if cd else None,
        })
    return result


# ── HTML Generator ──────────────────────────────────────────────────────
def generate_html(meetings, companies, deals, owners, updated_at):
    owners_js = json.dumps(owners)
    meetings_js = json.dumps(meetings)
    companies_js = json.dumps(companies)
    deals_js = json.dumps(deals)
    stages_js = json.dumps(DEAL_STAGES)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Boom Sales Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',sans-serif;background:#0a0e1a;color:#e2e8f0;min-height:100vh}}
.dash{{max-width:1480px;margin:0 auto;padding:0 36px 60px}}

/* ── Nav ── */
.nav{{position:sticky;top:0;z-index:100;background:rgba(10,14,26,.85);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,.06);padding:16px 36px;margin:0 -36px 32px;display:flex;align-items:center;justify-content:space-between}}
.nav-left h1{{font-size:22px;font-weight:800;background:linear-gradient(135deg,#60a5fa,#a78bfa,#f472b6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.nav-left .sub{{font-size:12px;color:#475569;margin-top:2px}}
.nav-center{{display:flex;align-items:center;gap:8px}}
.nav-center button{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);color:#94a3b8;border-radius:8px;padding:8px 14px;cursor:pointer;font:600 13px Inter;transition:.2s}}
.nav-center button:hover,.nav-center button.active{{background:rgba(96,165,250,.15);color:#60a5fa;border-color:rgba(96,165,250,.3)}}
.week-label{{font-size:14px;font-weight:600;color:#e2e8f0;min-width:200px;text-align:center}}
.nav-right{{font-size:11px;color:#334155}}
.view-btns{{display:flex;gap:4px;margin-left:16px}}
.view-btns button{{padding:6px 12px;border-radius:6px;font-size:11px;text-transform:uppercase;letter-spacing:.5px}}

/* ── KPI ── */
.kpis{{display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:28px}}
.kpi{{background:linear-gradient(145deg,rgba(255,255,255,.05),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.06);border-radius:16px;padding:18px 22px;position:relative;overflow:hidden;transition:.2s}}
.kpi:hover{{transform:translateY(-2px);box-shadow:0 8px 30px rgba(0,0,0,.3)}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:16px 16px 0 0}}
.kpi:nth-child(1)::before{{background:linear-gradient(90deg,#60a5fa,#3b82f6)}}
.kpi:nth-child(2)::before{{background:linear-gradient(90deg,#34d399,#10b981)}}
.kpi:nth-child(3)::before{{background:linear-gradient(90deg,#a78bfa,#8b5cf6)}}
.kpi:nth-child(4)::before{{background:linear-gradient(90deg,#fbbf24,#f59e0b)}}
.kpi:nth-child(5)::before{{background:linear-gradient(90deg,#34d399,#059669)}}
.kpi:nth-child(6)::before{{background:linear-gradient(90deg,#f472b6,#ec4899)}}
.kpi-lbl{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:#64748b;margin-bottom:6px}}
.kpi-val{{font-size:30px;font-weight:800;letter-spacing:-1px;line-height:1}}
.kpi:nth-child(1) .kpi-val{{color:#60a5fa}}.kpi:nth-child(2) .kpi-val{{color:#34d399}}.kpi:nth-child(3) .kpi-val{{color:#a78bfa}}.kpi:nth-child(4) .kpi-val{{color:#fbbf24}}.kpi:nth-child(5) .kpi-val{{color:#34d399}}.kpi:nth-child(6) .kpi-val{{color:#f472b6}}
.kpi-det{{font-size:11px;color:#475569;margin-top:4px}}

/* ── Cards & Grid ── */
.grid{{display:grid;gap:20px;margin-bottom:24px}}.g2{{grid-template-columns:1fr 1fr}}.g21{{grid-template-columns:2fr 1fr}}.g12{{grid-template-columns:1fr 2fr}}.g3{{grid-template-columns:1fr 1fr 1fr}}
.card{{background:linear-gradient(145deg,rgba(255,255,255,.05),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.06);border-radius:18px;padding:24px}}
.card-t{{font-size:13px;font-weight:600;color:#94a3b8;margin-bottom:16px;text-transform:uppercase;letter-spacing:.5px}}
.cc{{position:relative;width:100%}}.cc.m{{height:270px}}.cc.s{{height:210px}}.cc.t{{height:330px}}

/* ── Pipeline ── */
.pipe{{display:flex;gap:6px;margin-bottom:28px}}
.pipe-s{{flex:1;text-align:center;padding:16px 10px;border-radius:14px;transition:.2s}}
.pipe-s:hover{{transform:scale(1.03)}}
.pipe-n{{font-size:26px;font-weight:800;line-height:1}}.pipe-l{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.8px;margin-top:4px;opacity:.8}}.pipe-d{{font-size:11px;margin-top:3px;opacity:.6}}
.ps1{{background:rgba(96,165,250,.12);color:#60a5fa}}.ps2{{background:rgba(251,191,36,.12);color:#fbbf24}}.ps3{{background:rgba(167,139,250,.12);color:#a78bfa}}.ps4{{background:rgba(244,114,182,.12);color:#f472b6}}.ps5{{background:rgba(52,211,153,.12);color:#34d399}}.ps6{{background:rgba(100,116,139,.12);color:#94a3b8}}
.pipe-arr{{display:flex;align-items:center;color:#1e293b;font-size:18px}}

/* ── Heatmap ── */
.hm{{display:grid;gap:5px;font-size:12px}}.hm-h{{font-weight:600;color:#475569;text-align:center;padding:5px;font-size:9px;text-transform:uppercase;letter-spacing:.5px}}.hm-l{{display:flex;align-items:center;font-weight:500;color:#94a3b8;font-size:11px}}
.hm-c{{text-align:center;padding:8px 4px;border-radius:7px;font-weight:700;font-size:13px;transition:.15s}}.hm-c:hover{{transform:scale(1.08)}}
.h0{{background:rgba(255,255,255,.02);color:#1e293b}}.h1{{background:rgba(96,165,250,.1);color:#60a5fa}}.h2{{background:rgba(96,165,250,.2);color:#60a5fa}}.h3{{background:rgba(96,165,250,.3);color:#93c5fd}}.h4{{background:rgba(96,165,250,.45);color:#bfdbfe}}.h5{{background:rgba(96,165,250,.6);color:#fff}}

/* ── Tables ── */
.tbl{{width:100%;border-collapse:separate;border-spacing:0 3px}}
.tbl th{{text-align:left;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:#475569;padding:7px 10px;cursor:pointer}}.tbl th:hover{{color:#94a3b8}}
.tbl td{{padding:8px 10px;font-size:12px;font-weight:500}}
.tbl tbody tr{{background:rgba(255,255,255,.025);transition:.15s}}.tbl tbody tr:hover{{background:rgba(255,255,255,.06)}}
.tbl tbody tr td:first-child{{border-radius:7px 0 0 7px}}.tbl tbody tr td:last-child{{border-radius:0 7px 7px 0}}
.badge{{display:inline-block;padding:2px 9px;border-radius:99px;font-size:10px;font-weight:600}}
.b-grn{{background:rgba(52,211,153,.15);color:#34d399}}.b-red{{background:rgba(244,114,182,.15);color:#f472b6}}.b-ylw{{background:rgba(251,191,36,.15);color:#fbbf24}}.b-blu{{background:rgba(96,165,250,.15);color:#60a5fa}}.b-prp{{background:rgba(167,139,250,.15);color:#a78bfa}}.b-gry{{background:rgba(100,116,139,.15);color:#94a3b8}}
.scroll-box{{max-height:400px;overflow-y:auto;padding-right:6px}}
.scroll-box::-webkit-scrollbar{{width:4px}}.scroll-box::-webkit-scrollbar-track{{background:transparent}}.scroll-box::-webkit-scrollbar-thumb{{background:#1e293b;border-radius:4px}}
.sec-lbl{{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:#334155;margin:4px 0 16px}}
@media(max-width:1100px){{.kpis{{grid-template-columns:repeat(3,1fr)}}.g2,.g21,.g12,.g3{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="dash">

<!-- Nav -->
<div class="nav">
  <div class="nav-left">
    <h1>Boom Sales Dashboard</h1>
    <div class="sub">HubSpot Live Data &middot; 2026</div>
  </div>
  <div class="nav-center">
    <button id="prevW" onclick="changeWeek(-1)">&larr;</button>
    <div class="week-label" id="weekLabel">All Time</div>
    <button id="nextW" onclick="changeWeek(1)">&rarr;</button>
    <div class="view-btns">
      <button class="active" onclick="setView('all',this)">All</button>
      <button onclick="setView('weekly',this)">Weekly</button>
      <button onclick="setView('monthly',this)">Monthly</button>
    </div>
  </div>
  <div class="nav-right">Updated: {updated_at}</div>
</div>

<!-- KPIs -->
<div class="kpis" id="kpis"></div>

<!-- Pipeline -->
<div class="sec-lbl">Deal Pipeline</div>
<div class="pipe" id="pipeline"></div>

<!-- Charts Row 1 -->
<div class="grid g2">
  <div class="card"><div class="card-t">Meetings Per Week</div><div class="cc m"><canvas id="chWeekly"></canvas></div></div>
  <div class="card"><div class="card-t">Meeting Outcomes</div><div class="cc m"><canvas id="chOutcome"></canvas></div></div>
</div>

<!-- Charts Row 2 -->
<div class="grid g21">
  <div class="card"><div class="card-t">Sales Agent Activity</div><div class="cc m"><canvas id="chAgent"></canvas></div></div>
  <div class="card"><div class="card-t">Company Pipeline Stage</div><div class="cc m"><canvas id="chStage"></canvas></div></div>
</div>

<!-- Charts Row 3 -->
<div class="grid g2">
  <div class="card"><div class="card-t">PMS Distribution</div><div class="cc m"><canvas id="chPms"></canvas></div></div>
  <div class="card"><div class="card-t">Prospect Size (Listings)</div><div class="cc m"><canvas id="chSize"></canvas></div></div>
</div>

<!-- Heatmap -->
<div class="card" style="margin-bottom:24px"><div class="card-t">Agent &times; Week Heatmap</div><div id="heatmap"></div></div>

<!-- Tables -->
<div class="grid g2">
  <div class="card"><div class="card-t">Deals</div><div class="scroll-box" id="dealsBox"></div></div>
  <div class="card"><div class="card-t">Company Pipeline</div><div class="scroll-box" id="compBox"></div></div>
</div>

</div>

<script>
// ═══════ DATA (auto-generated from HubSpot) ═══════
const OWNERS = {owners_js};
const MEETINGS = {meetings_js};
const COMPANIES = {companies_js};
const DEALS = {deals_js};
const DEAL_STAGES = {stages_js};
// ═══════════════════════════════════════════════════

// ── Globals ──
Chart.defaults.color='#64748b';
Chart.defaults.font.family="'Inter',sans-serif";
Chart.defaults.font.size=11;
Chart.defaults.plugins.legend.labels.padding=14;
Chart.defaults.plugins.legend.labels.usePointStyle=true;
const GC='rgba(255,255,255,.04)';
const TT={{backgroundColor:'#1e293b',titleColor:'#e2e8f0',bodyColor:'#94a3b8',borderColor:'rgba(255,255,255,.1)',borderWidth:1,cornerRadius:8,padding:12}};

let currentView='all', currentIdx=-1, charts={{}};
const SALES_AGENTS=["604265803","86713013","84989259","78025138"];

// ── Week Utils ──
function isoWeek(ds){{const d=new Date(ds+'T00:00:00Z');d.setUTCDate(d.getUTCDate()+4-(d.getUTCDay()||7));const y=new Date(Date.UTC(d.getUTCFullYear(),0,1));return Math.ceil(((d-y)/864e5+1)/7)}}
function weekRange(y,w){{const jan4=new Date(Date.UTC(y,0,4));const d=jan4.getUTCDay()||7;const mon=new Date(jan4);mon.setUTCDate(jan4.getUTCDate()-d+1+(w-1)*7);const sun=new Date(mon);sun.setUTCDate(mon.getUTCDate()+6);const fmt=d=>d.toLocaleDateString('en-US',{{month:'short',day:'numeric',timeZone:'UTC'}});return fmt(mon)+' – '+fmt(sun)}}

// Build week list
const allWeeks=[...new Set(MEETINGS.filter(m=>m.date).map(m=>isoWeek(m.date)))].sort((a,b)=>a-b);

function getFilteredMeetings(){{
  if(currentView==='all'||currentIdx<0) return MEETINGS;
  if(currentView==='weekly'){{const w=allWeeks[currentIdx];return MEETINGS.filter(m=>m.date&&isoWeek(m.date)===w)}}
  if(currentView==='monthly'){{const mo=currentIdx;return MEETINGS.filter(m=>m.date&&new Date(m.date+'T00:00:00Z').getUTCMonth()===mo)}}
  return MEETINGS;
}}

function getFilteredDeals(){{
  if(currentView==='all'||currentIdx<0) return DEALS;
  if(currentView==='weekly'){{const w=allWeeks[currentIdx];return DEALS.filter(d=>d.created&&isoWeek(d.created)===w)}}
  if(currentView==='monthly'){{const mo=currentIdx;return DEALS.filter(d=>d.created&&new Date(d.created+'T00:00:00Z').getUTCMonth()===mo)}}
  return DEALS;
}}

// ── KPIs ──
function renderKPIs(){{
  const fm=getFilteredMeetings(), fd=getFilteredDeals();
  const completed=fm.filter(m=>m.outcome==='COMPLETED').length;
  const pipeStages=["1075460493","171811493","1275009440"];
  const pipeDeals=fd.filter(d=>pipeStages.includes(d.stage));
  const pipeVal=pipeDeals.reduce((s,d)=>s+d.amount,0);
  const wonDeals=fd.filter(d=>d.stage==="1108564665");
  const wonVal=wonDeals.reduce((s,d)=>s+d.amount,0);
  const fmt=n=>n.toLocaleString('en-US');
  const fmtD=n=>'$'+n.toLocaleString('en-US',{{maximumFractionDigits:0}});
  document.getElementById('kpis').innerHTML=`
    <div class="kpi"><div class="kpi-lbl">Total Meetings</div><div class="kpi-val">${{fmt(fm.length)}}</div><div class="kpi-det">Discovery + Demo</div></div>
    <div class="kpi"><div class="kpi-lbl">Completed</div><div class="kpi-val">${{fmt(completed)}}</div><div class="kpi-det">${{fm.length?Math.round(completed/fm.length*100):0}}% completion</div></div>
    <div class="kpi"><div class="kpi-lbl">Deals Created</div><div class="kpi-val">${{fmt(fd.length)}}</div><div class="kpi-det">Sales pipeline</div></div>
    <div class="kpi"><div class="kpi-lbl">Pipeline Value</div><div class="kpi-val">${{fmtD(pipeVal)}}</div><div class="kpi-det">${{pipeDeals.length}} open deals</div></div>
    <div class="kpi"><div class="kpi-lbl">Closed Won $</div><div class="kpi-val">${{fmtD(wonVal)}}</div><div class="kpi-det">${{wonDeals.length}} deals won</div></div>
    <div class="kpi"><div class="kpi-lbl">No Shows</div><div class="kpi-val">${{fmt(fm.filter(m=>m.outcome==='NO_SHOW').length)}}</div><div class="kpi-det">${{fm.length?Math.round(fm.filter(m=>m.outcome==='NO_SHOW').length/fm.length*100):0}}% rate</div></div>
  `;
}}

// ── Pipeline ──
function renderPipeline(){{
  const fd=getFilteredDeals();
  const stages=[
    ["1075455291","Discovery Completed","ps1"],
    ["1075460493","Negotiation","ps2"],
    ["171811493","Attack List","ps3"],
    ["1275009440","Contract Sent","ps4"],
    ["1108564665","Closed Won","ps5"],
    ["216501682","Closed Lost","ps6"],
  ];
  const fmtD=n=>'$'+n.toLocaleString('en-US',{{maximumFractionDigits:0}});
  let html='';
  stages.forEach(([id,lbl,cls],i)=>{{
    const ds=fd.filter(d=>d.stage===id);
    const amt=ds.reduce((s,d)=>s+d.amount,0);
    if(i>0) html+='<div class="pipe-arr">→</div>';
    html+=`<div class="pipe-s ${{cls}}"><div class="pipe-n">${{ds.length}}</div><div class="pipe-l">${{lbl}}</div><div class="pipe-d">${{fmtD(amt)}}</div></div>`;
  }});
  document.getElementById('pipeline').innerHTML=html;
}}

// ── Charts ──
function destroyChart(key){{if(charts[key]){{charts[key].destroy();delete charts[key]}}}}

function renderWeeklyChart(){{
  destroyChart('weekly');
  const weekData={{}};
  MEETINGS.forEach(m=>{{if(!m.date)return;const w=isoWeek(m.date);if(!weekData[w])weekData[w]={{disc:0,demo:0}};m.type==='demo'?weekData[w].demo++:weekData[w].disc++}});
  const weeks=allWeeks;
  const labels=weeks.map(w=>'W'+w);
  const disc=weeks.map(w=>(weekData[w]||{{}}).disc||0);
  const demo=weeks.map(w=>(weekData[w]||{{}}).demo||0);
  const selW=currentView==='weekly'&&currentIdx>=0?allWeeks[currentIdx]:null;
  const bgDisc=weeks.map(w=>w===selW?'#3b82f6':'rgba(59,130,246,.5)');
  const bgDemo=weeks.map(w=>w===selW?'#a78bfa':'rgba(167,139,250,.5)');
  charts.weekly=new Chart(document.getElementById('chWeekly'),{{
    type:'bar',data:{{labels,datasets:[
      {{label:'Discovery',data:disc,backgroundColor:bgDisc,borderRadius:4,borderSkipped:false}},
      {{label:'Demo',data:demo,backgroundColor:bgDemo,borderRadius:4,borderSkipped:false}}
    ]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{font:{{size:11}}}}}},tooltip:TT}},scales:{{x:{{stacked:true,grid:{{display:false}}}},y:{{stacked:true,grid:{{color:GC}},beginAtZero:true}}}}}}
  }});
}}

function renderOutcomeChart(){{
  destroyChart('outcome');
  const fm=getFilteredMeetings();
  const counts={{}};fm.forEach(m=>{{const o=m.outcome||'Pending';counts[o]=(counts[o]||0)+1}});
  const order=['COMPLETED','SCHEDULED','NO_SHOW','RESCHEDULED','CANCELED','Pending'];
  const colors=['#34d399','#60a5fa','#f472b6','#fbbf24','#94a3b8','#334155'];
  const labels=['Completed','Scheduled','No Show','Rescheduled','Canceled','Pending'];
  const data=order.map(o=>counts[o]||0);
  charts.outcome=new Chart(document.getElementById('chOutcome'),{{
    type:'doughnut',data:{{labels,datasets:[{{data,backgroundColor:colors,borderWidth:0,hoverOffset:6}}]}},
    options:{{responsive:true,maintainAspectRatio:false,cutout:'60%',plugins:{{legend:{{position:'right',labels:{{padding:12,font:{{size:11}}}}}},tooltip:TT}}}}
  }});
}}

function renderAgentChart(){{
  destroyChart('agent');
  const fm=getFilteredMeetings();
  const agents=SALES_AGENTS.filter(a=>OWNERS[a]);
  const outcomes=['COMPLETED','RESCHEDULED','NO_SHOW','CANCELED','SCHEDULED'];
  const oColors=['#34d399','#fbbf24','#f472b6','#94a3b8','#475569'];
  const oLabels=['Completed','Rescheduled','No Show','Canceled','Scheduled/Pending'];
  const datasets=outcomes.map((o,i)=>({{
    label:oLabels[i],data:agents.map(a=>fm.filter(m=>{{const mo=m.owner;const oo=m.outcome||'SCHEDULED';if(o==='SCHEDULED')return mo===a&&!['COMPLETED','RESCHEDULED','NO_SHOW','CANCELED'].includes(oo);return mo===a&&oo===o}}).length),
    backgroundColor:oColors[i],borderRadius:3,borderSkipped:false
  }}));
  charts.agent=new Chart(document.getElementById('chAgent'),{{
    type:'bar',data:{{labels:agents.map(a=>OWNERS[a]||a),datasets}},
    options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{position:'bottom',labels:{{font:{{size:10}},padding:10}}}},tooltip:TT}},scales:{{x:{{stacked:true,grid:{{color:GC}}}},y:{{stacked:true,grid:{{display:false}}}}}}}}
  }});
}}

function renderStageChart(){{
  destroyChart('stage');
  const counts={{}};COMPANIES.forEach(c=>{{if(c.stage)counts[c.stage]=(counts[c.stage]||0)+1}});
  const order=['Completed','Scheduled','Rescheduled','No Show','Canceled','To Schecule'];
  const colors=['#34d399','#60a5fa','#fbbf24','#f472b6','#94a3b8','#334155'];
  const data=order.map(s=>counts[s]||0);
  charts.stage=new Chart(document.getElementById('chStage'),{{
    type:'doughnut',data:{{labels:order,datasets:[{{data,backgroundColor:colors,borderWidth:0,hoverOffset:6}}]}},
    options:{{responsive:true,maintainAspectRatio:false,cutout:'58%',plugins:{{legend:{{position:'right',labels:{{padding:10,font:{{size:11}}}}}},tooltip:TT}}}}
  }});
}}

function renderPmsChart(){{
  destroyChart('pms');
  const counts={{}};COMPANIES.forEach(c=>{{if(c.pms&&c.pms!=='Manual')counts[c.pms]=(counts[c.pms]||0)+1}});
  const sorted=Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,10);
  const pmsColors=['rgba(59,130,246,.7)','rgba(167,139,250,.7)','rgba(52,211,153,.7)','rgba(251,191,36,.7)','rgba(244,114,182,.7)','rgba(249,115,22,.7)','rgba(100,116,139,.7)','rgba(96,165,250,.5)','rgba(139,92,246,.5)','rgba(236,72,153,.5)'];
  charts.pms=new Chart(document.getElementById('chPms'),{{
    type:'bar',data:{{labels:sorted.map(s=>s[0]),datasets:[{{data:sorted.map(s=>s[1]),backgroundColor:pmsColors,borderRadius:5,borderSkipped:false,barPercentage:.6}}]}},
    options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:TT}},scales:{{y:{{grid:{{color:GC}},beginAtZero:true}},x:{{grid:{{display:false}}}}}}}}
  }});
}}

function renderSizeChart(){{
  destroyChart('size');
  const buckets=[[1,50],[51,100],[101,200],[201,500],[501,99999]];
  const labels=['1–50','51–100','101–200','201–500','500+'];
  const data=buckets.map(([lo,hi])=>COMPANIES.filter(c=>c.listings&&c.listings>=lo&&c.listings<=hi).length);
  charts.size=new Chart(document.getElementById('chSize'),{{
    type:'bar',data:{{labels,datasets:[{{data,backgroundColor:'rgba(96,165,250,.45)',borderColor:'#60a5fa',borderWidth:1,borderRadius:5,borderSkipped:false,barPercentage:.8}}]}},
    options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:TT}},scales:{{y:{{grid:{{color:GC}},beginAtZero:true,title:{{display:true,text:'Companies',color:'#475569',font:{{size:10}}}}}},x:{{grid:{{display:false}},title:{{display:true,text:'Listing Count',color:'#475569',font:{{size:10}}}}}}}}}}
  }});
}}

// ── Heatmap ──
function renderHeatmap(){{
  const agents=SALES_AGENTS.filter(a=>OWNERS[a]);
  const weeks=allWeeks;
  const grid={{}};
  MEETINGS.forEach(m=>{{if(!m.date||!SALES_AGENTS.includes(m.owner))return;const w=isoWeek(m.date);const k=m.owner+'_'+w;grid[k]=(grid[k]||0)+1}});
  const maxVal=Math.max(...Object.values(grid),1);
  const hClass=v=>{{if(!v)return'h0';const r=v/maxVal;if(r<.15)return'h1';if(r<.3)return'h2';if(r<.5)return'h3';if(r<.75)return'h4';return'h5'}};
  const cols=weeks.length+1;
  let html=`<div class="hm" style="grid-template-columns:110px repeat(${{weeks.length}},1fr)">`;
  html+='<div class="hm-h"></div>';
  weeks.forEach(w=>html+=`<div class="hm-h">W${{w}}</div>`);
  agents.forEach(a=>{{
    html+=`<div class="hm-l">${{OWNERS[a]}}</div>`;
    weeks.forEach(w=>{{const v=grid[a+'_'+w]||0;html+=`<div class="hm-c ${{hClass(v)}}">${{v||''}}</div>`}});
  }});
  html+='</div>';
  document.getElementById('heatmap').innerHTML=html;
}}

// ── Tables ──
function renderDeals(){{
  const fd=getFilteredDeals();
  const stageClass=s=>({{
    '1108564665':'b-grn','216501682':'b-red','1075460493':'b-ylw','1275009440':'b-prp','171811493':'b-blu','1075455291':'b-blu'
  }})[s]||'b-gry';
  const fmtD=n=>'$'+n.toLocaleString('en-US',{{maximumFractionDigits:0}});
  let html='<table class="tbl"><thead><tr><th>Deal</th><th>Amount</th><th>Stage</th><th>Owner</th><th>Created</th></tr></thead><tbody>';
  fd.forEach(d=>{{
    html+=`<tr><td>${{d.name}}</td><td>${{fmtD(d.amount)}}</td><td><span class="badge ${{stageClass(d.stage)}}">${{DEAL_STAGES[d.stage]||d.stage}}</span></td><td>${{OWNERS[d.owner]||'–'}}</td><td>${{d.created}}</td></tr>`;
  }});
  html+='</tbody></table>';
  document.getElementById('dealsBox').innerHTML=html;
}}

function renderCompanies(){{
  const sorted=[...COMPANIES].filter(c=>c.listings).sort((a,b)=>(b.listings||0)-(a.listings||0));
  const stageClass=s=>({{Completed:'b-grn',Scheduled:'b-blu',Rescheduled:'b-ylw','No Show':'b-red',Canceled:'b-gry','To Schecule':'b-prp'}})[s]||'b-gry';
  let html='<table class="tbl"><thead><tr><th>Company</th><th>Listings</th><th>PMS</th><th>Stage</th></tr></thead><tbody>';
  sorted.slice(0,50).forEach(c=>{{
    html+=`<tr><td>${{c.name}}</td><td>${{c.listings?c.listings.toLocaleString():'–'}}</td><td>${{c.pms||'–'}}</td><td><span class="badge ${{stageClass(c.stage)}}">${{c.stage||'–'}}</span></td></tr>`;
  }});
  html+='</tbody></table>';
  document.getElementById('compBox').innerHTML=html;
}}

// ── Navigation ──
function setView(v,btn){{
  currentView=v;
  currentIdx=v==='weekly'?allWeeks.length-1:v==='monthly'?new Date().getMonth():-1;
  document.querySelectorAll('.view-btns button').forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  updateLabel();renderAll();
}}

function changeWeek(dir){{
  if(currentView==='all')return;
  const max=currentView==='weekly'?allWeeks.length-1:11;
  currentIdx=Math.max(0,Math.min(max,currentIdx+dir));
  updateLabel();renderAll();
}}

function updateLabel(){{
  const el=document.getElementById('weekLabel');
  if(currentView==='all'){{el.textContent='All Time (Jan 2026 – Now)';return}}
  if(currentView==='weekly'){{const w=allWeeks[currentIdx];el.textContent=`Week ${{w}} · ${{weekRange(2026,w)}}`;return}}
  if(currentView==='monthly'){{const months=['January','February','March','April','May','June','July','August','September','October','November','December'];el.textContent=months[currentIdx]+' 2026';return}}
}}

function renderAll(){{
  renderKPIs();renderPipeline();renderWeeklyChart();renderOutcomeChart();
  renderAgentChart();renderStageChart();renderPmsChart();renderSizeChart();
  renderHeatmap();renderDeals();renderCompanies();
}}

// ── Init ──
updateLabel();
renderAll();
</script>
</body>
</html>'''


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
    html = generate_html(meetings, companies, deals, owners, updated_at)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"  ✓ Dashboard saved to {OUTPUT_PATH}")
    print(f"  ✓ Done! {len(meetings)} meetings · {len(companies)} companies · {len(deals)} deals")


if __name__ == "__main__":
    main()
