"""
oneM2M 6LoWPAN Metrics Collector
----------------------------------
Subscribes to sensor containers in ACME-CSE and collects performance metrics
for BLE/6LoWPAN transport. Stripped-down version focused on:
  - CSE latency: sensor_ts_ms -> CIN ct (main metric)
  - Notify latency: CIN ct -> this collector
  - Payload overhead: raw JSON vs oneM2M CIN size
  - Inter-arrival time and throughput

Saves metrics_raw_6lo.json and prints a report.

Usage:
    pip install fastapi uvicorn requests
    python metrics_6lo.py

Then access: http://localhost:9002/report/text
"""

import asyncio
import json
import logging
import statistics
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

# -- Configuration -------------------------------------------------------------
CSE_BASE            = "http://192.168.1.124:8080"
CSE_ID              = "cse-in"
AE_NAME             = "sensor-ae-6lo"      # must match gateway_6lo.py
ORIGINATOR          = "CMetrics6Lo"
NOTIFICATION_URL    = "http://192.168.1.XXX:9002/notify"   # <-- THIS PC's LAN IP
METRICS_PORT        = 9002
COLLECTION_DURATION = 120       # seconds (shorter than BLE test, adjust as needed)

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metrics_6lo")

# -- Data structures -----------------------------------------------------------
def parse_acme_timestamp(ct: str) -> Optional[float]:
    """Parse ACME ct format '20260606T231933,024400' → ms since epoch."""
    try:
        parts = ct.split(",")
        dt = datetime.strptime(parts[0], "%Y%m%dT%H%M%S")
        ms = dt.timestamp() * 1000
        if len(parts) > 1:
            ms += int(parts[1]) / 1000   # microseconds → ms
        return ms
    except Exception:
        return None

@dataclass
class Reading:
    sensor_ts_ms:    int
    cse_ct_ms:       Optional[float]
    collector_rx_ts: float           # time.time() at reception
    device:          str
    payload_bytes:   int
    cin_bytes:       int

@dataclass
class State:
    readings:             List[Reading] = field(default_factory=list)
    archive:              List[Reading] = field(default_factory=list)
    subscribed:           set           = field(default_factory=set)
    start_time:           float         = 0.0
    end_time:             float         = 0.0
    collection_active:    bool          = False
    has_live_data:        bool          = False

state = State()

# -- oneM2M helpers ------------------------------------------------------------
def cse_headers(ty: int = None) -> dict:
    h = {
        "X-M2M-Origin": ORIGINATOR,
        "X-M2M-RI": f"req-{int(time.time()*1000)}",
        "X-M2M-RVI": "3",
        "Accept": "application/json",
    }
    if ty is not None:
        h["Content-Type"] = f"application/json;ty={ty}"
    return h

def register_ae():
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=10,
        headers=cse_headers(2),
        json={"m2m:ae": {"rn": "metrics-ae-6lo", "api": "N.onem2m.metrics6lo",
                         "rr": True, "srv": ["3"]}})
    log.info(f"Metrics AE: {r.status_code}")

def subscribe_ae_discovery():
    """Subscribe to AE to discover new containers automatically."""
    ae_url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}"
    requests.delete(f"{ae_url}/metrics6lo-discovery-sub",
        headers=cse_headers(), timeout=5)
    r = requests.post(ae_url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": "metrics6lo-discovery-sub",
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3], "chty": [3]}
        }})
    log.info(f"Discovery sub: {r.status_code}")

def subscribe_container(name: str) -> bool:
    if name in state.subscribed:
        return True
    url      = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{name}"
    sub_name = f"metrics6lo-sub-{name}"
    requests.delete(f"{url}/{sub_name}", headers=cse_headers(), timeout=5)
    r = requests.post(url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": sub_name,
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3]}
        }})
    if r.status_code in (200, 201):
        state.subscribed.add(name)
        log.info(f"Subscribed: [{name}]")
        return True
    log.warning(f"Subscribe [{name}] failed: {r.status_code}")
    return False

def list_containers() -> list:
    try:
        r = requests.get(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}",
            params={"rcn": 6, "ty": 3},
            headers=cse_headers(), timeout=10)
        if r.status_code == 200:
            refs = r.json().get("m2m:rrl", {}).get("rrf", [])
            return [ref["nm"] for ref in refs if ref.get("typ") == 3]
    except Exception as e:
        log.warning(f"Container list: {e}")
    return []

# -- Metrics -------------------------------------------------------------------
def safe_stats(data: list) -> dict:
    if not data:
        return {"n": 0, "min": 0, "max": 0, "mean": 0, "median": 0, "stdev": 0}
    return {
        "n":      len(data),
        "min":    round(min(data), 2),
        "max":    round(max(data), 2),
        "mean":   round(statistics.mean(data), 2),
        "median": round(statistics.median(data), 2),
        "stdev":  round(statistics.stdev(data), 2) if len(data) > 1 else 0,
    }

def calculate() -> dict:
    rds = state.archive if state.archive else state.readings
    if not rds:
        return {"error": "No data"}

    duration = (state.end_time or time.time()) - state.start_time

    cse_lats, ntf_lats, total_lats = [], [], []
    for r in rds:
        if r.sensor_ts_ms and r.cse_ct_ms:
            lat = r.cse_ct_ms - r.sensor_ts_ms
            if -5000 < lat < 120_000:
                cse_lats.append(lat)
        if r.cse_ct_ms:
            lat = r.collector_rx_ts * 1000 - r.cse_ct_ms
            if 0 < lat < 120_000:
                ntf_lats.append(lat)
        if r.sensor_ts_ms:
            lat = r.collector_rx_ts * 1000 - r.sensor_ts_ms
            if -5000 < lat < 120_000:
                total_lats.append(lat)

    sorted_r = sorted(rds, key=lambda r: r.collector_rx_ts)
    inter = [(sorted_r[i].collector_rx_ts - sorted_r[i-1].collector_rx_ts) * 1000
             for i in range(1, len(sorted_r))]
    bursts = sum(1 for ia in inter if ia < 1000)

    pay_sizes = [r.payload_bytes for r in rds]
    cin_sizes = [r.cin_bytes for r in rds]
    overhead  = [(c - p) / c * 100 for p, c in zip(pay_sizes, cin_sizes) if c > 0]

    devices = {}
    for r in rds:
        devices[r.device] = devices.get(r.device, 0) + 1

    return {
        "transport": "BLE/6LoWPAN",
        "collection": {
            "duration_s":           round(duration, 1),
            "total_messages":       len(rds),
            "message_rate_per_min": round(len(rds) / duration * 60, 2) if duration > 0 else 0,
            "containers":           list(state.subscribed),
            "devices":              devices,
        },
        "cse_latency_ms":    safe_stats(cse_lats),
        "notify_latency_ms": safe_stats(ntf_lats),
        "total_latency_ms":  safe_stats(total_lats),
        "inter_arrival_ms":  safe_stats(inter),
        "burst_analysis": {
            "burst_messages":    bursts,
            "burst_pct":         round(bursts / len(inter) * 100, 1) if inter else 0,
            "threshold_ms":      1000,
        },
        "payload_bytes":       safe_stats(pay_sizes),
        "cin_bytes":           safe_stats(cin_sizes),
        "overhead_pct":        safe_stats(overhead),
    }

def format_report(m: dict) -> str:
    if "error" in m:
        return f"ERROR: {m['error']}"
    c   = m["collection"]
    cse = m["cse_latency_ms"]
    ntf = m["notify_latency_ms"]
    tot = m["total_latency_ms"]
    ia  = m["inter_arrival_ms"]
    b   = m["burst_analysis"]
    pay = m["payload_bytes"]
    cin = m["cin_bytes"]
    oh  = m["overhead_pct"]
    return f"""
=============================================================
  oneM2M 6LoWPAN (BLE) - PERFORMANCE METRICS REPORT
=============================================================

COLLECTION
  Transport:      BLE/6LoWPAN (UDP over bt0)
  Duration:       {c['duration_s']} s
  Total msgs:     {c['total_messages']}
  Rate:           {c['message_rate_per_min']} msg/min
  Devices:        {c['devices']}
  Containers:     {c['containers']}

LATENCY  (ms)
  [1] Android/Sensor -> CSE  (sensor_ts_ms -> CIN ct)
      n={cse['n']}  min={cse['min']}  mean={cse['mean']}  median={cse['median']}  max={cse['max']}  σ={cse['stdev']}

  [2] CSE -> Collector  (CIN ct -> notification receipt)
      n={ntf['n']}  min={ntf['min']}  mean={ntf['mean']}  median={ntf['median']}  max={ntf['max']}  σ={ntf['stdev']}

  [3] Total End-to-End  (Sensor -> CSE -> Collector)
      n={tot['n']}  min={tot['min']}  mean={tot['mean']}  median={tot['median']}  max={tot['max']}  σ={tot['stdev']}

INTER-ARRIVAL  (ms between consecutive msgs at collector)
  n={ia['n']}  min={ia['min']}  mean={ia['mean']}  median={ia['median']}  max={ia['max']}  σ={ia['stdev']}

BURST ANALYSIS
  Burst msgs (< {b['threshold_ms']} ms apart): {b['burst_messages']} ({b['burst_pct']}%)

PAYLOAD SIZE  (bytes)
  Raw JSON:   n={pay['n']}  min={pay['min']}  mean={pay['mean']}  max={pay['max']}
  CIN total:  n={cin['n']}  min={cin['min']}  mean={cin['mean']}  max={cin['max']}
  Overhead:   mean={oh['mean']}%  max={oh['max']}%

=============================================================
"""

def save_report(m: dict):
    report = format_report(m)
    print(report)
    with open("metrics_report_6lo.txt", "w") as f:
        f.write(report)
        f.write("\n\nRAW JSON:\n")
        f.write(json.dumps(m, indent=2))

    archive = state.archive or state.readings
    raw = {
        "transport": "6lowpan",
        "summary": m,
        "readings": [
            {
                "sensor_ts_ms":    r.sensor_ts_ms,
                "cse_ct_ms":       r.cse_ct_ms,
                "collector_rx_ms": r.collector_rx_ts * 1000,
                "device":          r.device,
                "payload_bytes":   r.payload_bytes,
                "cin_bytes":       r.cin_bytes,
                "cse_latency_ms":  round(r.cse_ct_ms - r.sensor_ts_ms, 2)
                                   if r.cse_ct_ms and r.sensor_ts_ms else None,
                "notify_latency_ms": round(r.collector_rx_ts * 1000 - r.cse_ct_ms, 2)
                                     if r.cse_ct_ms else None,
                "total_latency_ms":  round(r.collector_rx_ts * 1000 - r.sensor_ts_ms, 2)
                                     if r.sensor_ts_ms else None,
                "overhead_pct":      round((r.cin_bytes - r.payload_bytes) / r.cin_bytes * 100, 2)
                                     if r.cin_bytes else None,
            }
            for r in archive
        ]
    }
    with open("metrics_raw_6lo.json", "w") as f:
        json.dump(raw, f, indent=2)
    log.info("Saved metrics_report_6lo.txt and metrics_raw_6lo.json")

# -- Notification processing ---------------------------------------------------
async def process_cin(cin: dict):
    rx_ts = time.time()
    con   = cin.get("con", "")
    lbl   = cin.get("lbl", [])
    ct    = cin.get("ct", "")

    device = "unknown"
    for l in lbl:
        if l.startswith("device:"):
            device = l.split(":", 1)[1]

    cse_ct_ms    = parse_acme_timestamp(ct)
    sensor_ts_ms = 0
    payload_bytes = 0
    try:
        parsed        = json.loads(con) if isinstance(con, str) else con
        sensor_ts_ms  = parsed.get("timestamp_ms", 0)
        payload_bytes = len(con.encode()) if isinstance(con, str) else len(json.dumps(con).encode())
    except Exception:
        pass

    cin_bytes = len(json.dumps(cin).encode())

    if not state.has_live_data:
        state.has_live_data = True
        log.info("Live data received — collection will start soon")

    if not state.collection_active:
        return

    r = Reading(
        sensor_ts_ms    = sensor_ts_ms,
        cse_ct_ms       = cse_ct_ms,
        collector_rx_ts = rx_ts,
        device          = device,
        payload_bytes   = payload_bytes,
        cin_bytes       = cin_bytes,
    )
    state.readings.append(r)
    state.archive.append(r)

    if len(state.readings) % 10 == 0:
        elapsed   = rx_ts - state.start_time
        remaining = max(0, COLLECTION_DURATION - elapsed)
        log.info(f"Collected {len(state.readings)} msgs | {remaining:.0f}s remaining")

# -- Collection timer ----------------------------------------------------------
async def collection_timer():
    await asyncio.sleep(5)
    log.info("Waiting for live data from sensor...")
    while not state.has_live_data:
        await asyncio.sleep(1)
    await asyncio.sleep(2)
    state.readings.clear()
    log.info(f"=== COLLECTION STARTED ({COLLECTION_DURATION}s) ===")
    state.start_time      = time.time()
    state.collection_active = True
    await asyncio.sleep(COLLECTION_DURATION)
    state.end_time          = time.time()
    state.collection_active = False
    log.info("=== COLLECTION COMPLETE ===")
    m = calculate()
    save_report(m)

# -- FastAPI -------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        register_ae()
    except Exception as e:
        log.warning(f"AE register: {e}")
    asyncio.create_task(setup())
    asyncio.create_task(collection_timer())
    yield

async def setup():
    await asyncio.sleep(3)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, subscribe_ae_discovery)
    containers = await loop.run_in_executor(None, list_containers)
    log.info(f"Existing containers: {containers}")
    for c in containers:
        await loop.run_in_executor(None, subscribe_container, c)

app = FastAPI(lifespan=lifespan)

@app.post("/notify")
async def notify(request: Request):
    try:
        body = await request.json()
        sgn  = body.get("m2m:sgn", {})
        if sgn.get("vrq"):
            return JSONResponse({"m2m:rsp": {"rsc": 2000}})
        nev = sgn.get("nev", {})
        rep = nev.get("rep", {})

        # New container discovered
        cnt = rep.get("m2m:cnt")
        if cnt and cnt.get("rn"):
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, subscribe_container, cnt["rn"])

        # New CIN
        cin = rep.get("m2m:cin")
        if cin:
            await process_cin(cin)
    except Exception as e:
        log.warning(f"Notify error: {e}")
    return JSONResponse({"m2m:rsp": {"rsc": 2000}})

@app.get("/report")
async def get_report():
    if state.collection_active:
        elapsed = time.time() - state.start_time
        return {
            "status":      "collecting",
            "messages":    len(state.readings),
            "elapsed_s":   round(elapsed, 1),
            "remaining_s": round(max(0, COLLECTION_DURATION - elapsed), 1),
        }
    m = calculate()
    return m

@app.get("/report/text")
async def get_report_text():
    m = calculate()
    return HTMLResponse(f"<pre style='font-family:monospace;padding:20px'>{format_report(m)}</pre>")

@app.get("/status")
async def status():
    return {
        "subscribed_containers": list(state.subscribed),
        "has_live_data":         state.has_live_data,
        "collection_active":     state.collection_active,
        "readings":              len(state.readings),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("metrics_6lo:app", host="0.0.0.0", port=METRICS_PORT, reload=False)