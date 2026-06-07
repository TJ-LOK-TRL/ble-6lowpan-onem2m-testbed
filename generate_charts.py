"""
generate_charts.py
------------------
Generates publication-quality charts from metrics_raw_6lo.json.
Optionally overlays BLE/GATT data from metrics_raw_ble.json if present.

Output files (saved to ./charts/):
  1. latency_distribution.png  - CSE latency histogram (6LoWPAN, optionally vs BLE)
  2. latency_timeseries.png    - CSE latency over time
  3. overhead_comparison.png   - Protocol overhead bar chart (6LoWPAN vs BLE if available)

Usage:
    pip install matplotlib numpy
    python generate_charts.py
"""

import json
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

matplotlib.rcParams.update({
    "font.family":      "serif",
    "font.size":        11,
    "axes.titlesize":   12,
    "axes.labelsize":   11,
    "xtick.labelsize":  10,
    "ytick.labelsize":  10,
    "legend.fontsize":  10,
    "figure.dpi":       150,
    "axes.grid":        True,
    "grid.alpha":       0.3,
    "axes.spines.top":  False,
    "axes.spines.right":False,
})

OUT_DIR = "charts"
os.makedirs(OUT_DIR, exist_ok=True)

ACME_CLOCK_OFFSET_MS = 3600000  # same as metrics_6lo.py

# -- Load data -----------------------------------------------------------------
def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def extract_readings(data, offset_ms=0):
    """Return lists of cse_latency, total_latency, overhead, timestamps."""
    readings = data.get("readings", [])
    cse_lats, total_lats, overheads, seq_times = [], [], [], []
    t0 = None
    for r in readings:
        sensor_ts = r.get("sensor_ts_ms", 0)
        cse_ct    = r.get("cse_ct_ms")
        col_rx    = r.get("collector_rx_ms", 0)
        pay       = r.get("payload_bytes", 0)
        cin       = r.get("cin_bytes", 0)

        if t0 is None and sensor_ts:
            t0 = sensor_ts

        if sensor_ts and cse_ct:
            lat = (cse_ct + offset_ms) - sensor_ts
            if -5000 < lat < 120_000:
                cse_lats.append(lat)
                seq_times.append((sensor_ts - t0) / 1000)  # seconds from start

        if sensor_ts and col_rx:
            lat = col_rx - sensor_ts
            if -5000 < lat < 120_000:
                total_lats.append(lat)

        if pay and cin:
            oh = (cin - pay) / cin * 100
            overheads.append(oh)

    return cse_lats, total_lats, overheads, seq_times

# -- Load 6LoWPAN data ---------------------------------------------------------
data_6lo = load_json("metrics_raw_6lo.json")
cse_6lo, tot_6lo, oh_6lo, ts_6lo = extract_readings(data_6lo, ACME_CLOCK_OFFSET_MS)

# -- Load BLE/GATT data if available -------------------------------------------
has_ble = os.path.exists("metrics_raw_ble.json")
if has_ble:
    data_ble = load_json("metrics_raw_ble.json")
    cse_ble, tot_ble, oh_ble, ts_ble = extract_readings(data_ble, ACME_CLOCK_OFFSET_MS)
else:
    # Try wifi
    has_wifi = os.path.exists("metrics_raw_wifi.json")
    if has_wifi:
        data_ble = load_json("metrics_raw_wifi.json")
        cse_ble, tot_ble, oh_ble, ts_ble = extract_readings(data_ble, 0)
        ble_label = "Wi-Fi/HTTP"
    has_ble = has_wifi

ble_label = "BLE/GATT" if has_ble and not os.path.exists("metrics_raw_wifi.json") else "Wi-Fi/HTTP"

COLOR_6LO = "#2c6fad"
COLOR_BLE = "#c0392b"

# ==============================================================================
# Chart 1 - CSE Latency Histogram
# ==============================================================================
fig, ax = plt.subplots(figsize=(7, 4))

bins = np.linspace(0, max(max(cse_6lo, default=2000),
                          max(cse_ble if has_ble else [0], default=0)) + 200, 30)

ax.hist(cse_6lo, bins=bins, alpha=0.75, color=COLOR_6LO,
        label=f"BLE/6LoWPAN (n={len(cse_6lo)})", edgecolor="white", linewidth=0.5)

if has_ble and cse_ble:
    ax.hist(cse_ble, bins=bins, alpha=0.65, color=COLOR_BLE,
            label=f"{ble_label} (n={len(cse_ble)})", edgecolor="white", linewidth=0.5)

# Median lines
if cse_6lo:
    med_6lo = np.median(cse_6lo)
    ax.axvline(med_6lo, color=COLOR_6LO, linestyle="--", linewidth=1.5,
               label=f"6LoWPAN median: {med_6lo:.0f} ms")
if has_ble and cse_ble:
    med_ble = np.median(cse_ble)
    ax.axvline(med_ble, color=COLOR_BLE, linestyle="--", linewidth=1.5,
               label=f"{ble_label} median: {med_ble:.0f} ms")

ax.set_xlabel("Sensor-to-CSE Latency (ms)")
ax.set_ylabel("Message Count")
ax.set_title("Sensor-to-CSE Latency Distribution")
ax.legend()
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "latency_distribution.png"), bbox_inches="tight")
plt.close(fig)
print("Saved latency_distribution.png")

# ==============================================================================
# Chart 2 - CSE Latency Time Series
# ==============================================================================
fig, ax = plt.subplots(figsize=(8, 4))

if cse_6lo and ts_6lo:
    n = min(len(ts_6lo), len(cse_6lo))
    ax.plot(ts_6lo[:n], cse_6lo[:n], color=COLOR_6LO, linewidth=1.2,
            alpha=0.8, label="BLE/6LoWPAN")
    ax.axhline(np.mean(cse_6lo[:n]), color=COLOR_6LO, linestyle="--",
               linewidth=1, alpha=0.6, label=f"6LoWPAN mean: {np.mean(cse_6lo[:n]):.0f} ms")

if has_ble and cse_ble and ts_ble:
    n = min(len(ts_ble), len(cse_ble))
    ax.plot(ts_ble[:n], cse_ble[:n], color=COLOR_BLE, linewidth=1.2,
            alpha=0.8, label=ble_label)
    ax.axhline(np.mean(cse_ble[:n]), color=COLOR_BLE, linestyle="--",
               linewidth=1, alpha=0.6, label=f"{ble_label} mean: {np.mean(cse_ble[:n]):.0f} ms")

ax.set_xlabel("Time from Collection Start (s)")
ax.set_ylabel("Sensor-to-CSE Latency (ms)")
ax.set_title("Sensor-to-CSE Latency over Collection Window")
ax.legend()
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "latency_timeseries.png"), bbox_inches="tight")
plt.close(fig)
print("Saved latency_timeseries.png")

# ==============================================================================
# Chart 3 - Protocol Overhead Comparison
# ==============================================================================
fig, ax = plt.subplots(figsize=(5, 4))

labels  = ["BLE/6LoWPAN"]
means   = [np.mean(oh_6lo) if oh_6lo else 0]
stdevs  = [np.std(oh_6lo) if oh_6lo else 0]
colors  = [COLOR_6LO]

if has_ble and oh_ble:
    labels.append(ble_label)
    means.append(np.mean(oh_ble))
    stdevs.append(np.std(oh_ble))
    colors.append(COLOR_BLE)

x = np.arange(len(labels))
bars = ax.bar(x, means, yerr=stdevs, capsize=5, color=colors,
              alpha=0.8, edgecolor="white", linewidth=0.8, width=0.5)

for bar, mean in zip(bars, means):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
            f"{mean:.1f}%", ha="center", va="bottom", fontsize=10)

ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_ylabel("oneM2M Protocol Overhead (%)")
ax.set_title("oneM2M Protocol Overhead by Transport")
ax.set_ylim(0, max(means) * 1.25 if means else 100)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "overhead_comparison.png"), bbox_inches="tight")
plt.close(fig)
print("Saved overhead_comparison.png")

print(f"\nAll charts saved to ./{OUT_DIR}/")
print(f"6LoWPAN: n={len(cse_6lo)} latency samples, mean={np.mean(cse_6lo):.1f}ms, median={np.median(cse_6lo):.1f}ms" if cse_6lo else "6LoWPAN: no latency data")