"""
oneM2M 6LoWPAN Sensor Simulator
---------------------------------
Simulates an IoT sensor that sends JSON payloads over BLE 6LoWPAN
to the gateway using an IPv6 UDP socket over the bt0 interface.

Run on the PC with the BLE adapter (PC1 / WSL2 with usbipd Bluetooth).
The gateway's link-local address must be set in GATEWAY_ADDR below.

Usage:
    python sensor_6lo.py

Requirements:
    pip install psutil
"""

import json
import math
import random
import socket
import struct
import time
import logging

# -- Configuration -------------------------------------------------------------
# Gateway link-local IPv6 address on bt0 (run `ip addr show bt0` on gateway PC)
GATEWAY_ADDR    = "fe80::f0a6:54ff:febc:5400"   # <-- FILL IN
GATEWAY_PORT    = 5683          # CoAP-style port (UDP, no CoAP lib needed)
BT_INTERFACE    = "bt0"         # 6LoWPAN network interface on this machine
SEND_INTERVAL   = 5.0           # seconds between readings
DEVICE_NAME     = "OneM2M-Sensor-6Lo"
SEQUENCE        = 0

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sensor")

# -- Simulated sensor data -----------------------------------------------------
def read_sensor() -> dict:
    global SEQUENCE
    SEQUENCE += 1
    t = time.time()
    return {
        "device":       DEVICE_NAME,
        "timestamp_ms": int(t * 1000),
        "sequence":     SEQUENCE,
        "temperature":  round(20.0 + 5.0 * math.sin(t / 60) + random.uniform(-0.5, 0.5), 2),
        "humidity":     round(50.0 + 10.0 * math.cos(t / 90) + random.uniform(-1.0, 1.0), 2),
        "pressure":     round(1013.25 + random.uniform(-2.0, 2.0), 2),
        "battery_pct":  max(0, min(100, 85 - SEQUENCE * 0.01 + random.uniform(-0.5, 0.5))),
        "transport":    "BLE/6LoWPAN"
    }

# -- Network -------------------------------------------------------------------
def get_interface_index(ifname: str) -> int:
    """Return the network interface index for the given interface name."""
    return socket.if_nametoindex(ifname)

def send_payload(sock: socket.socket, payload: bytes, iface_idx: int):
    """Send UDP payload to gateway over 6LoWPAN (IPv6 link-local)."""
    # For link-local addresses, scope_id (sin6_scope_id) must be set
    addr = (GATEWAY_ADDR, GATEWAY_PORT, 0, iface_idx)
    sent = sock.sendto(payload, addr)
    return sent

def main():
    if "xxxx" in GATEWAY_ADDR:
        log.error("GATEWAY_ADDR not configured! Edit sensor_6lo.py and set the gateway's bt0 link-local IPv6.")
        return

    iface_idx = get_interface_index(BT_INTERFACE)
    log.info(f"Interface {BT_INTERFACE} index={iface_idx}")

    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, 1)

    # Bind to local bt0 interface
    try:
        sock.bind(("", 0, 0, iface_idx))
    except OSError as e:
        log.warning(f"Bind warning (may be fine): {e}")

    log.info(f"Sending to [{GATEWAY_ADDR}%{BT_INTERFACE}]:{GATEWAY_PORT} every {SEND_INTERVAL}s")

    try:
        while True:
            data = read_sensor()
            payload = json.dumps(data, separators=(',', ':')).encode()
            try:
                sent = send_payload(sock, payload, iface_idx)
                log.info(f"Sent seq={data['sequence']} temp={data['temperature']}°C "
                         f"hum={data['humidity']}% ({sent}B)")
            except OSError as e:
                log.warning(f"Send failed: {e}")
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        log.info("Stopped.")
    finally:
        sock.close()

if __name__ == "__main__":
    main()