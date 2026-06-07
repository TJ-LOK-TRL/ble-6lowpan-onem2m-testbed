"""
oneM2M 6LoWPAN Gateway
-----------------------
Interworking Proxy AE for BLE/6LoWPAN sensors.
Listens on bt0 (IPv6 UDP) for sensor JSON payloads,
then POSTs them as oneM2M ContentInstances to ACME-CSE.

Architecture:
    [Sensor PC1] --BLE/6LoWPAN/UDP--> | gateway_6lo.py (AE) | --Mca/HTTP--> [ACME CSE @ 192.168.1.124]

Run on the PC that receives 6LoWPAN (PC2 / gateway).
Requires bt0 interface up and 6LoWPAN module loaded:
    sudo modprobe bluetooth_6lowpan
    echo 1 | sudo tee /sys/kernel/debug/bluetooth/6lowpan_enable
    sudo ip link set bt0 up

Usage:
    python gateway_6lo.py
"""

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import requests

# -- Configuration -------------------------------------------------------------
CSE_BASE         = "http://192.168.1.124:8080"   # Remote ACME-CSE
CSE_ID           = "cse-in"
AE_NAME          = "sensor-ae-6lo"
ORIGINATOR       = "C6LoSensor"
ACP_NAME         = "acp-sensor-6lo"

LISTEN_PORT      = 5683          # UDP port to receive sensor data
LISTEN_IFACE     = "bt0"         # 6LoWPAN interface
LISTEN_ADDR      = "::"          # all IPv6

DEVICE_NAME_DEFAULT = "6lo-sensor"

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gateway_6lo")

# -- State ---------------------------------------------------------------------
known_containers: set = set()

# -- oneM2M helpers (identical pattern to your existing gateway) ---------------
def cse_headers(ty: int = None, ri: str = None) -> dict:
    h = {
        "X-M2M-Origin": ORIGINATOR,
        "X-M2M-RI": ri or f"req-{int(time.time()*1000)}",
        "X-M2M-RVI": "3",
        "Accept": "application/json",
    }
    if ty is not None:
        h["Content-Type"] = f"application/json;ty={ty}"
    return h

def device_to_container_name(device_name: str) -> str:
    return "sensor-" + re.sub(r'[^a-zA-Z0-9]', '', device_name).lower()

def ensure_ae_and_acp():
    """One-time startup: register ACP + AE on the remote CSE."""
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=10,
        headers=cse_headers(1),
        json={"m2m:acp": {
            "rn": ACP_NAME,
            "pv": {"acr": [{"acor": [ORIGINATOR, "CDashboard", "CMetrics", "CAdmin", "CMetrics6Lo"],
                            "acop": 63}]},
            "pvs": {"acr": [{"acor": ["CAdmin"], "acop": 63}]}
        }})
    log.info(f"ACP: {r.status_code} {r.text[:80]}")

    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=10,
        headers=cse_headers(2),
        json={"m2m:ae": {
            "rn": AE_NAME,
            "api": "N.onem2m.sensor6lo",
            "rr": True,
            "srv": ["3"],
            "acpi": [f"/id-in/{CSE_ID}/{ACP_NAME}"]
        }})
    log.info(f"AE: {r.status_code} {r.text[:80]}")

def ensure_container(container_name: str, device_name: str, addr: str) -> bool:
    """Create container if it doesn't exist. Returns True if ready."""
    if container_name in known_containers:
        return True
    r = requests.post(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}", timeout=10,
        headers=cse_headers(3),
        json={"m2m:cnt": {
            "rn": container_name,
            "mni": 500,
            "acpi": [f"/id-in/{CSE_ID}/{ACP_NAME}"],
            "lbl": [f"device:{device_name}", f"address:{addr}", "type:6lowpan-sensor"]
        }})
    ok = r.status_code in (200, 201, 409)
    if ok:
        known_containers.add(container_name)
    log.info(f"Container [{container_name}]: {r.status_code}")
    return ok

async def post_cin(session: aiohttp.ClientSession,
                   container_name: str,
                   sensor_json: str,
                   device_name: str,
                   peer_addr: str) -> int:
    """Async POST of a ContentInstance to the remote CSE."""
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{container_name}"
    body = {"m2m:cin": {
        "con": sensor_json,
        "cnf": "application/json:0",
        "lbl": [
            "transport:BLE/6LoWPAN",
            f"device:{device_name}",
            f"peer:{peer_addr}",
            "source:gateway-6lo"
        ]
    }}
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with session.post(url, headers=cse_headers(4), json=body, timeout=timeout) as r:
            status = r.status
            log.info(f"[{device_name}] CIN POST → {status}")
            return status
    except Exception as e:
        log.warning(f"[{device_name}] CIN POST failed: {e}")
        return 0

# -- UDP 6LoWPAN receiver ------------------------------------------------------
class SensorProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

    async def get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
        return self.session

    def connection_made(self, transport):
        self.transport = transport
        log.info(f"Listening on UDP [{LISTEN_ADDR}]:{LISTEN_PORT} (bt0)")

    def datagram_received(self, data: bytes, addr):
        peer_ip   = addr[0]
        peer_port = addr[1]
        log.info(f"UDP from [{peer_ip}]:{peer_port} ({len(data)}B)")
        asyncio.ensure_future(self.handle_packet(data, peer_ip))

    async def handle_packet(self, data: bytes, peer_ip: str):
        try:
            payload_str = data.decode("utf-8")
            parsed      = json.loads(payload_str)
        except Exception as e:
            log.warning(f"Invalid payload: {e} | raw={data[:80]}")
            return

        device_name    = parsed.get("device", DEVICE_NAME_DEFAULT)
        container_name = device_to_container_name(device_name)

        # Ensure container exists (sync call in executor to avoid blocking loop)
        loop = asyncio.get_event_loop()
        ok   = await loop.run_in_executor(None, ensure_container,
                                          container_name, device_name, peer_ip)
        if not ok:
            log.warning(f"Container setup failed for {device_name}, dropping packet")
            return

        session = await self.get_session()
        await post_cin(session, container_name, payload_str, device_name, peer_ip)

    def error_received(self, exc):
        log.warning(f"UDP error: {exc}")

    def connection_lost(self, exc):
        log.info("UDP socket closed")

# -- Main ----------------------------------------------------------------------
async def main():
    # One-time CSE setup (sync, at startup)
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, ensure_ae_and_acp)
    except Exception as e:
        log.warning(f"CSE setup error: {e} — continuing anyway")

    # Bind UDP socket on bt0
    try:
        iface_idx = socket_iface_index(LISTEN_IFACE)
        log.info(f"bt0 interface index: {iface_idx}")
    except Exception as e:
        log.warning(f"Could not resolve {LISTEN_IFACE} index: {e}. Binding to all interfaces.")
        iface_idx = 0

    import socket as _socket
    sock = _socket.socket(_socket.AF_INET6, _socket.SOCK_DGRAM)
    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    # Bind to bt0 scope
    sock.bind((LISTEN_ADDR, LISTEN_PORT, 0, iface_idx))

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: SensorProtocol(loop),
        sock=sock
    )
    log.info(f"Gateway ready — forwarding to {CSE_BASE}/{CSE_ID}/{AE_NAME}")

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutting down...")
    finally:
        transport.close()
        if protocol.session and not protocol.session.closed:
            await protocol.session.close()

def socket_iface_index(ifname: str) -> int:
    import socket as _socket
    return _socket.if_nametoindex(ifname)

if __name__ == "__main__":
    asyncio.run(main())
