#!/usr/bin/env python3
"""
c8y_bootstrap_tls12.py

Bootstrap a device to Cumulocity using SmartREST over MQTT with TLS 1.2 and a local CA bundle.

Place `ca-certificates.crt` in the same folder as this script.

Requires: pip install paho-mqtt
"""

import ssl
import time
import queue
import sys
from paho.mqtt import client as mqtt

# ----------------------- CONFIG (EDIT THESE) -----------------------
BOOTSTRAP_HOST = "mqtt-bootstrap.example.com"
BOOTSTRAP_PORT = 8883

# If your tenant must be included in username use "tenant/username"
BOOTSTRAP_USERNAME = "bootstrapUser"
BOOTSTRAP_PASSWORD = "bootstrapPassword"
BOOTSTRAP_CLIENT_ID = "bootstrap-client-1"

# Device registration values
DEVICE_SERIAL = "Z277PF3015"
DEVICE_TYPE = "X Service"

# Topics & intervals
BOOTSTRAP_TOPIC = "s/us"
PUBLISH_TOPIC = "s/us"
PUBLISH_INTERVAL = 10

# TLS / x509 settings (we are NOT using x509 client certs)
USE_X509 = False
# Place the CA bundle file in the same folder as this script and name it: ca-certificates.crt
CA_CERTS = "ca-certificates.crt"
CLIENT_CERT = None
CLIENT_KEY = None

BOOTSTRAP_REPLY_TIMEOUT = 20
# -------------------------------------------------------------------

SMARTREST_PAYLOAD = f"1000,{DEVICE_SERIAL},{DEVICE_TYPE}"
msg_q = queue.Queue()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[+] Connected to {BOOTSTRAP_HOST}:{BOOTSTRAP_PORT} (rc={rc})")
        client.subscribe(BOOTSTRAP_TOPIC, qos=1)
        print(f"[+] Subscribed to {BOOTSTRAP_TOPIC}")
    else:
        print(f"[-] Connect failed (rc={rc}). See MQTT return codes (e.g. 1=refused, unacceptable protocol version etc.)")


def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="replace")
    print(f"\n--- MESSAGE RECEIVED on {msg.topic} ---\n{payload}\n---------------------------")
    msg_q.put((msg.topic, payload))


def create_tls_context():
    """
    Create an SSL context that:
      - loads CA_CERTS from local file
      - requires server cert verification
      - forces TLSv1.2 as the minimum and maximum where supported
    """
    if not CA_CERTS:
        raise ValueError("CA_CERTS must be set to the path of your CA bundle (ca-certificates.crt).")

    # create default context for server auth
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Force minimum TLS version to 1.2 (and allow higher if available)
    # On Python versions supporting TLSVersion:
    try:
        context.minimum_version = ssl.TLSVersion.TLSv1_2
    except AttributeError:
        # Fallback for older Python: restrict options
        context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1

    # Load CA bundle from local file (placed in same folder as script)
    context.load_verify_locations(CA_CERTS)

    # Require verification of server cert & hostname
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True

    # Do not allow insecure connections
    # (client.tls_insecure_set(False) will be used by paho; here we already require verification)
    return context


def run_bootstrap():
    client = mqtt.Client(client_id=BOOTSTRAP_CLIENT_ID, clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message

    # Username/password for bootstrap (if provided)
    if BOOTSTRAP_USERNAME:
        client.username_pw_set(BOOTSTRAP_USERNAME, BOOTSTRAP_PASSWORD)

    # TLS context
    ssl_context = create_tls_context()
    client.tls_set_context(ssl_context)
    # ensure paho doesn't skip verification
    client.tls_insecure_set(False)

    print(f"[i] Connecting to {BOOTSTRAP_HOST}:{BOOTSTRAP_PORT} with TLS 1.2+ ...")
    try:
        client.connect(BOOTSTRAP_HOST, BOOTSTRAP_PORT, keepalive=60)
    except Exception as e:
        print(f"[-] Connection failed: {e}")
        return None, None

    client.loop_start()
    time.sleep(0.5)

    print(f"[i] Publishing SmartREST payload to {BOOTSTRAP_TOPIC}: {SMARTREST_PAYLOAD}")
    client.publish(BOOTSTRAP_TOPIC, SMARTREST_PAYLOAD, qos=1)

    print(f"[i] Waiting up to {BOOTSTRAP_REPLY_TIMEOUT}s for bootstrap reply...")
    try:
        topic, payload = msg_q.get(timeout=BOOTSTRAP_REPLY_TIMEOUT)
    except queue.Empty:
        print("[-] No bootstrap reply received within timeout.")
        client.loop_stop()
        client.disconnect()
        return None, None

    client.loop_stop()
    client.disconnect()
    print("[+] Bootstrap connection closed.")
    return topic, payload


def parse_bootstrap_payload(payload):
    lines = [l.strip() for l in payload.splitlines() if l.strip()]
    parsed = {"raw_lines": lines, "candidates": []}
    for ln in lines:
        parts = [p.strip() for p in ln.split(",")]
        parsed["candidates"].append(parts)
    creds = []
    for parts in parsed["candidates"]:
        if len(parts) >= 2:
            for i in range(len(parts)-1):
                user = parts[i]
                pwd = parts[i+1]
                if user and pwd and not user.isnumeric():
                    creds.append({"username": user, "password": pwd, "source": parts})
    parsed["credentials"] = creds
    return parsed


def main():
    topic, payload = run_bootstrap()
    if topic is None:
        print("Bootstrap failed. Check host/port, network, and that 'ca-certificates.crt' exists in script folder.")
        sys.exit(1)

    print("\n=== Bootstrap reply (raw) ===")
    print(payload)
    print("=== end ===\n")

    parsed = parse_bootstrap_payload(payload)
    if parsed["credentials"]:
        print("[+] Possible credentials found:")
        for i, c in enumerate(parsed["credentials"], 1):
            print(f"  Candidate {i}: username='{c['username']}' password='{c['password']}' (source parts: {c['source']})")
    else:
        print("[i] No username/password automatically detected. See raw lines below:")
        for ln in parsed["raw_lines"]:
            print("  ", ln)

    print("\n[i] Use the detected credentials (or the bootstrap response instructions) to connect to the operational broker.")
    print("[i] If you want, I can extend this script to automatically perform the operational connect and publish telemetry after bootstrap.")

if __name__ == "__main__":
    main()
