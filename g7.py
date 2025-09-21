#!/usr/bin/env python3
"""
c8y_bootstrap_try_user_formats.py

Tries bootstrap using username/password with two username formats:
  1) plain BOOTSTRAP_USERNAME
  2) BOOTSTRAP_TENANT/BOOTSTRAP_USERNAME (if BOOTSTRAP_TENANT set)

Uses TLS1.2 and ca-certificates.crt placed in same folder.

Requires: pip install paho-mqtt
"""

import ssl
import time
import queue
import sys
import threading
from paho.mqtt import client as mqtt

# ----------------------- CONFIG (EDIT) -----------------------
BOOTSTRAP_HOST = "deviot.hillrom.com"     # your bootstrap host
BOOTSTRAP_PORT = 8883
BOOTSTRAP_TENANT = "yourTenant"           # set if you know it, else leave "" (empty string)
BOOTSTRAP_USERNAME = "bootstrapUser"      # username provided (without tenant prefix)
BOOTSTRAP_PASSWORD = "bootstrapPassword"  # password provided
BOOTSTRAP_CLIENT_ID = "Centrella-Z277PF3015"  # unique client id for MQTT

DEVICE_SERIAL = "Z277PF3015"
DEVICE_TYPE = "X Service"

BOOTSTRAP_TOPIC = "s/us"   # where to publish registration SmartREST
BOOTSTRAP_REPLY_TIMEOUT = 20  # seconds to wait for bootstrap reply

# CA bundle filename in same folder
CA_CERTS = "ca-certificates.crt"
# ------------------------------------------------------------

SMARTREST_PAYLOAD = f"1000,{DEVICE_SERIAL},{DEVICE_TYPE}"

# internal
msg_q = queue.Queue()
conn_event = threading.Event()
conn_result = {"rc": None, "flags": None}


def on_connect(client, userdata, flags, rc):
    # rc 0=OK, 1..5 = errors (5=not authorized)
    conn_result["rc"] = rc
    conn_result["flags"] = flags
    print(f"[paho] on_connect called: rc={rc}, flags={flags}")
    conn_event.set()


def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="replace")
    print(f"\n---- Message received on topic {msg.topic} ----\n{payload}\n---------------------------------------------")
    msg_q.put((msg.topic, payload))


def create_tls_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # force TLS 1.2 minimum
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    except AttributeError:
        ctx.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    if not CA_CERTS:
        raise ValueError("Set CA_CERTS to your CA bundle filename (ca-certificates.crt)")
    ctx.load_verify_locations(cafile=CA_CERTS)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    return ctx


def attempt_connect(username_to_try):
    """
    Attempt to connect using the given username (and BOOTSTRAP_PASSWORD).
    Returns tuple (connected_bool, client_object_or_None)
    """
    print(f"\n=== Attempting MQTT CONNECT with username: '{username_to_try}' ===")
    client = mqtt.Client(client_id=BOOTSTRAP_CLIENT_ID, clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message

    if username_to_try:
        client.username_pw_set(username_to_try, BOOTSTRAP_PASSWORD)

    try:
        ctx = create_tls_context()
        client.tls_set_context(ctx)
        client.tls_insecure_set(False)
    except Exception as e:
        print("[-] Failed to set TLS context:", e)
        return False, None

    try:
        client.connect(BOOTSTRAP_HOST, BOOTSTRAP_PORT, keepalive=60)
    except Exception as e:
        print("[-] client.connect exception:", e)
        return False, None

    client.loop_start()

    # wait for on_connect (or timeout)
    conn_event.clear()
    waited = conn_event.wait(timeout=8)
    if not waited:
        print("[-] No CONNACK received within 8s; connection may have failed or timed out.")
        client.loop_stop()
        client.disconnect()
        return False, None

    rc = conn_result.get("rc")
    if rc == 0:
        print("[+] CONNECT authorized (rc=0).")
        return True, client
    else:
        print(f"[-] CONNECT not authorized or failed (rc={rc}).")
        client.loop_stop()
        client.disconnect()
        return False, None


def bootstrap_flow(client):
    # subscribe to bootstrap topic to receive reply
    client.subscribe(BOOTSTRAP_TOPIC, qos=1)
    print(f"[i] Subscribed to {BOOTSTRAP_TOPIC}. Publishing SmartREST payload:")
    print("    ", SMARTREST_PAYLOAD)
    client.publish(BOOTSTRAP_TOPIC, SMARTREST_PAYLOAD, qos=1)

    print(f"[i] Waiting up to {BOOTSTRAP_REPLY_TIMEOUT}s for bootstrap reply on {BOOTSTRAP_TOPIC}...")
    try:
        topic, payload = msg_q.get(timeout=BOOTSTRAP_REPLY_TIMEOUT)
        print("\n--- Bootstrap reply (raw) ---")
        print(payload)
        print("--- end ---\n")
    except Exception:
        print("[-] No bootstrap reply received within timeout.")


def main():
    # Try plain username first
    tried = []
    if BOOTSTRAP_USERNAME:
        tried.append(BOOTSTRAP_USERNAME)
    # Try tenant-prefixed if tenant set
    if BOOTSTRAP_TENANT:
        tenant_pref = f"{BOOTSTRAP_TENANT}/{BOOTSTRAP_USERNAME}"
        if tenant_pref not in tried:
            tried.append(tenant_pref)

    if not tried:
        print("[-] No username configured. Edit BOOTSTRAP_USERNAME at top of script.")
        sys.exit(1)

    success = False
    connected_client = None
    for u in tried:
        ok, client = attempt_connect(u)
        if ok:
            success = True
            connected_client = client
            break
        # small gap before trying next format
        time.sleep(0.5)

    if not success:
        print("\n***** All attempts failed with rc != 0 *****")
        print("-> Make sure you have the correct bootstrap username/password, and if the tenant expects 'tenant/username' use the tenant prefix.")
        print("-> Also check with your admin whether the bootstrap account is enabled and the password is correct.")
        sys.exit(2)

    # If connected, do bootstrap flow (subscribe/publish and wait for reply)
    try:
        bootstrap_flow(connected_client)
    finally:
        print("[i] Closing MQTT connection.")
        connected_client.loop_stop()
        connected_client.disconnect()


if __name__ == "__main__":
    main()
