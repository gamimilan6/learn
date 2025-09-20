"""
mqtt_full_bootstrap.py

Flow:
  1) Connect with BOOTSTRAP credentials
  2) Publish registration line: 1000,<serial>,<device_type>  (to topic s/us)
  3) Wait for server response containing new credentials (messageId 1001)
     - Supports JSON AgentBootstrap ({"messageId":1001, "Username": "...", ...})
     - Supports SmartREST comma-line (starts with "1001,...")
  4) Parse and print the new credentials, disconnect, then reconnect using them
  5) Continue normal operation (subscribe to debug topics & publish telemetry)

Edit the CONFIG section below before running.
"""

import ssl
import time
import json
import threading
from pathlib import Path
import paho.mqtt.client as mqtt

# ----------------------- CONFIG (EDIT THESE) -----------------------
# Bootstrap (initial) connection credentials - provided by your provisioning system
BOOTSTRAP_HOST = "mqtt-bootstrap.example.com"
BOOTSTRAP_PORT = 8883
BOOTSTRAP_TENANT = "bootstrapTenant"        # the tenant part for bootstrap user (if applicable)
BOOTSTRAP_USERNAME = "bootstrapUser"        # bootstrap username
BOOTSTRAP_PASSWORD = "bootstrapPassword"    # bootstrap password (may be empty)
BOOTSTRAP_CLIENT_ID = "bootstrap-client-1"

# Device registration values you will publish (replace with your device serial/type)
DEVICE_SERIAL = "Z277PF3010"
DEVICE_TYPE = "FLC Service"

# Topics
BOOTSTRAP_TOPIC = "s/us"      # where to send registration payload and receive bootstrap reply
OPERATION_SUB_TOPIC = "#"     # debug: subscribe to all; change to production topics as needed
PUBLISH_TOPIC = "s/us"        # where to publish telemetry after full connect
PUBLISH_INTERVAL = 10         # seconds between telemetry publishes

# TLS / CA settings (script folder)
# Place ca-certificates.crt or ca-certificates.pem in same dir if you need a specific CA bundle
# If not present, system CA store will be used.
# ------------------------------------------------------------------

# -------------------- Internal globals / events --------------------
bootstrap_response_event = threading.Event()
bootstrap_result = {}   # will store parsed new credentials when received
cwd = Path(".")

# detect CA file if present
preferred_ca_files = ["ca-certificates.crt", "ca-certificates.pem", "ca.crt", "cacert.pem"]
CA_CERT = None
for fname in preferred_ca_files:
    if (cwd / fname).exists():
        CA_CERT = str(cwd / fname)
        break

print("=== MQTT Full Bootstrap Script ===")
print(f"Bootstrap broker: {BOOTSTRAP_HOST}:{BOOTSTRAP_PORT}")
print(f"Device serial/type: {DEVICE_SERIAL} / {DEVICE_TYPE}")
print(f"CA cert: {CA_CERT if CA_CERT else 'SYSTEM CA store'}")
print("Client cert/key: NOT used (username/password only)")
print("=================================\n")

# -------------------- Helper: parse bootstrap reply --------------------
def try_parse_bootstrap_payload(payload_str):
    """
    Attempt to parse a bootstrap response and return dict with keys:
      - username (string)
      - password (string)
      - tenant (string) optional
      - host (string) optional
    Returns None if not a bootstrap response.
    Handles:
      - JSON AgentBootstrap: {"messageId":1001, "Username":"...", "Password":"...", "TenantId":"...", "Hostname":"..."}
      - SmartREST line: "1001,<username>,<password>,<tenant>,<host>" or other ordering
      - SmartREST variant: "1001,<tenant>,<username>,<password>,<host>" (we will attempt several sensible mappings)
    """
    # Quick guard
    if not payload_str:
        return None

    # 1) Try JSON parse
    try:
        j = json.loads(payload_str)
        # look for messageId or keys
        if isinstance(j, dict):
            mid = j.get("messageId") or j.get("MessageId") or j.get("MessageID")
            if mid == 1001 or str(mid) == "1001" or any(k.lower() == "username" for k in j.keys()):
                # gather known keys (case-insensitive)
                def get_k(*keys):
                    for k in keys:
                        if k in j:
                            return j[k]
                        # try case-insensitive
                        for kk in j:
                            if kk.lower() == k.lower():
                                return j[kk]
                    return None
                username = get_k("Username", "username", "User")
                password = get_k("Password", "password", "Pass")
                tenant = get_k("TenantId", "tenantid", "Tenant")
                hostname = get_k("Hostname", "hostname", "Host")
                return {"username": username, "password": password, "tenant": tenant, "host": hostname}
    except Exception:
        pass

    # 2) Try SmartREST comma-line parsing
    # Many bootstrap replies are plain CSV-like: "1001,username,password,tenant,host" or similar.
    # We'll split and look for a leading '1001'
    parts = [p.strip() for p in payload_str.split(",")]
    if len(parts) >= 1:
        # sometimes payload may be prefixed by quotes or other artifacts - normalize
        if parts[0].startswith("1001") or parts[0] == "1001":
            # remove the leading token if it's exactly "1001" or prefixed
            if parts[0] == "1001":
                tokens = parts[1:]
            else:
                # e.g. "1001<something>" unlikely, but handle if first token begins with 1001
                # in that case, try to treat tokens[0] as next
                tokens = parts[1:]
            # Heuristics to map tokens to fields:
            # common format: [username, password, tenant, host]
            res = {}
            if len(tokens) >= 1:
                res["username"] = tokens[0] or None
            if len(tokens) >= 2:
                res["password"] = tokens[1] or None
            if len(tokens) >= 3:
                res["tenant"] = tokens[2] or None
            if len(tokens) >= 4:
                res["host"] = tokens[3] or None
            return res
    # Not recognized as bootstrap reply
    return None

# -------------------- MQTT callbacks for bootstrap phase --------------------
def make_on_message_for_bootstrap(client, verbose=True):
    def on_message(c, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8", errors="ignore")
        except Exception:
            payload = None
        if verbose:
            print(f"[bootstrap on_message] topic={msg.topic} payload={payload}")
        # Attempt parse
        parsed = try_parse_bootstrap_payload(payload)
        if parsed:
            print("[bootstrap] Detected bootstrap response. Parsed fields:", parsed)
            # store into global bootstrap_result and signal event
            bootstrap_result.update(parsed)
            bootstrap_response_event.set()
    return on_message

# -------------------- Establish TLS context helper --------------------
def create_tls_context(ca_cert_path=None):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_mode = ssl.CERT_REQUIRED
    if ca_cert_path:
        try:
            ctx.load_verify_locations(cafile=ca_cert_path)
            print(f"[tls] Loaded CA file: {ca_cert_path}")
        except Exception as e:
            print(f"[tls] Failed loading CA file {ca_cert_path}: {e}. Falling back to system CA store.")
            ctx.load_default_certs()
    else:
        ctx.load_default_certs()
        print("[tls] Using system CA store.")
    return ctx

# -------------------- Bootstrap connect -> register -> receive creds --------------------
def run_bootstrap_and_get_credentials(timeout_seconds=30):
    """
    Connect using bootstrap credentials, publish registration payload, wait for bootstrap response.
    Returns dict with parsed credentials if found, else None.
    """
    client = mqtt.Client(client_id=BOOTSTRAP_CLIENT_ID, clean_session=True)
    # Attach callbacks
    client.on_message = make_on_message_for_bootstrap(client)
    def on_connect(c, u, flags, rc):
        if rc == 0:
            print(f"[bootstrap on_connect] connected (rc={rc}). Subscribing to {BOOTSTRAP_TOPIC} and sending registration.")
            c.subscribe(BOOTSTRAP_TOPIC)
            # Compose registration payload
            reg_line = f"1000,{DEVICE_SERIAL},{DEVICE_TYPE}"
            # Send registration to bootstrap topic
            rc_pub = c.publish(BOOTSTRAP_TOPIC, reg_line, qos=1)
            print(f"[bootstrap] Published registration: {reg_line} (publish rc,mid={rc_pub})")
        else:
            print(f"[bootstrap on_connect] connect failed rc={rc}")
    client.on_connect = on_connect
    client.on_disconnect = lambda c, u, rc: print(f"[bootstrap on_disconnect] rc={rc}")

    # set username/password for bootstrap
    if BOOTSTRAP_PASSWORD is not None:
        client.username_pw_set(f"{BOOTSTRAP_TENANT}/{BOOTSTRAP_USERNAME}", BOOTSTRAP_PASSWORD)

    # TLS
    tls_ctx = create_tls_context(CA_CERT)
    client.tls_set_context(tls_ctx)
    client.tls_insecure_set(False)

    try:
        client.connect(BOOTSTRAP_HOST, port=BOOTSTRAP_PORT, keepalive=20)
    except Exception as e:
        print(f"[bootstrap] Connection failed: {e}")
        return None

    client.loop_start()
    got = bootstrap_response_event.wait(timeout=timeout_seconds)
    client.loop_stop()
    client.disconnect()

    if not got:
        print(f"[bootstrap] Timeout waiting for bootstrap response (waited {timeout_seconds}s).")
        return None

    # bootstrap_result may contain username/password/tenant/host
    creds = dict(bootstrap_result)  # copy
    # Normalize field names
    username = creds.get("username") or creds.get("Username")
    password = creds.get("password") or creds.get("Password")
    tenant = creds.get("tenant") or creds.get("TenantId") or BOOTSTRAP_TENANT
    host = creds.get("host") or creds.get("hostname") or BOOTSTRAP_HOST

    if not username and not password and not tenant:
        print("[bootstrap] Parsed response did not contain usable credentials.")
        return None

    parsed = {"username": username, "password": password, "tenant": tenant, "host": host}
    print("[bootstrap] Final parsed credentials:", parsed)
    return parsed

# -------------------- After bootstrap: connect with new credentials and run normal loop --------------------
def run_normal_operation(new_cred):
    """
    new_cred: dict with keys: username, password, tenant, host (host optional)
    """
    host = new_cred.get("host") or BOOTSTRAP_HOST
    tenant = new_cred.get("tenant") or BOOTSTRAP_TENANT
    username = new_cred.get("username")
    password = new_cred.get("password") or ""

    # Build client id (you can change policy)
    client_id = f"{DEVICE_SERIAL}-client"

    client = mqtt.Client(client_id=client_id, clean_session=True)

    # callbacks
    def on_connect(c, u, flags, rc):
        if rc == 0:
            print(f"[op on_connect] Connected as {tenant}/{username} (rc={rc}). Subscribing to {OPERATION_SUB_TOPIC}")
            c.subscribe(OPERATION_SUB_TOPIC)
        else:
            print(f"[op on_connect] Connection failed rc={rc}")

    def on_message(c, u, msg):
        try:
            payload = msg.payload.decode("utf-8")
        except Exception:
            payload = msg.payload
        print(f"[op on_message] {msg.topic} | qos={msg.qos} | payload={payload}")

    def on_disconnect(c, u, rc):
        print(f"[op on_disconnect] rc={rc}")

    def on_publish(c, u, mid):
        print(f"[op on_publish] mid={mid}")

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    # set username/password in tenant/username format for Cumulocity
    if username:
        client.username_pw_set(f"{tenant}/{username}", password)
        print(f"[op] Will connect with username: {tenant}/{username}")
    else:
        print("[op] No username parsed from bootstrap. Will attempt anonymous connection (may be rejected).")

    # TLS context
    tls_ctx = create_tls_context(CA_CERT)
    client.tls_set_context(tls_ctx)
    client.tls_insecure_set(False)

    try:
        print(f"[op] Connecting to broker {host}:{BOOTSTRAP_PORT} ...")
        client.connect(host, port=BOOTSTRAP_PORT, keepalive=20)
    except Exception as e:
        print(f"[op] Connect failed: {e}")
        return

    client.loop_start()

    try:
        while True:
            payload = json.dumps({"c8y_Temperature": {"T": 25.0, "serial": DEVICE_SERIAL}})
            rc, mid = client.publish(PUBLISH_TOPIC, payload, qos=1)
            print(f"[op publish_request] rc={rc} mid={mid} topic={PUBLISH_TOPIC} payload={payload}")
            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        print("\n[op] Interrupted by user. Shutting down.")
    finally:
        client.loop_stop()
        client.disconnect()
        print("[op] Disconnected cleanly.")

# -------------------- Main flow --------------------
def main():
    print("[main] Starting bootstrap sequence...")
    creds = run_bootstrap_and_get_credentials(timeout_seconds=30)
    if not creds:
        print("[main] Bootstrap failed or timed out. Exiting.")
        return
    print("[main] Bootstrap succeeded. Now connecting with provided credentials...\n")
    run_normal_operation(creds)

if __name__ == "__main__":
    main()
