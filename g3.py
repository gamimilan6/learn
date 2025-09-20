"""
mqtt_connect_manual.py

- Fill HOST, PORT, TENANT, USERNAME, PASSWORD manually below.
- Place ca-certificates.crt or ca-certificates.pem in same folder if you need a local CA bundle.
- Works without client key/cert (username/password auth).
"""

import ssl
import time
import os
from pathlib import Path
import paho.mqtt.client as mqtt

# ------------------------------
# CONFIG - Edit these manually
# ------------------------------
HOST = "mqtt.example.com"      # <- set broker hostname here
PORT = 8883                    # <- typically 8883 for MQTT over TLS
TENANT = "myTenant"            # <- your tenant id
USERNAME = "deviceUser"        # <- username (device user)
PASSWORD = "devicePassword"    # <- password (may be empty if auth by cert only)
CLIENT_ID = "my-device-001"    # <- change if you want unique client id
KEEPALIVE = 20                 # heartbeat seconds
PUBLISH_TOPIC = "s/us"         # example topic; change as needed
PUBLISH_INTERVAL = 10          # seconds between test publishes
# ------------------------------

# Optional: If later you get a client certificate and key, set CLIENT_CERT and CLIENT_KEY.
# For now leave them None since you said you don't have any key file.
CLIENT_CERT = None   # e.g. "client.crt" or "client.pem" (if contains cert+key)
CLIENT_KEY  = None   # e.g. "client.key" (if key is separate)

# Auto-detect CA filenames in the current directory
cwd = Path(".")
preferred_ca_files = ["ca-certificates.crt", "ca-certificates.pem", "ca.crt", "cacert.pem"]
CA_CERT = None
for fname in preferred_ca_files:
    p = cwd / fname
    if p.exists():
        CA_CERT = str(p)
        break

print("Configuration summary:")
print(f"  Host: {HOST}:{PORT}")
print(f"  Tenant/Username: {TENANT}/{USERNAME}")
print(f"  Client ID: {CLIENT_ID}")
print(f"  Using CA cert: {CA_CERT if CA_CERT else 'SYSTEM CA store'}")
print(f"  Client cert/key provided: {bool(CLIENT_CERT and CLIENT_KEY)}")
print()

mqtt_username = f"{TENANT}/{USERNAME}"

# ---- Callbacks ----
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[on_connect] Connected (rc={rc}). Subscribing to example topic.")
        # Subscribe to a sample topic (change as required)
        client.subscribe("#")   # subscribe to all topics for debugging (remove in production)
    else:
        print(f"[on_connect] Failed to connect, rc={rc}")

def on_disconnect(client, userdata, rc):
    if rc == 0:
        print("[on_disconnect] Clean disconnect.")
    else:
        print(f"[on_disconnect] Unexpected disconnect (rc={rc}). Will attempt reconnect automatically.")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
    except Exception:
        payload = msg.payload
    print(f"[on_message] Topic: {msg.topic} | QoS: {msg.qos} | Payload: {payload}")

def on_publish(client, userdata, mid):
    print(f"[on_publish] mid={mid}")

def on_subscribe(client, userdata, mid, granted_qos):
    print(f"[on_subscribe] mid={mid} | granted_qos={granted_qos}")

# ---- Create client and set callbacks ----
client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_publish = on_publish
client.on_subscribe = on_subscribe

# set username/password if provided
if PASSWORD:
    client.username_pw_set(mqtt_username, PASSWORD)
    print("[info] Username/password auth set.")
else:
    print("[info] No password provided; will attempt TLS-only auth (if server accepts).")

# ---- TLS setup ----
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.verify_mode = ssl.CERT_REQUIRED

if CA_CERT:
    try:
        ctx.load_verify_locations(CA_CERT)
        print(f"[info] Loaded CA file: {CA_CERT}")
    except Exception as e:
        print(f"[warning] Failed to load CA file {CA_CERT}: {e}. Falling back to system CA store.")
        ctx.load_default_certs()
else:
    ctx.load_default_certs()
    print("[info] Using system CA store.")

# If you later have client cert+key, uncomment/load here:
if CLIENT_CERT and CLIENT_KEY:
    try:
        ctx.load_cert_chain(certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
        print(f"[info] Loaded client cert {CLIENT_CERT} and key {CLIENT_KEY}")
    except Exception as e:
        print(f"[error] Loading client cert/key failed: {e}")
        raise

client.tls_set_context(ctx)
client.tls_insecure_set(False)   # enforce host name verification

# ---- Connect and loop ----
def main():
    try:
        print(f"[info] Connecting to {HOST}:{PORT} ...")
        client.connect(HOST, port=PORT, keepalive=KEEPALIVE)
    except Exception as e:
        print(f"[error] Connect failed: {e}")
        return

    # use background loop so we can publish periodically
    client.loop_start()

    try:
        while True:
            # publish a small test message
            payload = '{"c8y_Temperature": {"T": 25.0}}'
            result = client.publish(PUBLISH_TOPIC, payload, qos=1)
            # result is (rc, mid)
            print(f"[publish_request] rc={result[0]} mid={result[1]} topic={PUBLISH_TOPIC} payload={payload}")
            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        print("\n[info] KeyboardInterrupt received, shutting down...")
    finally:
        client.loop_stop()
        client.disconnect()
        print("[info] Disconnected and loop stopped.")

if __name__ == "__main__":
    main()

  
