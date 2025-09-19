"""
Cumulocity TLS1.2 bootstrap -> get tenant broker -> connect -> publish
Python (paho-mqtt) single-file agent.

How it works (implemented behavior):
1. Connects to the *bootstrap* broker over TLS1.2 using username/password.
2. Subscribes to the bootstrap response topic (s/d/l/AgentBootstrap).
3. Publishes a bootstrap request to the bootstrap publish topic (s/u/l/AgentBootstrap)
   containing the payload you specified (xxxx, yyyy, zzzz).
4. Waits (with timeout) for a JSON response on the response topic that contains
   the real broker details (expects keys like broker_host, broker_port, username, password).
5. Connects to the real broker over TLS1.2 (optionally with client cert for mTLS),
   publishes the same payload (or whatever you want), then exits.

Fill the CONFIG section below with your values.
Requires: pip install paho-mqtt
"""

import ssl
import json
import socket
import time
import threading
from paho.mqtt import client as mqtt

# ---------------- CONFIG ----------------
# Bootstrap broker (replace)
BOOTSTRAP_HOST = "bootstrap.xxxxxxx.com"
BOOTSTRAP_PORT = 8883               # TLS port

BOOTSTRAP_USER = "BOOTSTRAP_USER"
BOOTSTRAP_PASS = "BOOTSTRAP_PASS"

# CA / client certs
# - If you have a CA certificate file to verify the broker, set CA_CERT_PATH.
# - If the broker expects mutual TLS (client cert), set CLIENT_CERT_PATH and CLIENT_KEY_PATH.
CA_CERT_PATH = "/path/to/ca-cert.pem"          # set to None if you intentionally skip server verification (NOT RECOMMENDED)
CLIENT_CERT_PATH = None                        # set to path if mTLS required, else None
CLIENT_KEY_PATH = None                         # set to path if mTLS required, else None

# MQTT topics from your screenshot:
BOOTSTRAP_PUBLISH_TOPIC   = "s/u/l/AgentBootstrap"   # we publish bootstrap request here
BOOTSTRAP_SUBSCRIBE_TOPIC = "s/d/l/AgentBootstrap"   # we listen for bootstrap response here

# Payload you said you have
BOOTSTRAP_PAYLOAD = {
    "xxxx": "value_x",
    "yyyy": "value_y",
    "zzzz": "value_z"
}

# Topic to use when publishing to the real broker (change per your tenant requirements)
REAL_PUBLISH_TOPIC = "s/u/l/AgentData"   # example; change to whatever cumulocity expects for device data

# Timeout (seconds) waiting for bootstrap response
BOOTSTRAP_RESPONSE_TIMEOUT = 15

# Optional: client id prefix
CLIENT_ID_PREFIX = "agent-"

# ----------------------------------------

# runtime container for bootstrap response
bootstrap_response = {}
bootstrap_response_event = threading.Event()


def make_tls_context():
    """
    Create an SSLContext that enforces TLS1.2.
    If CA_CERT_PATH is provided, server certificate will be verified.
    If CLIENT_CERT_PATH/CLIENT_KEY_PATH are provided, client cert (mTLS) will be loaded.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # require certificate verification unless CA_CERT_PATH is None
    if CA_CERT_PATH:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.check_hostname = True
        ctx.load_verify_locations(CA_CERT_PATH)
    else:
        # Warning: disabling verification is insecure. Use only for debug/test.
        ctx.verify_mode = ssl.CERT_NONE
        ctx.check_hostname = False

    if CLIENT_CERT_PATH and CLIENT_KEY_PATH:
        ctx.load_cert_chain(certfile=CLIENT_CERT_PATH, keyfile=CLIENT_KEY_PATH)

    return ctx


# ---------- Bootstrap callbacks ----------
def on_bootstrap_connect(client, userdata, flags, rc):
    print("[bootstrap] connected rc=", rc)
    # subscribe to the expected response topic
    client.subscribe(BOOTSTRAP_SUBSCRIBE_TOPIC, qos=1)
    # send bootstrap request (adjust structure if your server expects another shape)
    request = {
        "request": "getBroker",
        "client_id": socket.gethostname(),
        "payload": BOOTSTRAP_PAYLOAD
    }
    payload_str = json.dumps(request)
    print("[bootstrap] publishing request to", BOOTSTRAP_PUBLISH_TOPIC, "->", payload_str)
    client.publish(BOOTSTRAP_PUBLISH_TOPIC, payload_str, qos=1)


def on_bootstrap_message(client, userdata, msg):
    try:
        text = msg.payload.decode()
        print("[bootstrap] message on", msg.topic, "payload:", text)
        data = json.loads(text)
    except Exception as e:
        print("[bootstrap] failed to parse message:", e)
        return

    # Expect something like:
    # { "broker_host": "real-broker.example.com", "broker_port": 8883,
    #   "username": "tenant_user", "password": "tenant_pass", "publish_topic": "..." }
    # Validate minimally:
    if "broker_host" in data:
        bootstrap_response.update(data)
        bootstrap_response_event.set()
        # disconnect after receiving
        client.disconnect()
    else:
        print("[bootstrap] message missing broker_host; ignoring")


def do_bootstrap():
    ctx = make_tls_context()
    client_id = CLIENT_ID_PREFIX + "bootstrap-" + socket.gethostname()
    client = mqtt.Client(client_id=client_id, clean_session=True)
    client.tls_set_context(ctx)

    # If you also need username/password for bootstrap, set it
    if BOOTSTRAP_USER:
        client.username_pw_set(BOOTSTRAP_USER, BOOTSTRAP_PASS)

    client.on_connect = on_bootstrap_connect
    client.on_message = on_bootstrap_message

    # optional: set will, logging, etc.
    try:
        client.connect(BOOTSTRAP_HOST, BOOTSTRAP_PORT, keepalive=60)
    except Exception as e:
        raise SystemExit(f"[bootstrap] connect failed: {e}")

    # run network loop in separate thread to allow timeout waiting for response
    client.loop_start()

    # wait until response or timeout
    if bootstrap_response_event.wait(timeout=BOOTSTRAP_RESPONSE_TIMEOUT):
        print("[bootstrap] received response:", bootstrap_response)
    else:
        client.loop_stop()
        client.disconnect()
        raise SystemExit("[bootstrap] timed out waiting for bootstrap response")

    client.loop_stop()
    client.disconnect()


# ---------- Connect to real broker and publish ----------
publish_done_event = threading.Event()


def on_real_connect(client, userdata, flags, rc):
    print("[real] connected rc=", rc)
    # publish a payload once connected. adapt payload/topic as needed.
    payload_json = json.dumps(BOOTSTRAP_PAYLOAD)
    topic = bootstrap_response.get("publish_topic", REAL_PUBLISH_TOPIC)
    print("[real] publishing to", topic, "payload:", payload_json)
    (rc_code, mid) = client.publish(topic, payload_json, qos=1)
    print("[real] publish invoked rc_code=", rc_code, "mid=", mid)


def on_real_publish(client, userdata, mid):
    print("[real] publish confirmed mid=", mid)
    publish_done_event.set()
    # disconnect after publish confirmed
    client.disconnect()


def publish_to_real_broker():
    # extract required values from bootstrap_response
    broker_host = bootstrap_response.get("broker_host")
    broker_port = int(bootstrap_response.get("broker_port", 8883))
    broker_user = bootstrap_response.get("username")
    broker_pass = bootstrap_response.get("password")

    if not broker_host:
        raise SystemExit("[real] no broker_host in bootstrap response")

    ctx = make_tls_context()
    client_id = CLIENT_ID_PREFIX + "device-" + socket.gethostname()
    client = mqtt.Client(client_id=client_id, clean_session=True)
    client.tls_set_context(ctx)

    if broker_user:
        client.username_pw_set(broker_user, broker_pass)

    client.on_connect = on_real_connect
    client.on_publish = on_real_publish

    try:
        client.connect(broker_host, broker_port, keepalive=60)
    except Exception as e:
        raise SystemExit(f"[real] connect failed: {e}")

    client.loop_start()

    # wait until publish confirmed or timeout
    if not publish_done_event.wait(timeout=10):
        client.loop_stop()
        client.disconnect()
        raise SystemExit("[real] timed out waiting for publish confirmation")

    client.loop_stop()
    client.disconnect()
    print("[real] publish complete, exiting")


# ---------------- Main flow ----------------
if __name__ == "__main__":
    print("Starting bootstrap sequence...")
    do_bootstrap()   # fills bootstrap_response on success

    # Optional: print a friendly note about server certificate verification
    if not CA_CERT_PATH:
        print("WARNING: CA_CERT_PATH is not set. Server certificate verification is DISABLED. Use only for testing.")

    print("Bootstrapped. Connecting to real broker and publishing...")
    publish_to_real_broker()
    print("All done.")
