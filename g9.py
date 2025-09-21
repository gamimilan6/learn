#!/usr/bin/env python3
import json, time, ssl, sys, queue
from paho.mqtt import client as mqtt

CA_FILE = "ca-certificates.crt"

# ========= EDIT THESE (fallbacks) =========
DEFAULT_MQTT_HOST = "deviot.hillrom.com"
PORT = 8883

BOOTSTRAP_USER_ONLY = "devicebootstrap"   # common default; your admin may use a different name
BOOTSTRAP_PASS = "<bootstrap-password>"   # ask your admin if needed
BOOTSTRAP_TOPIC = "s/us"

CLIENT_ID = "Centrella-Z277PF3015"
SERIAL_DEFAULT = "Z277PF3015"
TYPE_DEFAULT = "X Service"
# =========================================

def tls_ctx():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    except AttributeError:
        ctx.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    ctx.load_verify_locations(CA_FILE)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    return ctx

def connect_and_publish(host, username, password, payload):
    q = queue.Queue()
    def on_connect(c,u,f,rc):
        print(f"[on_connect] rc={rc}")
        if rc==0:
            c.subscribe(BOOTSTRAP_TOPIC, qos=1)
            c.publish(BOOTSTRAP_TOPIC, payload, qos=1)
            print("[MQTT] Published:", payload)
        else:
            print("[MQTT] Not authorized or failed (rc=", rc, ")")

    def on_message(c,u,m):
        print("\n--- RX on", m.topic, "---\n", m.payload.decode(errors="replace"), "\n-------------")
        q.put(True)

    cli = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
    cli.on_connect = on_connect
    cli.on_message = on_message
    if username:
        cli.username_pw_set(username, password)
    cli.tls_set_context(tls_ctx())
    cli.tls_insecure_set(False)
    print(f"[MQTT] Connecting to {host}:{PORT} as {username!r} ...")
    cli.connect(host, PORT, keepalive=60)
    cli.loop_start()
    try:
        got = q.get(timeout=20)
    except Exception:
        print("[MQTT] No reply within 20s.")
    finally:
        cli.loop_stop(); cli.disconnect()

def main():
    try:
        with open("registration_result.json") as f:
            reg = json.load(f)
    except Exception as e:
        print("[ERR] Cannot read registration_result.json:", e)
        sys.exit(2)

    tenant = reg.get("tenant")
    host = reg.get("tenant_host") or DEFAULT_MQTT_HOST
    device_user = reg.get("device_user")
    device_pass = reg.get("device_pass")
    serial = reg.get("serial") or SERIAL_DEFAULT
    dev_type = reg.get("type") or TYPE_DEFAULT

    payload = f"1000,{serial},{dev_type}"

    # If the microservice returned device credentials, connect operationally and publish to s/us
    if device_user and device_pass:
        username = f"{tenant}/{device_user}" if tenant and "/" not in device_user else device_user
        print("[MODE] Using operational device credentials returned by registration microservice.")
        connect_and_publish(host, username, device_pass, payload)
        return

    # Otherwise do bootstrap using tenant/devicebootstrap (VERY common pattern)
    if not tenant:
        print("[ERR] Registration did not return tenant; cannot form 'tenant/username'. Fix registration step.")
        sys.exit(3)

    bootstrap_username = f"{tenant}/{BOOTSTRAP_USER_ONLY}"
    print("[MODE] Bootstrap mode using:", bootstrap_username)
    connect_and_publish(host, bootstrap_username, BOOTSTRAP_PASS, payload)

if __name__ == "__main__":
    main()
