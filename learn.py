#!/usr/bin/env python3
"""
connect_tls_env_ca.py

Connect to MQTT broker using TLS 1.2. Read settings from env.json (same folder).
Uses Linuxcafilename or RootCAName (if present) for server certificate verification.
Does NOT attempt mutual TLS (no client key/cert).

Requirements:
    pip install paho-mqtt
"""

import json
import logging
import ssl
import time
from pathlib import Path
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("connect-tls-env-ca")

BASE = Path(__file__).parent
ENV_FILE = BASE / "env.json"
PROFILE_FILE = BASE / "profile.json"
PERSIST_FILE = BASE / "persist.json"

def load_json(path: Path):
    if not path.exists():
        log.debug("Not found: %s", path)
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Failed to parse %s: %s", path.name, e)
        return {}

def find_ca_file(env):
    """
    Look for CA using env keys Linuxcafilename or RootCAName.
    Return path string if found, else None.
    """
    linux_ca = env.get("Linuxcafilename") or env.get("linuxcafilename")
    root_ca = env.get("RootCAName") or env.get("rootCAName") or env.get("RootCA")
    # 1) try linux_ca as absolute path
    if linux_ca:
        p = Path(linux_ca)
        if p.is_file():
            log.info("Using CA from Linuxcafilename (absolute): %s", p)
            return str(p)
        # 2) try relative to script folder
        p2 = BASE / linux_ca
        if p2.is_file():
            log.info("Using CA from Linuxcafilename (relative): %s", p2)
            return str(p2)

    # 3) try RootCAName relative to script
    if root_ca:
        p3 = BASE / root_ca
        if p3.is_file():
            log.info("Using CA from RootCAName (relative): %s", p3)
            return str(p3)
        # maybe RootCAName is an absolute path too
        p4 = Path(root_ca)
        if p4.is_file():
            log.info("Using CA from RootCAName (absolute): %s", p4)
            return str(p4)

    # no CA file found in env — return None to indicate system store should be used
    log.info("No CA file found in env.json keys (Linuxcafilename/RootCAName). Will use system CA store.")
    return None

def build_config(env, profile, persist):
    host = env.get("RemoteServiceBootstrapBrokerHostName") or env.get("MQTTHostname") or "localhost"
    # prefer secure port key
    secure_port = env.get("SecureMQTTPort") or env.get("secureMQTTPort") or None
    bootstrap_port = env.get("RemoteServiceBootstrapBrokerPort") or env.get("RemoteServiceBootstrapBrokerPort")
    port = int(secure_port or bootstrap_port or 8883)

    username = env.get("RemoteServiceBootstrapBrokerUsername") or env.get("MQTTUsername") or None
    password = env.get("RemoteServiceBootstrapBrokerPassword") or env.get("MQTTPassword") or None

    serial = env.get("SerialNumber") or "unknown-serial"
    rso = env.get("RemoteServiceOwnershipInfo") if isinstance(env, dict) else None
    org = None
    if isinstance(rso, dict):
        org = rso.get("organization") or rso.get("Organization")
    org = org or env.get("organization") or env.get("Organization") or "unknown-org"

    bootstrap_topic = "device/bootstrap"
    subscribe_topic = f"device/{serial}/#"

    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "serial": str(serial),
        "organization": str(org),
        "bootstrap_topic": bootstrap_topic,
        "subscribe_topic": subscribe_topic
    }

def make_tls_context(ca_path: str | None):
    """
    Create SSLContext enforcing certificate verification and TLS 1.2+.
    If ca_path is None, rely on system CA store.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # disable old protocols
    ctx.options |= ssl.OP_NO_TLSv1
    ctx.options |= ssl.OP_NO_TLSv1_1
    # do NOT disable TLSv1_3 here; allow v1.2+ by default
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True

    if ca_path:
        try:
            ctx.load_verify_locations(cafile=ca_path)
            log.info("Loaded CA file: %s", ca_path)
        except Exception as e:
            log.error("Failed to load CA file %s: %s", ca_path, e)
            raise
    else:
        log.info("Using system CA bundle for verification.")

    # Optionally set strong ciphers here if required by broker
    # ctx.set_ciphers("ECDHE+AESGCM")

    return ctx

def main():
    env = load_json(ENV_FILE)
    profile = load_json(PROFILE_FILE)
    persist = load_json(PERSIST_FILE)

    cfg = build_config(env, profile, persist)
    ca_path = find_ca_file(env)

    log.info("Broker to connect: %s:%s (TLS expected)", cfg["host"], cfg["port"])

    # Create client with MQTT v3.1.1
    client_id = f"device-{cfg['serial']}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, clean_session=True)

    if cfg["username"] and cfg["password"]:
        client.username_pw_set(cfg["username"], cfg["password"])
        log.info("Using username auth (username=%s)", cfg["username"])
    else:
        log.info("No username/password provided in env.json")

    # Create TLS context and attach
    try:
        tls_ctx = make_tls_context(ca_path)
    except Exception:
        log.error("TLS context creation failed. Aborting.")
        return

    client.tls_set_context(tls_ctx)
    client.tls_insecure_set(False)   # strict verification

    # Callbacks
    def on_connect(c, userdata, flags, rc):
        code_map = {
            0: "Connection accepted",
            1: "Refused: unacceptable protocol version",
            2: "Refused: identifier rejected",
            3: "Refused: server unavailable",
            4: "Refused: bad username or password",
            5: "Refused: not authorized"
        }
        log.info("on_connect rc=%s -> %s", rc, code_map.get(rc, "Unknown"))
        if rc == 0:
            c.subscribe(cfg["subscribe_topic"])
            log.info("Subscribed to: %s", cfg["subscribe_topic"])
            payload = {"SerialNumber": cfg["serial"], "Organization": cfg["organization"], "Timestamp": int(time.time())}
            c.publish(cfg["bootstrap_topic"], json.dumps(payload), qos=1)
            log.info("Published bootstrap to %s", cfg["bootstrap_topic"])

    def on_message(c, userdata, msg):
        try:
            txt = msg.payload.decode("utf-8", errors="replace")
            log.info("Message on %s: %s", msg.topic, txt)
        except Exception as e:
            log.exception("Failed to decode message: %s", e)

    def on_disconnect(c, userdata, rc):
        log.warning("Disconnected (rc=%s)", rc)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Try connect
    try:
        client.connect(cfg["host"], cfg["port"], keepalive=60)
    except Exception as e:
        log.exception("Socket-level connect failed: %s", e)
        return

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log.info("User interrupted - disconnecting")
        client.disconnect()

if __name__ == "__main__":
    main()
