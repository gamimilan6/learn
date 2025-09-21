#!/usr/bin/env python3
"""
c8y_debug_connect.py

Diagnostic tool for troubleshooting TLS+MQTT bootstrap to Cumulocity.

Place 'ca-certificates.crt' in same folder. Edit CONFIG section.

Outputs:
 - TCP reachability test
 - TLS handshake + server certificate details (uses CA bundle)
 - MQTT connection attempt with verbose logging (paho-mqtt)
"""
import socket
import ssl
import sys
import traceback
import time
from paho.mqtt import client as mqtt

# ---------------- CONFIG ----------------
BOOTSTRAP_HOST = "mqtt-bootstrap.example.com"   # <-- EDIT
BOOTSTRAP_PORT = 8883                          # <-- EDIT

BOOTSTRAP_USERNAME = "bootstrapUser"           # or "tenant/bootstrapUser"
BOOTSTRAP_PASSWORD = "bootstrapPassword"
BOOTSTRAP_CLIENT_ID = "bootstrap-client-1"

CA_CERTS = "ca-certificates.crt"               # placed in same folder
BOOTSTRAP_TOPIC = "s/us"
SMARTREST_PAYLOAD = "1000,Z277PF3015,X Service"

TCP_TIMEOUT = 8
TLS_TIMEOUT = 8
MQTT_CONNECT_TIMEOUT = 12
# -----------------------------------------

def test_tcp_connect(host, port, timeout=5):
    print("\n== [1] TCP reachability test ==")
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        print(f"[+] TCP connect OK: {host}:{port}")
        s.close()
        return True
    except Exception as e:
        print(f"[-] TCP connect FAILED: {e!r}")
        print("    -> Check network, firewall, host/port, and DNS resolution.")
        return False

def test_tls_handshake(host, port, ca_bundle, timeout=8):
    print("\n== [2] TLS handshake and certificate inspection ==")
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # Force minimum TLS 1.2
        try:
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        except AttributeError:
            context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        context.load_verify_locations(cafile=ca_bundle)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True

        raw_sock = socket.create_connection((host, port), timeout=timeout)
        raw_sock.settimeout(timeout)
        ssl_sock = context.wrap_socket(raw_sock, server_hostname=host)
        # If wrap_socket succeeds, print details
        cert = ssl_sock.getpeercert(binary_form=False)
        der_cert = ssl_sock.getpeercert(binary_form=True)
        print("[+] TLS handshake succeeded (using CA bundle).")
        # Certificate basic info:
        subject = cert.get('subject', ())
        issuer = cert.get('issuer', ())
        not_before = cert.get('notBefore', 'N/A')
        not_after = cert.get('notAfter', 'N/A')
        print("Server certificate info:")
        print("  subject:", subject)
        print("  issuer :", issuer)
        print(f"  valid  : {not_before}  ->  {not_after}")
        # fingerprint (SHA256)
        import hashlib
        fp_sha256 = hashlib.sha256(der_cert).hexdigest()
        fp_sha1 = hashlib.sha1(der_cert).hexdigest()
        print("  fingerprint (SHA256):", fp_sha256)
        print("  fingerprint (SHA1)  :", fp_sha1)
        # negotiated TLS version and cipher
        print("TLS negotiated:", ssl_sock.version(), ssl_sock.cipher())
        ssl_sock.close()
        return True
    except ssl.SSLCertVerificationError as ce:
        print("[-] TLS certificate verification FAILED:")
        print("    ", repr(ce))
        print("    -> Possible causes: CA bundle doesn't include issuer, hostname mismatch, expired certificate.")
        traceback.print_exc()
        return False
    except Exception as e:
        print("[-] TLS handshake FAILED:", repr(e))
        traceback.print_exc()
        return False

# MQTT logging callback
def on_log(client, userdata, level, buf):
    # Print everything; helpful for debugging
    print("[paho-log]", level, buf)

def on_connect(client, userdata, flags, rc):
    print(f"[paho] on_connect: rc={rc}, flags={flags}")
    if rc == 0:
        print("[paho] MQTT connect OK")
    else:
        print("[paho] MQTT connect returned non-zero rc (see MQTT v3.1/3.1.1 codes):", rc)

def on_message(client, userdata, msg):
    print(f"[paho] MSG recv topic={msg.topic} payload={msg.payload!r}")

def mqtt_connect_test(host, port, ca_bundle, username, password, client_id, topic, payload):
    print("\n== [3] MQTT connect attempt (paho-mqtt, verbose) ==")
    client = mqtt.Client(client_id=client_id, clean_session=True)
    client.on_log = on_log
    client.on_connect = on_connect
    client.on_message = on_message

    if username:
        client.username_pw_set(username, password)

    # TLS context like before
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        try:
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        except AttributeError:
            context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        context.load_verify_locations(cafile=ca_bundle)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
    except Exception as e:
        print("[-] Failed creating TLS context:", repr(e))
        traceback.print_exc()
        return False

    try:
        client.tls_set_context(context)
        client.tls_insecure_set(False)
    except Exception as e:
        print("[-] client.tls_set_context failed:", repr(e))
        traceback.print_exc()
        return False

    # Try connecting (this will run network loop)
    try:
        client.connect(host, port, keepalive=60)
    except Exception as e:
        print("[-] client.connect threw exception:", repr(e))
        traceback.print_exc()
        return False

    # Start loop, publish, wait briefly, then disconnect
    client.loop_start()
    try:
        # wait for connect
        waited = 0
        while waited < MQTT_CONNECT_TIMEOUT:
            time.sleep(0.5)
            waited += 0.5
        # publish a SmartREST payload (QoS 1)
        print("[paho] Publishing test SmartREST payload (qos=1):", payload)
        rc = client.publish(topic, payload, qos=1)
        print("[paho] publish() returned:", rc)
        # wait a bit for responses
        time.sleep(2)
    except Exception as e:
        print("[-] Error during MQTT loop/publish:", repr(e))
        traceback.print_exc()
    finally:
        client.loop_stop()
        client.disconnect()
        print("[paho] Disconnected.")
    return True

if __name__ == "__main__":
    print("c8y_debug_connect.py starting — will perform TCP / TLS / MQTT checks.")
    print("EDIT the CONFIG at top to point to your bootstrap host/credentials if needed.\n")
    # 1) TCP test
    tcp_ok = test_tcp_connect(BOOTSTRAP_HOST, BOOTSTRAP_PORT, timeout=TCP_TIMEOUT)

    # 2) TLS handshake
    tls_ok = test_tls_handshake(BOOTSTRAP_HOST, BOOTSTRAP_PORT, CA_CERTS, timeout=TLS_TIMEOUT)

    # 3) MQTT connect
    mqtt_ok = mqtt_connect_test(BOOTSTRAP_HOST, BOOTSTRAP_PORT, CA_CERTS,
                                BOOTSTRAP_USERNAME, BOOTSTRAP_PASSWORD,
                                BOOTSTRAP_CLIENT_ID, BOOTSTRAP_TOPIC, SMARTREST_PAYLOAD)

    print("\n=== SUMMARY ===")
    print("TCP reachability:", "OK" if tcp_ok else "FAILED")
    print("TLS handshake    :", "OK" if tls_ok else "FAILED")
    print("MQTT connect     :", "Attempted (see above output)")

    if not tcp_ok:
        print("\nNext steps if TCP failed:")
        print(" - Check DNS: run `nslookup {}` or `ping {}` on your machine.".format(BOOTSTRAP_HOST))
        print(" - Check firewall to the bootstrap port ({}).".format(BOOTSTRAP_PORT))
        print(" - Ask your network team if port is blocked.\n".format(BOOTSTRAP_PORT))

    if tcp_ok and not tls_ok:
        print("\nNext steps if TLS failed:")
        print(" - Ensure 'ca-certificates.crt' matches the CA that signed the broker certificate.")
        print(" - Run on your machine: `openssl s_client -connect {}:{} -CAfile {}`".format(BOOTSTRAP_HOST, BOOTSTRAP_PORT, CA_CERTS))
        print(" - Check certificate CN/SAN matches the hostname you used ({}).".format(BOOTSTRAP_HOST))

    if tls_ok and not mqtt_ok:
        print("\nNext steps if MQTT failed despite TLS success:")
        print(" - Check bootstrap username format (sometimes must be tenant/username).")
        print(" - Make sure bootstrap user/password is correct.")
        print(" - Verify the bootstrap topic is correct (s/us is common).")
        print(" - Enable server-side logs (if you have access) to see why it rejects connect/publish.")

    print("\nWhen you run this, paste the full console output here and I'll analyze it and give the exact fix.")
