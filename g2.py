import ssl
import json
import time
import paho.mqtt.client as mqtt

# -------------------------------------------------
# Load bootstrap data (example - replace with your file)
# -------------------------------------------------
with open("bootstrap.json", "r") as f:
    bootstrap = json.load(f)

host = bootstrap["Hostname"]
tenant = bootstrap["TenantId"]
user = bootstrap["Username"]
pwd  = bootstrap["Password"]   # can be empty if server only uses certs

mqtt_user = f"{tenant}/{user}"
port = 8883
keepalive = 20

# -------------------------------------------------
# TLS settings
# -------------------------------------------------
CA_CERT     = "ca.crt"       # server's CA certificate (needed if private CA)
CLIENT_CERT = "client.pem"   # your PEM file containing cert+key
CLIENT_KEY  = "client.key"   # if key is in separate file, else None

# Create MQTT client
client = mqtt.Client(client_id="my-device-001")

# Set username/password if bootstrap gave them
if pwd:
    client.username_pw_set(mqtt_user, pwd)

# Configure SSL context
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.verify_mode = ssl.CERT_REQUIRED

# Load CA certificate if provided
try:
    ctx.load_verify_locations(CA_CERT)
    print(f"Loaded CA certificate: {CA_CERT}")
except Exception as e:
    print(f"No CA cert loaded ({e}), using system store")
    ctx.load_default_certs()

# Load client certificate/key
try:
    if CLIENT_KEY:
        ctx.load_cert_chain(certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
    else:
        ctx.load_cert_chain(certfile=CLIENT_CERT)
    print(f"Loaded client certificate: {CLIENT_CERT}")
except Exception as e:
    print("Failed to load client cert/key:", e)
    raise

# Apply TLS settings
client.tls_set_context(ctx)
client.tls_insecure_set(False)

# -------------------------------------------------
# Callbacks
# -------------------------------------------------
def on_connect(c, u, f, rc):
    print("Connected with result code:", rc)
    # Subscribe to a topic (example)
    c.subscribe("s/us")

def on_message(c, u, msg):
    print(f"Received: topic={msg.topic}, payload={msg.payload.decode(errors='ignore')}")

def on_publish(c, u, mid):
    print("Message published, mid:", mid)

def on_disconnect(c, u, rc):
    print("Disconnected, rc:", rc)

client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.on_disconnect = on_disconnect

# -------------------------------------------------
# Connect & Loop
# -------------------------------------------------
print(f"Connecting to {host}:{port} as {mqtt_user}")
client.connect(host, port, keepalive)
client.loop_start()

# Example publish every 10 seconds
try:
    while True:
        payload = '{"c8y_Temperature": {"T": 26.5}}'
        client.publish("s/us", payload, qos=1)
        time.sleep(10)
except KeyboardInterrupt:
    print("Stopping...")
finally:
    client.loop_stop()
    client.disconnect()
