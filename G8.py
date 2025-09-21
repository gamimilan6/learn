#!/usr/bin/env python3
import json, sys
import requests

# ========= EDIT THESE =========
REG_URL = "https://<your-reg-service-host>/api/register"  # ask your admin for the exact URL
SERIAL = "Z277PF3015"
ORGANIZATION = "X Service"   # whatever your flow calls "organization"
CA_FILE = "ca-certificates.crt"
TIMEOUT = 15
# ==============================

payload = {
    "serial": SERIAL,
    "organization": ORGANIZATION
    # If your API needs other names, e.g. {"agent_serial_number":..., "organization":...} adjust here
}

try:
    r = requests.post(REG_URL, json=payload, timeout=TIMEOUT, verify=CA_FILE)
except Exception as e:
    print(f"[ERR] Registration HTTP call failed: {e}")
    sys.exit(2)

print("[HTTP]", r.status_code)
print(r.text)

if r.status_code // 100 != 2:
    print("[ERR] Non-2xx response. Fix REG_URL/payload per your microservice.")
    sys.exit(3)

try:
    data = r.json()
except Exception:
    print("[ERR] Response is not JSON. Paste the text above to your admin or here.")
    sys.exit(4)

# Typical keys you might get back (examples – adapt to your real payload):
tenant = data.get("tenant") or data.get("tenantId") or data.get("tenant_code")
tenant_host = data.get("host") or data.get("tenantHost") or data.get("mqttHost")

device_user = data.get("deviceUsername")    # sometimes microservice returns operational device creds
device_pass = data.get("devicePassword")

print("\n[PARSED]")
print("tenant      :", tenant)
print("tenant_host :", tenant_host)
print("device_user :", device_user)
print("device_pass :", "***" if device_pass else None)

# Save what we found for the next script
out = {
    "tenant": tenant,
    "tenant_host": tenant_host,
    "device_user": device_user,
    "device_pass": device_pass,
    "serial": SERIAL,
    "type": ORGANIZATION,
}
with open("registration_result.json", "w") as f:
    json.dump(out, f, indent=2)
print("\n[OK] Wrote registration_result.json")
