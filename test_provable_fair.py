import hashlib
import hmac

# ── Example inputs from your row ──────────────────────────
server_seed_hash = "c6656428c631747b6d9c89232eab201ad8dc187f19f74dda18dbaf67dc1a8268"
server_seed      = "b2184bbbbb1e9dc438f99ba24e3610999adb419bc8bc5ed2f9d200e174f4a8fb"
client_seed      = "77ecfa83b02c6f630d7636bd3af18b7f"
nonce            = 6
random_hash      = "ddb1510947b80f351a19607853aa6918d404d7d1737a009d78508d81097abdcd"
# ──────────────────────────────────────────────────────────

# 1) Check the seed hash
calc_hash = hashlib.sha256(server_seed.encode()).hexdigest()
if calc_hash != server_seed_hash:
    print("❌ server_seed_hash mismatch!")
else:
    print("✅ server_seed_hash is valid")

# 2) Recompute the HMAC for the batch
payload = f"{client_seed}{nonce}".encode()
calc_hmac = hmac.new(server_seed.encode(), payload, hashlib.sha256).hexdigest()
if calc_hmac != random_hash:
    print("❌ random_hash (HMAC) mismatch!")
else:
    print("✅ random_hash is valid — your draw was provably fair")
