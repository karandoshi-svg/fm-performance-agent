#!/usr/bin/env python3
"""
Runs locally (Mac) every Monday 8:50 AM.
Refreshes the Superset access token and pushes it to GitHub Secrets
so GitHub Actions has a valid token when it fires at 9 AM.
"""
import base64, json, os, requests, time
from pathlib import Path

GITHUB_REPO  = 'karandoshi-svg/fm-performance-agent'
TOKEN_FILE   = Path('~/.pfm-superset-tokens.json').expanduser()

# Read PAT from env file
_env = {}
for line in Path('~/.pfm-agent.env').expanduser().read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k, v = line.strip().split('=', 1)
        _env[k] = v
GITHUB_PAT = _env.get('GITHUB_PAT', os.environ.get('GITHUB_PAT', ''))

# ── Step 1: Refresh Superset token locally ────────────────────────────────────
tokens = json.loads(TOKEN_FILE.read_text())
print("Refreshing Superset token locally...")
resp = requests.post('https://superset.robinhood.com/token', data={
    'grant_type':    'refresh_token',
    'client_id':     tokens['client_id'],
    'client_secret': tokens['client_secret'],
    'refresh_token': tokens['refresh_token'],
}, timeout=15)

if resp.status_code != 200:
    print(f"ERROR: Token refresh failed ({resp.status_code}): {resp.text[:200]}")
    raise SystemExit(1)

data = resp.json()
tokens['access_token']           = data['access_token']
tokens['access_token_expires_at'] = time.time() + data.get('expires_in', 3600)
if 'refresh_token' in data:
    tokens['refresh_token'] = data['refresh_token']
TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
print(f"  Token refreshed (expires in {data.get('expires_in')}s)")

# ── Step 2: Get GitHub repo public key for secret encryption ──────────────────
hdrs = {
    'Authorization': f'Bearer {GITHUB_PAT}',
    'Accept': 'application/vnd.github+json',
}
key_resp = requests.get(
    f'https://api.github.com/repos/{GITHUB_REPO}/actions/secrets/public-key',
    headers=hdrs, timeout=10,
)
key_resp.raise_for_status()
pubkey_data = key_resp.json()
pubkey_id  = pubkey_data['key_id']
pubkey_b64 = pubkey_data['key']

# ── Step 3: Encrypt secret value with repo public key (libsodium) ─────────────
try:
    from nacl import public as nacl_public, encoding as nacl_enc
    def encrypt_secret(value: str) -> str:
        pk = nacl_public.PublicKey(pubkey_b64.encode(), nacl_enc.Base64Encoder)
        box = nacl_public.SealedBox(pk)
        encrypted = box.encrypt(value.encode())
        return base64.b64encode(encrypted).decode()
except ImportError:
    # Fallback: install pynacl inline
    import subprocess, sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pynacl', '-q'])
    from nacl import public as nacl_public, encoding as nacl_enc
    def encrypt_secret(value: str) -> str:
        pk = nacl_public.PublicKey(pubkey_b64.encode(), nacl_enc.Base64Encoder)
        box = nacl_public.SealedBox(pk)
        encrypted = box.encrypt(value.encode())
        return base64.b64encode(encrypted).decode()

# ── Step 4: Push secrets to GitHub ───────────────────────────────────────────
def update_secret(name: str, value: str):
    encrypted = encrypt_secret(value)
    r = requests.put(
        f'https://api.github.com/repos/{GITHUB_REPO}/actions/secrets/{name}',
        headers=hdrs,
        json={'encrypted_value': encrypted, 'key_id': pubkey_id},
        timeout=10,
    )
    if r.status_code in (201, 204):
        print(f"  Updated {name}")
    else:
        print(f"  ERROR updating {name}: {r.status_code} {r.text[:100]}")

print("Pushing fresh tokens to GitHub Secrets...")
update_secret('SUPERSET_ACCESS_TOKEN',  tokens['access_token'])
update_secret('SUPERSET_REFRESH_TOKEN', tokens['refresh_token'])
print("Done. GitHub Actions will use a fresh token in 10 minutes.")
