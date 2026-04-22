#!/usr/bin/env python3
"""
Runs locally (Mac) every Monday 8:50 AM.
1. Refreshes the Superset access token
2. Pre-fetches US/EU metrics locally (Superset requires internal network)
3. Pushes tokens + pre-fetched data to GitHub Secrets
   so GitHub Actions has everything it needs when it fires at 9 AM.
"""
import base64, datetime, json, os, requests, time
from pathlib import Path

GITHUB_REPO  = 'karandoshi-svg/fm-performance-agent'
TOKEN_FILE   = Path('~/.pfm-superset-tokens.json').expanduser()
SUPERSET_MCP = 'https://superset.robinhood.com/mcp'

# Read PAT from env file
_env = {}
for line in Path('~/.pfm-agent.env').expanduser().read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k, v = line.strip().split('=', 1)
        _env[k] = v
GITHUB_PAT = _env.get('GITHUB_PAT', os.environ.get('GITHUB_PAT', ''))

# ── Step 1: Refresh Superset token locally ─────────────────────────────────────
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
tokens['access_token']            = data['access_token']
tokens['access_token_expires_at'] = time.time() + data.get('expires_in', 3600)
if 'refresh_token' in data:
    tokens['refresh_token'] = data['refresh_token']
TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
print(f"  Token refreshed (expires in {data.get('expires_in')}s)")

SUPA_TOKEN = tokens['access_token']
SUPA_HDRS  = {
    'Authorization': f'Bearer {SUPA_TOKEN}',
    'Content-Type':  'application/json',
    'Accept':        'application/json, text/event-stream',
}

# ── Step 2: Pre-fetch US/EU data locally ───────────────────────────────────────
PT = datetime.timezone(datetime.timedelta(hours=-7))
today        = datetime.datetime.now(tz=PT).date()
days_since_monday = today.weekday()
last_sunday  = today - datetime.timedelta(days=days_since_monday + 1)
last_monday  = last_sunday - datetime.timedelta(days=6)
Q2_START     = datetime.date(2026, 4, 1)

rpt_start = last_monday.strftime('%Y-%m-%d')
rpt_end   = (last_sunday + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
prev_start = (last_monday - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
qtd_start  = Q2_START.strftime('%Y-%m-%d')
qtd_end_s  = today.strftime('%Y-%m-%d')

def mcp_sql(sql):
    r = requests.post(SUPERSET_MCP, headers=SUPA_HDRS, timeout=90,
        json={'jsonrpc':'2.0','id':1,'method':'tools/call',
              'params':{'name':'execute_sql','arguments':{'request':{
                  'sql':sql,'database_id':2,'schema':'default','limit':1}}}})
    for line in r.text.splitlines():
        if line.startswith('data:'):
            d = line[5:].strip()
            if d and d != '[DONE]':
                p = json.loads(d)
                if 'result' in p:
                    return p['result'].get('structuredContent',{}).get('rows',[{}])[0]
    return {}

def mcp_chart(chart_id):
    r = requests.post(SUPERSET_MCP, headers=SUPA_HDRS, timeout=90,
        json={'jsonrpc':'2.0','id':1,'method':'tools/call',
              'params':{'name':'get_chart_data','arguments':{'request':{'identifier':chart_id}}}})
    for line in r.text.splitlines():
        if line.startswith('data:'):
            d = line[5:].strip()
            if d and d != '[DONE]':
                p = json.loads(d)
                if 'result' in p:
                    try:
                        return json.loads(p['result']['content'][0]['text'])
                    except Exception:
                        pass
    return None

def row_total(row):
    return sum(v for v in row.values() if isinstance(v,(int,float)) and v)

def get_last_two_rows(chart):
    rows = (chart or {}).get('data', [])
    rows = [r for r in rows if any(isinstance(v,(int,float)) and v for v in r.values())]
    return (rows[-2] if len(rows)>=2 else {}), (rows[-1] if rows else {})

print("Pre-fetching US metrics...")
us_week = mcp_sql(f"SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas, SUM(acq_ltv_amount)/SUM(cost_amount) as roi FROM pfm_2025_report WHERE dt_epoch_ms >= '{rpt_start}' AND dt_epoch_ms < '{rpt_end}' AND cost_amount > 0 LIMIT 1")
us_prev = mcp_sql(f"SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas, SUM(acq_ltv_amount)/SUM(cost_amount) as roi FROM pfm_2025_report WHERE dt_epoch_ms >= '{prev_start}' AND dt_epoch_ms < '{rpt_start}' AND cost_amount > 0 LIMIT 1")
us_qtd  = mcp_sql(f"SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas, SUM(gold_amount) as gold FROM pfm_2025_report WHERE dt_epoch_ms >= '{qtd_start}' AND dt_epoch_ms < '{qtd_end_s}' AND cost_amount > 0 LIMIT 1")
print(f"  US week spend={us_week.get('spend',0):.0f} nfas={us_week.get('nfas',0):.0f}")

print("Pre-fetching EU metrics...")
eu_spend_chart = mcp_chart(22464)
eu_nfa_chart   = mcp_chart(10731)
eu_roi_chart   = mcp_chart(10101)
eu_sp_prev, eu_sp_curr = get_last_two_rows(eu_spend_chart)
eu_nf_prev, eu_nf_curr = get_last_two_rows(eu_nfa_chart)
eu_roi_rows = (eu_roi_chart or {}).get('data', [])
print(f"  EU week spend={row_total(eu_sp_curr):.0f} nfas={int(row_total(eu_nf_curr))}")

prefetched = {
    'fetched_at': datetime.datetime.now(tz=PT).isoformat(),
    'rpt_start': rpt_start,
    'rpt_end': rpt_end,
    'us_week': {k: (float(v) if v else 0) for k,v in us_week.items()},
    'us_prev': {k: (float(v) if v else 0) for k,v in us_prev.items()},
    'us_qtd':  {k: (float(v) if v else 0) for k,v in us_qtd.items()},
    'eu_w_spend': row_total(eu_sp_curr),
    'eu_p_spend': row_total(eu_sp_prev),
    'eu_w_nfas':  int(row_total(eu_nf_curr)),
    'eu_p_nfas':  int(row_total(eu_nf_prev)),
    'eu_w_roi':   eu_roi_rows[-2].get('ROI',0) if len(eu_roi_rows)>=2 else 0,
    'eu_p_roi':   eu_roi_rows[-3].get('ROI',0) if len(eu_roi_rows)>=3 else 0,
}
print(f"  Pre-fetch complete.")

# ── Step 3: Get GitHub repo public key ────────────────────────────────────────
hdrs = {'Authorization': f'Bearer {GITHUB_PAT}', 'Accept': 'application/vnd.github+json'}
key_resp = requests.get(
    f'https://api.github.com/repos/{GITHUB_REPO}/actions/secrets/public-key',
    headers=hdrs, timeout=10)
key_resp.raise_for_status()
pubkey_data = key_resp.json()
pubkey_id   = pubkey_data['key_id']
pubkey_b64  = pubkey_data['key']

from nacl import public as nacl_public, encoding as nacl_enc

def encrypt_secret(value: str) -> str:
    pk  = nacl_public.PublicKey(pubkey_b64.encode(), nacl_enc.Base64Encoder)
    box = nacl_public.SealedBox(pk)
    return base64.b64encode(box.encrypt(value.encode())).decode()

def update_secret(name: str, value: str):
    r = requests.put(
        f'https://api.github.com/repos/{GITHUB_REPO}/actions/secrets/{name}',
        headers=hdrs,
        json={'encrypted_value': encrypt_secret(value), 'key_id': pubkey_id},
        timeout=10)
    status = 'OK' if r.status_code in (201, 204) else f'ERROR {r.status_code}'
    print(f"  {name}: {status}")

# ── Step 4: Push everything to GitHub ─────────────────────────────────────────
print("Pushing to GitHub Secrets...")
update_secret('SUPERSET_ACCESS_TOKEN',  tokens['access_token'])
update_secret('SUPERSET_REFRESH_TOKEN', tokens['refresh_token'])
update_secret('PREFETCHED_DATA',        json.dumps(prefetched))
print("Done. GitHub Actions will have full US/EU data at 9 AM.")
