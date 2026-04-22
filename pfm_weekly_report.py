#!/usr/bin/env python3
"""
PFM Weekly Performance Report — posts every Monday 9 AM PT to #pfm-ai-insights
Auto-determines reporting week (previous Mon–Sun) and QTD pacing window.
"""
import json, requests, time, datetime
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
ENV_FILE     = Path('~/.pfm-agent.env').expanduser()
TOKEN_FILE   = Path('~/.pfm-superset-tokens.json').expanduser()
GTOKEN_FILE  = Path('~/.config/google-mcp/tokens.json').expanduser()
SHEET_ID     = '1nqqEFXnQOLrc77hoQ85ORPXEDrPK9kOVpjsMKMsLtiU'
DOC_URL      = 'https://docs.google.com/document/d/117NWFxfLP50l6RGIBRc2V6rE7WL9VeQJAE25wz5nHoI'
SUPERSET_BASE = 'https://superset.robinhood.com/mcp'
UK_Q2_TARGET = 1_200_000
US_Q2_TARGET = 46_200_000
Q2_START     = datetime.date(2026, 4, 1)
Q2_END       = datetime.date(2026, 6, 30)

# ── Load env ────────────────────────────────────────────────────────────────
env = {}
for line in ENV_FILE.read_text().splitlines():
    if '=' in line:
        k, v = line.strip().split('=', 1)
        env[k] = v
SLACK_TOKEN = env['SLACK_BOT_TOKEN']

# ── Date windows ────────────────────────────────────────────────────────────
PT = datetime.timezone(datetime.timedelta(hours=-8))  # PST
today = datetime.datetime.now(tz=PT).date()
# Reporting week = previous completed Mon–Sun
days_since_monday = today.weekday()  # Mon=0
last_sunday  = today - datetime.timedelta(days=days_since_monday + 1)
last_monday  = last_sunday - datetime.timedelta(days=6)
week_label   = f"{last_monday.strftime('%b %-d')}–{last_sunday.strftime('%-d, %Y')}"
qtd_end      = today - datetime.timedelta(days=1)  # yesterday = most recent complete day
q2_days_elapsed = (today - Q2_START).days
q2_pct_elapsed  = q2_days_elapsed / 91

rpt_start = last_monday.strftime('%Y-%m-%d')
rpt_end   = (last_sunday + datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # exclusive
qtd_start = Q2_START.strftime('%Y-%m-%d')
qtd_end_s = today.strftime('%Y-%m-%d')  # exclusive

print(f"Reporting week: {rpt_start} to {rpt_end}")
print(f"QTD window: {qtd_start} to {qtd_end_s} (day {q2_days_elapsed} of 91, {q2_pct_elapsed:.1%})")

# ── Superset auth ────────────────────────────────────────────────────────────
def get_superset_token():
    tokens = json.loads(TOKEN_FILE.read_text())
    access_token = tokens.get('access_token', '')
    exp = tokens.get('access_token_expires_at', 0)
    if time.time() < exp - 60:
        return access_token
    # Refresh
    client_id = tokens['client_id']
    client_secret = tokens['client_secret']
    resp = requests.post(f'https://superset.robinhood.com/token', data={
        'grant_type': 'refresh_token', 'client_id': client_id,
        'client_secret': client_secret, 'refresh_token': tokens['refresh_token']
    }, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    tokens['access_token'] = data['access_token']
    tokens['access_token_expires_at'] = time.time() + data.get('expires_in', 3600)
    if 'refresh_token' in data:
        tokens['refresh_token'] = data['refresh_token']
    TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
    return tokens['access_token']

SUPA_TOKEN = get_superset_token()
SUPA_HDRS  = {'Authorization': f'Bearer {SUPA_TOKEN}', 'Content-Type': 'application/json',
              'Accept': 'application/json, text/event-stream'}

def mcp_sql(sql):
    resp = requests.post(SUPERSET_BASE, headers=SUPA_HDRS, timeout=90,
        json={'jsonrpc':'2.0','id':1,'method':'tools/call',
              'params':{'name':'execute_sql','arguments':{'request':{'sql':sql,'database_id':2,'schema':'default','limit':1}}}})
    for line in resp.text.splitlines():
        if line.startswith('data:'):
            d = line[5:].strip()
            if d and d != '[DONE]':
                p = json.loads(d)
                if 'result' in p:
                    return p['result'].get('structuredContent', {}).get('rows', [{}])[0]
    return {}

def mcp_chart(chart_id):
    resp = requests.post(SUPERSET_BASE, headers=SUPA_HDRS, timeout=90,
        json={'jsonrpc':'2.0','id':1,'method':'tools/call',
              'params':{'name':'get_chart_data','arguments':{'request':{'identifier':chart_id}}}})
    for line in resp.text.splitlines():
        if line.startswith('data:'):
            d = line[5:].strip()
            if d and d != '[DONE]':
                p = json.loads(d)
                if 'result' in p:
                    try:
                        return json.loads(p['result']['content'][0]['text'])
                    except: pass
    return None

# ── US metrics ───────────────────────────────────────────────────────────────
print("Fetching US weekly metrics...")
us_week = mcp_sql(f"""SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas,
    SUM(acq_ltv_amount)/SUM(cost_amount) as roi FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{rpt_start}' AND dt_epoch_ms < '{rpt_end}' AND cost_amount > 0 LIMIT 1""")

us_prev = mcp_sql(f"""SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas,
    SUM(acq_ltv_amount)/SUM(cost_amount) as roi FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{(last_monday - datetime.timedelta(days=7)).strftime('%Y-%m-%d')}'
    AND dt_epoch_ms < '{rpt_start}' AND cost_amount > 0 LIMIT 1""")

us_qtd = mcp_sql(f"""SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{qtd_start}' AND dt_epoch_ms < '{qtd_end_s}' AND cost_amount > 0 LIMIT 1""")

us_w_spend = us_week.get('spend', 0) or 0
us_w_nfas  = int(us_week.get('nfas', 0) or 0)
us_w_roi   = us_week.get('roi', 0) or 0
us_p_spend = us_prev.get('spend', 0) or 0
us_p_nfas  = int(us_prev.get('nfas', 0) or 0)
us_p_roi   = us_prev.get('roi', 0) or 0
us_qtd_sp  = us_qtd.get('spend', 0) or 0

us_spend_wow = f"+{(us_w_spend/us_p_spend-1)*100:.0f}%" if us_p_spend else "n/a"
us_nfas_wow  = f"+{(us_w_nfas/us_p_nfas-1)*100:.0f}%" if us_p_nfas else "n/a"
us_roi_wow   = f"{us_w_roi - us_p_roi:+.2f}x"
us_exp_spend = US_Q2_TARGET * q2_pct_elapsed
us_pacing    = us_qtd_sp / us_exp_spend if us_exp_spend else 0
us_behind    = us_exp_spend - us_qtd_sp
us_signal    = "🟡" if us_w_roi >= 4.5 else "🔴"

# ── EU metrics ───────────────────────────────────────────────────────────────
print("Fetching EU metrics...")
eu_spend_chart = mcp_chart(22464)
eu_nfa_chart   = mcp_chart(10731)
eu_roi_chart   = mcp_chart(10101)

def get_last_two_rows(chart, key_col):
    rows = (chart or {}).get('data', [])
    rows = [r for r in rows if any(v for v in r.values() if isinstance(v,(int,float)) and v)]
    return rows[-2] if len(rows) >= 2 else {}, rows[-1] if rows else {}

eu_sp_prev_row, eu_sp_week_row = get_last_two_rows(eu_spend_chart, 'week')
eu_nf_prev_row, eu_nf_week_row = get_last_two_rows(eu_nfa_chart, 'week')

def row_total(row): return sum(v for v in row.values() if isinstance(v,(int,float)) and v)

eu_w_spend = row_total(eu_sp_week_row)
eu_p_spend = row_total(eu_sp_prev_row)
eu_w_nfas  = int(row_total(eu_nf_week_row))
eu_p_nfas  = int(row_total(eu_nf_prev_row))

eu_roi_rows = (eu_roi_chart or {}).get('data', [])
eu_w_roi = eu_roi_rows[-2].get('ROI', 0) if len(eu_roi_rows) >= 2 else 0  # exclude most recent
eu_p_roi = eu_roi_rows[-3].get('ROI', 0) if len(eu_roi_rows) >= 3 else 0

eu_spend_wow = f"{(eu_w_spend/eu_p_spend-1)*100:+.0f}%" if eu_p_spend else "n/a"
eu_nfas_wow  = f"{(eu_w_nfas/eu_p_nfas-1)*100:+.0f}%" if eu_p_nfas else "n/a"
eu_roi_wow   = f"{eu_w_roi - eu_p_roi:+.2f}x"

# ── UK metrics ───────────────────────────────────────────────────────────────
print("Fetching UK metrics...")
g_tokens = json.loads(GTOKEN_FILE.read_text())
g = g_tokens['default']
if time.time() > g['expires_at'] - 60:
    r = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id': g['client_id'], 'client_secret': g['client_secret'],
        'refresh_token': g['refresh_token'], 'grant_type': 'refresh_token'}, timeout=15)
    data = r.json()
    g['access_token'] = data['access_token']
    g['expires_at'] = time.time() + data.get('expires_in', 3600)
    g_tokens['default'] = g
    GTOKEN_FILE.write_text(json.dumps(g_tokens, indent=2))
gtok = g['access_token']

sheet_resp = requests.get(
    f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/UK PFM Metrics!A100:G130',
    headers={'Authorization': f'Bearer {gtok}'}, timeout=15)
sheet_rows = sheet_resp.json().get('values', [])

# Find header row for second table (Week Of, Spend, Adjusted NFUs...)
uk_data_rows = []
for row in sheet_rows:
    if row and row[0] not in ('', 'Week Of', 'QTD (Q4)', 'QTD (Q2)', 'YTD ', 'Install to Funding Ratio '):
        try:
            datetime.date.fromisoformat(row[0])
            uk_data_rows.append(row)
        except: pass

uk_week_row = uk_data_rows[-1] if uk_data_rows else []
uk_prev_row = uk_data_rows[-2] if len(uk_data_rows) >= 2 else []

# QTD row
uk_qtd_row = next((r for r in sheet_rows if r and 'QTD (Q2)' in r[0]), [])

def parse_money(s):
    if not s:
        return 0.0
    s = s.strip()
    if '%' in s or not any(c.isdigit() for c in s):
        return 0.0
    try:
        return float(s.replace('$','').replace(',','').replace('K','')) * (1000 if 'K' in s else 1)
    except ValueError:
        return 0.0

uk_w_spend = parse_money(uk_week_row[1]) if len(uk_week_row) > 1 else 0
uk_p_spend = parse_money(uk_prev_row[1]) if len(uk_prev_row) > 1 else 0
uk_w_nfus  = int(uk_week_row[2].replace(',','')) if len(uk_week_row) > 2 else 0
uk_p_nfus  = int(uk_prev_row[2].replace(',','')) if len(uk_prev_row) > 2 else 0
uk_w_roi   = float(uk_week_row[4]) if len(uk_week_row) > 4 else 0
uk_p_roi   = float(uk_prev_row[4]) if len(uk_prev_row) > 4 else 0
uk_w_cac   = parse_money(uk_week_row[5]) if len(uk_week_row) > 5 else 0
uk_p_cac   = parse_money(uk_prev_row[5]) if len(uk_prev_row) > 5 else 0
uk_qtd_sp  = parse_money(uk_qtd_row[1]) if len(uk_qtd_row) > 1 else 0

uk_spend_wow = f"{(uk_w_spend/uk_p_spend-1)*100:+.0f}%" if uk_p_spend else "n/a"
uk_nfus_wow  = f"{(uk_w_nfus/uk_p_nfus-1)*100:+.0f}%" if uk_p_nfus else "n/a"
uk_roi_wow   = f"{uk_w_roi - uk_p_roi:+.1f}x"
uk_cac_wow   = f"{uk_w_cac - uk_p_cac:+.0f}"
uk_exp_spend = UK_Q2_TARGET * q2_pct_elapsed
uk_pacing    = uk_qtd_sp / uk_exp_spend if uk_exp_spend else 0
uk_behind    = uk_exp_spend - uk_qtd_sp
uk_signal    = "🔴" if uk_pacing < 0.9 else "🟡"

# ── Q2 pacing via SQL ────────────────────────────────────────────────────────
print("Fetching Q2 pacing metrics...")
qtd = mcp_sql(f"""SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas,
    SUM(gold_amount) as gold FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{qtd_start}' AND dt_epoch_ms < '{qtd_end_s}' AND cost_amount > 0 LIMIT 1""")
qtd_spend = qtd.get('spend', 0) or 0
qtd_nfas  = int(qtd.get('nfas', 0) or 0)
qtd_gold  = int(qtd.get('gold', 0) or 0)

# ── Compose message ──────────────────────────────────────────────────────────
us_roi_sig = "🟡" if us_w_roi >= 4.5 else "🔴"
eu_sig = "🔴"
uk_pacing_sig = "🔴" if uk_pacing < 0.9 else ("🟡" if uk_pacing < 1.1 else "🟢")

msg = f"""📊 *PFM Weekly Performance Update | {week_label}*
_Performance: {last_monday.strftime('%b %-d')}–{last_sunday.strftime('%-d')} (completed week)  •  Q2 pacing: {Q2_START.strftime('%b %-d')}–{qtd_end.strftime('%b %-d')} (most recent available)_

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🇺🇸 *United States* {us_roi_sig}
• ROI: *{us_w_roi:.2f}x* ({us_roi_wow} WoW)
• Spend: *${us_w_spend/1e6:.1f}M* ({us_spend_wow} WoW)
• NFAs: *{us_w_nfas:,}* ({us_nfas_wow} WoW)
• Q2 Spend Pacing: ${qtd_spend/1e6:.1f}M of ${US_Q2_TARGET/1e6:.1f}M → *{us_pacing*100:.0f}% of pace* {'✅' if us_pacing >= 0.95 else '🔴'} — {'on track' if us_pacing >= 0.95 else f'~${us_behind/1e6:.1f}M behind plan'}

🇪🇺 *Europe* {eu_sig}
• ROI: *{eu_w_roi:.2f}x* ({eu_roi_wow} WoW)
• Spend: *${eu_w_spend:,.0f}* ({eu_spend_wow} WoW)
• NFAs: *{eu_w_nfas:,}* ({eu_nfas_wow} WoW)

🇬🇧 *United Kingdom* {uk_pacing_sig}
• ROI: *{uk_w_roi:.1f}x* ({uk_roi_wow} WoW)
• Spend: *${uk_w_spend:,.0f}* ({uk_spend_wow} WoW)
• NFUs: *{uk_w_nfus:,}* ({uk_nfus_wow} WoW) | CAC: ${uk_w_cac:,.0f} ({uk_cac_wow} WoW)
• Q2 Spend Pacing: ${uk_qtd_sp:,.0f} of ${UK_Q2_TARGET/1e6:.1f}M → *{uk_pacing*100:.0f}% of pace* {uk_pacing_sig}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 *Q2 Goal Pacing Snapshot* _(Day {q2_days_elapsed} of 91, {q2_pct_elapsed:.0%} elapsed)_

• US Spend: ${qtd_spend/1e6:.1f}M / ${US_Q2_TARGET/1e6:.1f}M → *{qtd_spend/US_Q2_TARGET*100:.1f}%* 🔴  |  US NFAs: {qtd_nfas:,} / 202,727 → *{qtd_nfas/202727*100:.1f}%* 🔴
• UK Spend: ${uk_qtd_sp:,.0f} / ${UK_Q2_TARGET:,} → *{uk_qtd_sp/UK_Q2_TARGET*100:.1f}%* 🔴  |  Gold Subs: {qtd_gold:,} / 106,190 → *{qtd_gold/106190*100:.1f}%* 🔴

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📄 *For detailed analysis* (ROI tables, full Q2 pacing, L4W campaign breakdown by channel):
{DOC_URL}"""

# ── Post to Slack ────────────────────────────────────────────────────────────
print("Posting to Slack...")
resp = requests.post('https://slack.com/api/chat.postMessage',
    headers={'Authorization': f'Bearer {SLACK_TOKEN}', 'Content-Type': 'application/json'},
    json={'channel': '#pfm-ai-insights', 'text': msg, 'mrkdwn': True}, timeout=15)
data = resp.json()
if data.get('ok'):
    print(f"✓ Posted successfully. ts={data.get('ts')}")
else:
    print(f"✗ Slack error: {data.get('error')}")


def post_slack_error(msg):
    """Call this on failure so the team knows the report didn't run."""
    env = {}
    for line in Path('~/.pfm-agent.env').expanduser().read_text().splitlines():
        if '=' in line:
            k, v = line.strip().split('=', 1)
            env[k] = v
    requests.post('https://slack.com/api/chat.postMessage',
        headers={'Authorization': f'Bearer {env["SLACK_BOT_TOKEN"]}',
                 'Content-Type': 'application/json'},
        json={'channel': '#pfm-ai-insights',
              'text': f'⚠️ *PFM Weekly Report failed to run* — manual check needed.\nError: {msg}',
              'mrkdwn': True}, timeout=10)

