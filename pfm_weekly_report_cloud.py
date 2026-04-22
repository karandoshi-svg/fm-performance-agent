#!/usr/bin/env python3
"""
PFM Weekly Performance Report — cloud version (GitHub Actions).
Reads all credentials from environment variables instead of local token files.
Posts every Monday 9 AM PST to #pfm-ai-insights.
"""
import json, os, requests, time, datetime

# ── Config ──────────────────────────────────────────────────────────────────
SHEET_ID      = '1nqqEFXnQOLrc77hoQ85ORPXEDrPK9kOVpjsMKMsLtiU'
DOC_URL       = 'https://docs.google.com/document/d/117NWFxfLP50l6RGIBRc2V6rE7WL9VeQJAE25wz5nHoI'
SUPERSET_BASE = 'https://superset.robinhood.com/mcp'
UK_Q2_TARGET  = 1_200_000
US_Q2_TARGET  = 46_200_000
Q2_START      = datetime.date(2026, 4, 1)

# ── Credentials from env vars ────────────────────────────────────────────────
SLACK_TOKEN    = os.environ['SLACK_BOT_TOKEN'].strip()
G_ACCESS_TOKEN = os.environ.get('GOOGLE_ACCESS_TOKEN', '').strip()
G_REFRESH_TOKEN     = os.environ['GOOGLE_REFRESH_TOKEN'].strip()
G_CLIENT_ID         = os.environ['GOOGLE_CLIENT_ID'].strip()
G_CLIENT_SECRET     = os.environ['GOOGLE_CLIENT_SECRET'].strip()


# ── Date windows ─────────────────────────────────────────────────────────────
PT = datetime.timezone(datetime.timedelta(hours=-7))  # PDT (UTC-7 in summer); adjust to -8 in winter
today        = datetime.datetime.now(tz=PT).date()
days_since_monday = today.weekday()  # Mon=0
last_sunday  = today - datetime.timedelta(days=days_since_monday + 1)
last_monday  = last_sunday - datetime.timedelta(days=6)
week_label   = f"{last_monday.strftime('%b %-d')}–{last_sunday.strftime('%-d, %Y')}"
qtd_end      = today - datetime.timedelta(days=1)
q2_days_elapsed = (today - Q2_START).days
q2_pct_elapsed  = q2_days_elapsed / 91

rpt_start = last_monday.strftime('%Y-%m-%d')
rpt_end   = (last_sunday + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
qtd_start = Q2_START.strftime('%Y-%m-%d')
qtd_end_s = today.strftime('%Y-%m-%d')

print(f"Reporting week: {rpt_start} to {rpt_end}")
print(f"QTD window: {qtd_start} to {qtd_end_s} (day {q2_days_elapsed} of 91, {q2_pct_elapsed:.1%})")


# ── Superset auth (refresh token → access token) ─────────────────────────────
# Access token is kept fresh by sync_tokens_to_github.py running on Mac at 8:50 AM
# (Superset /token endpoint blocks external IPs — refresh only works on local network)
SUPA_TOKEN = os.environ['SUPERSET_ACCESS_TOKEN'].strip()
print(f"  Using SUPERSET_ACCESS_TOKEN (len={len(SUPA_TOKEN)})")
SUPA_HDRS  = {
    'Authorization': f'Bearer {SUPA_TOKEN}',
    'Content-Type':  'application/json',
    'Accept':        'application/json, text/event-stream',
}


def mcp_sql(sql: str) -> dict:
    resp = requests.post(SUPERSET_BASE, headers=SUPA_HDRS, timeout=90,
        json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
              'params': {'name': 'execute_sql',
                         'arguments': {'request': {'sql': sql, 'database_id': 2,
                                                   'schema': 'default', 'limit': 1}}}})
    for line in resp.text.splitlines():
        if line.startswith('data:'):
            d = line[5:].strip()
            if d and d != '[DONE]':
                p = json.loads(d)
                if 'result' in p:
                    return p['result'].get('structuredContent', {}).get('rows', [{}])[0]
    return {}


def mcp_chart(chart_id: int):
    resp = requests.post(SUPERSET_BASE, headers=SUPA_HDRS, timeout=90,
        json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
              'params': {'name': 'get_chart_data',
                         'arguments': {'request': {'identifier': chart_id}}}})
    for line in resp.text.splitlines():
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


# ── Google OAuth (refresh in-memory) ─────────────────────────────────────────
def get_google_token() -> str:
    print("  Refreshing Google token...")
    resp = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id':     G_CLIENT_ID,
        'client_secret': G_CLIENT_SECRET,
        'refresh_token': G_REFRESH_TOKEN,
        'grant_type':    'refresh_token',
    }, timeout=15)
    if resp.status_code != 200:
        print(f"  Google token refresh failed ({resp.status_code}), using GOOGLE_ACCESS_TOKEN env var")
        return G_ACCESS_TOKEN
    data = resp.json()
    print(f"  Google token refreshed (expires in {data.get('expires_in', '?')}s)")
    return data['access_token']

GTOK = get_google_token()


# ── US metrics ────────────────────────────────────────────────────────────────
print("Fetching US weekly metrics...")
us_week = mcp_sql(f"""
    SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas,
           SUM(acq_ltv_amount)/SUM(cost_amount) as roi
    FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{rpt_start}' AND dt_epoch_ms < '{rpt_end}'
      AND cost_amount > 0 LIMIT 1
""")

us_prev = mcp_sql(f"""
    SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas,
           SUM(acq_ltv_amount)/SUM(cost_amount) as roi
    FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{(last_monday - datetime.timedelta(days=7)).strftime('%Y-%m-%d')}'
      AND dt_epoch_ms < '{rpt_start}' AND cost_amount > 0 LIMIT 1
""")

us_qtd = mcp_sql(f"""
    SELECT SUM(cost_amount) as spend, SUM(nfa_amount) as nfas, SUM(gold_amount) as gold
    FROM pfm_2025_report
    WHERE dt_epoch_ms >= '{qtd_start}' AND dt_epoch_ms < '{qtd_end_s}'
      AND cost_amount > 0 LIMIT 1
""")

us_w_spend = us_week.get('spend', 0) or 0
us_w_nfas  = int(us_week.get('nfas',  0) or 0)
us_w_roi   = us_week.get('roi',   0) or 0
us_p_spend = us_prev.get('spend', 0) or 0
us_p_nfas  = int(us_prev.get('nfas',  0) or 0)
us_p_roi   = us_prev.get('roi',   0) or 0
us_qtd_sp  = us_qtd.get('spend',  0) or 0
us_qtd_nfas = int(us_qtd.get('nfas', 0) or 0)
us_qtd_gold = int(us_qtd.get('gold', 0) or 0)

us_spend_wow = f"{(us_w_spend/us_p_spend-1)*100:+.0f}%" if us_p_spend else "n/a"
us_nfas_wow  = f"{(us_w_nfas/us_p_nfas-1)*100:+.0f}%"  if us_p_nfas  else "n/a"
us_roi_wow   = f"{us_w_roi - us_p_roi:+.2f}x"
us_exp_spend = US_Q2_TARGET * q2_pct_elapsed
us_pacing    = us_qtd_sp / us_exp_spend if us_exp_spend else 0
us_behind    = us_exp_spend - us_qtd_sp
us_roi_sig   = "🟡" if us_w_roi >= 4.5 else "🔴"
us_pace_sig  = "✅" if us_pacing >= 0.95 else ("🟡" if us_pacing >= 0.85 else "🔴")

print(f"  US spend=${us_w_spend/1e6:.2f}M  NFAs={us_w_nfas:,}  ROI={us_w_roi:.2f}x")


# ── EU metrics ────────────────────────────────────────────────────────────────
print("Fetching EU metrics...")
eu_spend_chart = mcp_chart(22464)
eu_nfa_chart   = mcp_chart(10731)
eu_roi_chart   = mcp_chart(10101)

def get_last_two_rows(chart):
    rows = (chart or {}).get('data', [])
    rows = [r for r in rows if any(isinstance(v, (int, float)) and v for v in r.values())]
    prev = rows[-2] if len(rows) >= 2 else {}
    curr = rows[-1] if rows else {}
    return prev, curr

def row_total(row):
    return sum(v for v in row.values() if isinstance(v, (int, float)) and v)

eu_sp_prev, eu_sp_curr = get_last_two_rows(eu_spend_chart)
eu_nf_prev, eu_nf_curr = get_last_two_rows(eu_nfa_chart)

eu_w_spend = row_total(eu_sp_curr)
eu_p_spend = row_total(eu_sp_prev)
eu_w_nfas  = int(row_total(eu_nf_curr))
eu_p_nfas  = int(row_total(eu_nf_prev))

eu_roi_rows = (eu_roi_chart or {}).get('data', [])
eu_w_roi = eu_roi_rows[-2].get('ROI', 0) if len(eu_roi_rows) >= 2 else 0
eu_p_roi = eu_roi_rows[-3].get('ROI', 0) if len(eu_roi_rows) >= 3 else 0

eu_spend_wow = f"{(eu_w_spend/eu_p_spend-1)*100:+.0f}%" if eu_p_spend else "n/a"
eu_nfas_wow  = f"{(eu_w_nfas/eu_p_nfas-1)*100:+.0f}%"  if eu_p_nfas  else "n/a"
eu_roi_wow   = f"{eu_w_roi - eu_p_roi:+.2f}x"
eu_sig       = "🟡" if eu_w_roi >= 2.5 else "🔴"

print(f"  EU spend=${eu_w_spend:,.0f}  NFAs={eu_w_nfas:,}  ROI={eu_w_roi:.2f}x")


# ── UK metrics ────────────────────────────────────────────────────────────────
print("Fetching UK metrics...")
sheet_resp = requests.get(
    f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/UK PFM Metrics!A100:G130',
    headers={'Authorization': f'Bearer {GTOK}'}, timeout=15)
sheet_rows = sheet_resp.json().get('values', [])

uk_data_rows = []
for row in sheet_rows:
    if row and row[0] not in ('', 'Week Of', 'QTD (Q4)', 'QTD (Q2)', 'YTD ', 'Install to Funding Ratio '):
        try:
            datetime.date.fromisoformat(row[0])
            # Only weekly performance rows have a $ spend in col 1 — skip ratio/pct tables
            if len(row) > 1 and str(row[1]).startswith('$'):
                uk_data_rows.append(row)
        except Exception:
            pass

uk_week_row = uk_data_rows[-1] if uk_data_rows else []
uk_prev_row = uk_data_rows[-2] if len(uk_data_rows) >= 2 else []
uk_qtd_row  = next((r for r in sheet_rows if r and 'QTD (Q2)' in r[0]), [])

def parse_money(s: str) -> float:
    if not s:
        return 0.0
    s = s.strip()
    if '%' in s or not any(c.isdigit() for c in s):
        return 0.0
    mult = 1000 if 'K' in s else 1
    try:
        return float(s.replace('$', '').replace(',', '').replace('K', '')) * mult
    except ValueError:
        return 0.0

def parse_int(s: str) -> int:
    if not s:
        return 0
    s = s.strip()
    if '%' in s or not any(c.isdigit() for c in s):
        return 0
    try:
        return int(s.replace(',', '').split('.')[0])
    except ValueError:
        return 0

uk_w_spend = parse_money(uk_week_row[1]) if len(uk_week_row) > 1 else 0
uk_p_spend = parse_money(uk_prev_row[1]) if len(uk_prev_row) > 1 else 0
uk_w_nfus  = parse_int(uk_week_row[2]) if len(uk_week_row) > 2 else 0
uk_p_nfus  = parse_int(uk_prev_row[2]) if len(uk_prev_row) > 2 else 0
uk_w_roi   = float(uk_week_row[4]) if len(uk_week_row) > 4 else 0
uk_p_roi   = float(uk_prev_row[4]) if len(uk_prev_row) > 4 else 0
uk_w_cac   = parse_money(uk_week_row[5]) if len(uk_week_row) > 5 else 0
uk_p_cac   = parse_money(uk_prev_row[5]) if len(uk_prev_row) > 5 else 0
uk_qtd_sp  = parse_money(uk_qtd_row[1]) if len(uk_qtd_row) > 1 else 0

uk_spend_wow  = f"{(uk_w_spend/uk_p_spend-1)*100:+.0f}%" if uk_p_spend else "n/a"
uk_nfus_wow   = f"{(uk_w_nfus/uk_p_nfus-1)*100:+.0f}%"  if uk_p_nfus  else "n/a"
uk_roi_wow    = f"{uk_w_roi - uk_p_roi:+.1f}x"
uk_cac_wow    = f"{uk_w_cac - uk_p_cac:+.0f}"
uk_exp_spend  = UK_Q2_TARGET * q2_pct_elapsed
uk_pacing     = uk_qtd_sp / uk_exp_spend if uk_exp_spend else 0
uk_behind     = uk_exp_spend - uk_qtd_sp
uk_pacing_sig = "✅" if uk_pacing >= 0.95 else ("🟡" if uk_pacing >= 0.85 else "🔴")

print(f"  UK spend=${uk_w_spend:,.0f}  NFUs={uk_w_nfus:,}  ROI={uk_w_roi:.1f}x")


# ── Check if Superset data is available (it requires Robinhood internal network) ──
superset_unavailable = (us_w_spend == 0 and eu_w_spend == 0 and us_w_nfas == 0)
if superset_unavailable:
    print("WARNING: Superset returned all-zero data — likely not reachable from this network.")

# ── Compose Slack message ─────────────────────────────────────────────────────
US_NFA_TARGET  = 202_727
GOLD_TARGET    = 106_190

nfa_pacing     = us_qtd_nfas / US_NFA_TARGET if US_NFA_TARGET else 0
gold_pacing    = us_qtd_gold / GOLD_TARGET    if GOLD_TARGET   else 0
nfa_pace_sig   = "✅" if nfa_pacing >= 0.95 else ("🟡" if nfa_pacing >= 0.85 else "🔴")
gold_pace_sig  = "✅" if gold_pacing >= 0.95 else ("🟡" if gold_pacing >= 0.85 else "🔴")
us_spend_pace_sig = "✅" if us_pacing >= 0.95 else ("🟡" if us_pacing >= 0.85 else "🔴")

if superset_unavailable:
    us_section_sig  = "⚠️"
    us_section_body = "_Superset data unavailable - run from Mac for full US/EU metrics_"
    eu_section_sig  = "⚠️"
    eu_section_body = "_Superset data unavailable_"
else:
    us_section_sig  = us_roi_sig
    us_section_body = (
        f"• ROI: *{us_w_roi:.2f}x* ({us_roi_wow} WoW)\n"
        f"• Spend: *${us_w_spend/1e6:.1f}M* ({us_spend_wow} WoW)\n"
        f"• NFAs: *{us_w_nfas:,}* ({us_nfas_wow} WoW)\n"
        f"• Q2 Spend Pacing: ${us_qtd_sp/1e6:.1f}M / ${US_Q2_TARGET/1e6:.0f}M"
        f" -> *{us_pacing*100:.0f}% of pace* {us_spend_pace_sig}"
    )
    eu_section_sig  = eu_sig
    eu_section_body = (
        f"• ROI: *{eu_w_roi:.2f}x* ({eu_roi_wow} WoW)\n"
        f"• Spend: *${eu_w_spend:,.0f}* ({eu_spend_wow} WoW)\n"
        f"• NFAs: *{eu_w_nfas:,}* ({eu_nfas_wow} WoW)"
    )

msg = f"""*PFM Weekly Performance Update | {week_label}*
_Performance: {last_monday.strftime('%b %-d')}–{last_sunday.strftime('%-d')} (completed week)  •  Q2 pacing: {Q2_START.strftime('%b %-d')}–{qtd_end.strftime('%b %-d')} (most recent available)_

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

:flag-us: *United States* {us_section_sig}
{us_section_body}

:flag-eu: *Europe* {eu_section_sig}
{eu_section_body}

:flag-gb: *United Kingdom* {uk_pacing_sig}
• ROI: *{uk_w_roi:.1f}x* ({uk_roi_wow} WoW)
• Spend: *${uk_w_spend:,.0f}* ({uk_spend_wow} WoW)
• NFUs: *{uk_w_nfus:,}* ({uk_nfus_wow} WoW)  |  CAC: ${uk_w_cac:,.0f} ({uk_cac_wow} WoW)
• Q2 Spend Pacing: ${uk_qtd_sp:,.0f} / ${UK_Q2_TARGET/1e6:.1f}M → *{uk_pacing*100:.0f}% of pace* {uk_pacing_sig}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

:dart: *Q2 Goal Pacing Snapshot* _(Day {q2_days_elapsed} of 91, {q2_pct_elapsed:.0%} elapsed)_

• US Spend:  ${us_qtd_sp/1e6:.1f}M  /  ${US_Q2_TARGET/1e6:.0f}M  →  *{us_qtd_sp/US_Q2_TARGET*100:.1f}%* {us_spend_pace_sig}
• US NFAs:   {us_qtd_nfas:,}  /  {US_NFA_TARGET:,}  →  *{nfa_pacing*100:.1f}%* {nfa_pace_sig}
• Gold Subs: {us_qtd_gold:,}  /  {GOLD_TARGET:,}  →  *{gold_pacing*100:.1f}%* {gold_pace_sig}
• UK Spend:  ${uk_qtd_sp:,.0f}  /  ${UK_Q2_TARGET/1e6:.1f}M  →  *{uk_qtd_sp/UK_Q2_TARGET*100:.1f}%* {uk_pacing_sig}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

:page_facing_up: *For detailed analysis* (ROI tables, full Q2 pacing, L4W campaign breakdown by channel):
{DOC_URL}"""

# ── Post to Slack ─────────────────────────────────────────────────────────────
print("Posting to Slack...")
resp = requests.post(
    'https://slack.com/api/chat.postMessage',
    headers={'Authorization': f'Bearer {SLACK_TOKEN}', 'Content-Type': 'application/json'},
    json={'channel': '#pfm-ai-insights', 'text': msg, 'mrkdwn': True},
    timeout=15,
)
data = resp.json()
if data.get('ok'):
    print(f"Posted successfully. ts={data.get('ts')}")
else:
    print(f"Slack error: {data.get('error')}")
    raise SystemExit(f"Slack post failed: {data.get('error')}")
