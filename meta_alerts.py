#!/usr/bin/env python3
"""
Meta Ads Creative / Campaign CPA Alerts
========================================
Queries ads.meta_ads_report_view (Presto) for 28 days of daily spend + purchase,
then surfaces three alert types:

  1. Creative fatigue   — CPA > $120 for 7+ consecutive days
  2. Campaign/ad-set risk — sustained CPA deterioration over 5+ days vs baseline
  3. Campaign improvement — meaningful CPA improvement vs recent 7-day baseline

Entry point: run_meta_alerts(mcp_rows_fn) -> dict
  mcp_rows_fn(sql, database_id, schema, limit) -> List[dict]

Returns:
  {
    "creative_fatigue": [...],
    "campaign_risk":    [...],
    "adset_risk":       [...],
    "improvement":      [...],
    "slack_section":    str,   # concise block for Slack
    "gdoc_section":     str,   # detailed block for Google Doc
  }
"""
from __future__ import annotations

import datetime
from typing import Callable, Dict, List, Optional, Tuple

# ── Superset / Presto config ──────────────────────────────────────────────────
# Update META_DB_ID to match your Superset Presto database_id
# (run: SELECT id, database_name FROM dbs in Superset SQL Lab to confirm)
META_DB_ID  = 5        # Presto — confirmed via get_dataset_info on dataset 454
META_SCHEMA = "ads"
META_TABLE  = "meta_ads_report_view"

# ── Column names in meta_ads_report_view ─────────────────────────────────────
COL_DATE     = "date"
COL_CAMPAIGN = "campaign_name"
COL_ADSET    = "ad_set_name"   # view has both adset_name and ad_set_name; use ad_set_name
COL_AD       = "ad_name"
COL_SPEND    = "spend"
COL_PURCHASE = "purchase"

# ── Alert thresholds ──────────────────────────────────────────────────────────
LOOKBACK_DAYS            = 28
CPF_FATIGUE_THRESHOLD    = 120.0   # CPA > $120 triggers fatigue
FATIGUE_MIN_DAYS         = 7       # consecutive days above threshold required
RISK_DETERIORATION_PCT   = 20.0    # % worse than baseline to flag risk
RISK_MIN_DAYS            = 5       # days of sustained deterioration required
IMPROVEMENT_MIN_PCT      = 15.0    # % CPA improvement vs prior window
IMPROVEMENT_MIN_DAYS     = 3       # days of sustained improvement required
ROLLING_WINDOW           = 3       # smoothing window (days) for CPA series
MIN_SPEND                = 500.0   # 28-day spend floor (noise filter)
MIN_PURCHASES            = 5       # 28-day purchase floor (noise filter)
MAX_FATIGUE_ALERTS       = 10      # cap alerts in output
MAX_RISK_ALERTS          = 8
MAX_IMPROVEMENT_ALERTS   = 5


# =============================================================================
# Data fetching
# =============================================================================

def _build_sql(grain: str, start_date: str, end_date: str) -> str:
    """Build GROUP BY SQL for the requested grain."""
    if grain == "creative":
        group_cols = f"{COL_DATE}, {COL_CAMPAIGN}, {COL_ADSET}, {COL_AD}"
    elif grain == "adset":
        group_cols = f"{COL_DATE}, {COL_CAMPAIGN}, {COL_ADSET}"
    else:  # campaign
        group_cols = f"{COL_DATE}, {COL_CAMPAIGN}"

    return (
        f"SELECT {group_cols}, "
        f"SUM({COL_SPEND}) AS spend, SUM({COL_PURCHASE}) AS purchase "
        f"FROM {META_TABLE} "
        f"WHERE {COL_DATE} >= DATE '{start_date}' AND {COL_DATE} < DATE '{end_date}' "
        f"GROUP BY {group_cols} "
        f"ORDER BY {COL_DATE}"
    )


def fetch_meta_data(mcp_rows: Callable, grain: str) -> List[Dict]:
    today      = datetime.date.today()
    end_date   = today.strftime("%Y-%m-%d")
    start_date = (today - datetime.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    sql        = _build_sql(grain, start_date, end_date)
    try:
        rows = mcp_rows(sql, database_id=META_DB_ID, schema=META_SCHEMA, limit=5000, timeout=300)
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"  [meta_alerts] {grain} query failed: {e}")
        return []


# =============================================================================
# Build per-entity time series
# =============================================================================

# Row type: (date, spend, purchase)
_Row = Tuple[datetime.date, float, float]


def _entity_key(row: dict, grain: str) -> tuple:
    if grain == "creative":
        return (row.get(COL_CAMPAIGN, ""), row.get(COL_ADSET, ""), row.get(COL_AD, ""))
    if grain == "adset":
        return (row.get(COL_CAMPAIGN, ""), row.get(COL_ADSET, ""))
    return (row.get(COL_CAMPAIGN, ""),)


def build_entity_series(rows: List[Dict], grain: str) -> Dict[tuple, List[_Row]]:
    """Return {entity_key: [(date, spend, purchase), ...]} sorted ascending by date."""
    series: Dict[tuple, List[_Row]] = {}
    for row in rows:
        try:
            d = datetime.date.fromisoformat(str(row[COL_DATE])[:10])
        except (KeyError, ValueError):
            continue
        spend    = float(row.get(COL_SPEND,    0) or 0)
        purchase = float(row.get(COL_PURCHASE, 0) or 0)
        key = _entity_key(row, grain)
        series.setdefault(key, []).append((d, spend, purchase))
    for key in series:
        series[key].sort(key=lambda x: x[0])
    return series


def _rolling_cpa(series: List[_Row], window: int = ROLLING_WINDOW) -> List[Tuple[datetime.date, float]]:
    """
    Pooled rolling CPA over `window` days: sum(spend) / sum(purchase).
    Pooling avoids distortion from single low-purchase days.
    Only emits a point when purchase > 0 in the window.
    """
    result = []
    for i in range(len(series)):
        chunk          = series[max(0, i - window + 1): i + 1]
        total_spend    = sum(s for _, s, _ in chunk)
        total_purchase = sum(p for _, _, p in chunk)
        if total_purchase > 0:
            result.append((series[i][0], total_spend / total_purchase))
    return result


def _totals(series: List[_Row]) -> Tuple[float, float]:
    return sum(s for _, s, _ in series), sum(p for _, _, p in series)


# =============================================================================
# Alert 1 — Creative fatigue
# =============================================================================

def detect_creative_fatigue(entity_series: Dict[tuple, List[_Row]]) -> List[Dict]:
    flagged = []

    for key, series in entity_series.items():
        total_spend, total_purchases = _totals(series)
        if total_spend < MIN_SPEND or total_purchases < MIN_PURCHASES:
            continue

        smoothed = _rolling_cpa(series)
        if not smoothed:
            continue

        # Find the longest consecutive run above the threshold
        max_run = current_run = 0
        run_end_idx = -1
        for i, (_, cpa) in enumerate(smoothed):
            if cpa > CPF_FATIGUE_THRESHOLD:
                current_run += 1
                if current_run > max_run:
                    max_run    = current_run
                    run_end_idx = i
            else:
                current_run = 0

        if max_run < FATIGUE_MIN_DAYS:
            continue

        latest_cpa = smoothed[-1][1]

        # Prior CPA = pooled CPA for the 7 days immediately before the streak started
        streak_start_idx = run_end_idx - max_run + 1
        pre_streak_start = max(0, streak_start_idx - 7)
        pre_window = series[pre_streak_start:streak_start_idx]
        pre_spend, pre_purch = _totals(pre_window)
        prior_cpa = (pre_spend / pre_purch) if pre_purch > 0 else None

        recent         = series[-7:] if len(series) >= 7 else series
        recent_spend   = sum(s for _, s, _ in recent)
        recent_purch   = sum(p for _, _, p in recent)

        # Skip if no purchases in the recent window — CPA is not meaningful
        if recent_purch <= 0:
            continue

        flagged.append({
            "grain":            "creative",
            "campaign_name":    key[0],
            "adset_name":       key[1] if len(key) > 1 else "",
            "ad_name":          key[2] if len(key) > 2 else "",
            "latest_cpa":       latest_cpa,
            "prior_cpa":        prior_cpa,
            "streak_days":      max_run,
            "recent_spend":     recent_spend,
            "recent_purchases": recent_purch,
            "commentary":       _fatigue_commentary(series, latest_cpa, prior_cpa),
        })

    flagged.sort(key=lambda x: x["latest_cpa"], reverse=True)
    return flagged[:MAX_FATIGUE_ALERTS]


def _fatigue_commentary(series: List[_Row], latest_cpa: float, prior_cpa: Optional[float]) -> str:
    """Classify likely fatigue driver from spend and purchase trends."""
    if len(series) < 14:
        return "Sustained CPA above threshold; insufficient history to classify driver."

    spend_recent = sum(s for _, s, _ in series[-7:])
    spend_prior  = sum(s for _, s, _ in series[-14:-7])
    purch_recent = sum(p for _, _, p in series[-7:])
    purch_prior  = sum(p for _, _, p in series[-14:-7])

    spend_delta = (spend_recent - spend_prior) / spend_prior * 100 if spend_prior > 0 else 0
    purch_delta = (purch_recent - purch_prior) / purch_prior * 100 if purch_prior > 0 else 0
    cpa_delta   = (latest_cpa - prior_cpa) / prior_cpa * 100 if prior_cpa and prior_cpa > 0 else 0

    if spend_delta > 15 and purch_delta < 5:
        return (
            f"Spend rising (+{spend_delta:.0f}% WoW) but purchases flat — "
            "likely scale fatigue or audience saturation."
        )
    if purch_delta < -20:
        return (
            f"Purchases declining ({purch_delta:.0f}% WoW) — "
            "likely creative wearout; consider rotating asset."
        )
    if cpa_delta > 30:
        return (
            f"CPA up {cpa_delta:.0f}% vs pre-streak — conversion efficiency deteriorating; "
            "broader campaign or auction pressure possible."
        )
    return (
        "CPA above threshold for extended period with no recovery. "
        "Likely audience fatigue or increased bid competition."
    )


# =============================================================================
# Alert 2 — Campaign / ad-set CPA risk (sustained deterioration)
# =============================================================================

def detect_cpa_risk(entity_series: Dict[tuple, List[_Row]], grain: str) -> List[Dict]:
    flagged = []

    for key, series in entity_series.items():
        total_spend, total_purchases = _totals(series)
        if total_spend < MIN_SPEND or total_purchases < MIN_PURCHASES:
            continue

        smoothed = _rolling_cpa(series)
        if len(smoothed) < RISK_MIN_DAYS + 7:
            continue

        # Baseline: pooled CPA over days 8–14 before today
        baseline_window = series[-14:-7] if len(series) >= 14 else series[:max(1, len(series) // 2)]
        b_spend, b_purch = _totals(baseline_window)
        if b_purch <= 0:
            continue
        baseline_cpa = b_spend / b_purch

        # Recent: last RISK_MIN_DAYS days pooled
        recent_window = series[-RISK_MIN_DAYS:]
        r_spend, r_purch = _totals(recent_window)
        if r_purch <= 0:
            continue
        recent_cpa = r_spend / r_purch

        deterioration_pct = (recent_cpa - baseline_cpa) / baseline_cpa * 100
        if deterioration_pct < RISK_DETERIORATION_PCT:
            continue

        # Confirm sustained: require RISK_MIN_DAYS smoothed points all above threshold
        threshold_cpa = baseline_cpa * (1 + RISK_DETERIORATION_PCT / 100)
        days_above = sum(1 for _, cpa in smoothed[-RISK_MIN_DAYS:] if cpa > threshold_cpa)
        if days_above < RISK_MIN_DAYS:
            continue

        entity_name = key[0] if grain == "campaign" else f"{key[0]} / {key[1]}"
        flagged.append({
            "grain":             grain,
            "entity_name":       entity_name,
            "campaign_name":     key[0],
            "adset_name":        key[1] if len(key) > 1 else "",
            "recent_cpa":        recent_cpa,
            "baseline_cpa":      baseline_cpa,
            "deterioration_pct": deterioration_pct,
            "days_above":        days_above,
            "recent_spend":      r_spend,
            "recent_purchases":  r_purch,
            "trend_summary":     _trend_label(smoothed),
        })

    flagged.sort(key=lambda x: x["deterioration_pct"], reverse=True)
    return flagged[:MAX_RISK_ALERTS]


# =============================================================================
# Alert 3 — Campaign CPA improvement
# =============================================================================

def detect_cpa_improvement(entity_series: Dict[tuple, List[_Row]]) -> List[Dict]:
    flagged = []

    for key, series in entity_series.items():
        total_spend, total_purchases = _totals(series)
        if total_spend < MIN_SPEND or total_purchases < MIN_PURCHASES:
            continue
        if len(series) < 14:
            continue

        curr_window  = series[-7:]
        prior_window = series[-14:-7]

        c_spend, c_purch = _totals(curr_window)
        p_spend, p_purch = _totals(prior_window)
        if c_purch <= 0 or p_purch <= 0:
            continue

        # Campaign must still be actively spending — filter out paused/shutdown campaigns
        if c_spend < MIN_SPEND:
            continue

        curr_cpa  = c_spend / c_purch
        prior_cpa = p_spend / p_purch
        improvement_pct = (prior_cpa - curr_cpa) / prior_cpa * 100  # positive = better

        if improvement_pct < IMPROVEMENT_MIN_PCT:
            continue

        # Confirm sustained: count how many of the last 7 smoothed points
        # are already below the improvement threshold
        smoothed        = _rolling_cpa(series)
        threshold_cpa   = prior_cpa * (1 - IMPROVEMENT_MIN_PCT / 100)
        improving_days  = 0
        for _, cpa in reversed(smoothed[-7:]):
            if cpa < threshold_cpa:
                improving_days += 1
            else:
                break  # must be a contiguous run from the most recent day

        if improving_days < IMPROVEMENT_MIN_DAYS:
            continue

        flagged.append({
            "grain":           "campaign",
            "campaign_name":   key[0],
            "curr_cpa":        curr_cpa,
            "prior_cpa":       prior_cpa,
            "improvement_pct": improvement_pct,
            "improving_days":  improving_days,
            "curr_spend":      c_spend,
            "curr_purchases":  c_purch,
            "trend_summary":   _trend_label(smoothed),
        })

    flagged.sort(key=lambda x: x["improvement_pct"], reverse=True)
    return flagged[:MAX_IMPROVEMENT_ALERTS]


def _trend_label(smoothed: List[Tuple[datetime.date, float]]) -> str:
    if len(smoothed) < 3:
        return "insufficient data"
    recent = [cpa for _, cpa in smoothed[-5:]]
    if recent[-1] > recent[0] * 1.10:
        return "rising (worsening)"
    if recent[-1] < recent[0] * 0.90:
        return "declining (improving)"
    return "stable"


# =============================================================================
# Formatting helpers
# =============================================================================

def _fmt_cpa(v: Optional[float]) -> str:
    return f"${v:,.0f}" if v is not None else "N/A"


def _fmt_spend(v: float) -> str:
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


# =============================================================================
# Slack section (concise)
# =============================================================================

def format_slack_section(
    fatigue: List[Dict],
    campaign_risk: List[Dict],
    adset_risk: List[Dict],
    improvement: List[Dict],
) -> str:
    lines = [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        ":meta: *Meta Creative & Campaign Alerts*",
        "",
    ]

    # Creative fatigue
    if fatigue:
        lines.append(f":rotating_light: *Creative Fatigue* — {len(fatigue)} flagged")
        for a in fatigue[:3]:
            name = a["ad_name"] or a["adset_name"] or a["campaign_name"]
            lines.append(
                f"  • *{name}*  |  "
                f"CPA: {_fmt_cpa(a['latest_cpa'])} "
                f"(prior: {_fmt_cpa(a['prior_cpa'])}, {a['streak_days']}d streak)  |  "
                f"Spend: {_fmt_spend(a['recent_spend'])}  |  "
                f"Purchases: {int(a['recent_purchases'])}"
            )
        if len(fatigue) > 3:
            lines.append(f"  _+{len(fatigue) - 3} more — see Google Doc for full list_")
    else:
        lines.append(":white_check_mark: *Creative Fatigue* — no creatives triggered threshold")

    lines.append("")

    # CPA risk
    all_risk = campaign_risk + adset_risk
    if all_risk:
        lines.append(f":warning: *CPA Risk* — {len(all_risk)} campaigns/ad sets flagged")
        for a in all_risk[:3]:
            lines.append(
                f"  • [{a['grain'].upper()}] *{a['entity_name']}*  |  "
                f"CPA: {_fmt_cpa(a['recent_cpa'])} "
                f"(+{a['deterioration_pct']:.0f}% vs baseline, {a['days_above']}d)  |  "
                f"Spend: {_fmt_spend(a['recent_spend'])}"
            )
        if len(all_risk) > 3:
            lines.append(f"  _+{len(all_risk) - 3} more — see Google Doc_")
    else:
        lines.append(":white_check_mark: *CPA Risk* — no campaigns/ad sets flagged")

    lines.append("")

    # Improvement
    if improvement:
        lines.append(f":chart_with_upwards_trend: *Improving Campaigns* — {len(improvement)} flagged")
        for a in improvement[:3]:
            lines.append(
                f"  • *{a['campaign_name']}*  |  "
                f"CPA: {_fmt_cpa(a['curr_cpa'])} "
                f"({a['improvement_pct']:.0f}% better vs prior 7d, {a['improving_days']}d sustained)  |  "
                f"Spend: {_fmt_spend(a['curr_spend'])}"
            )
    else:
        lines.append(":white_circle: *Improving Campaigns* — no meaningful improvement detected")

    return "\n".join(lines)


# =============================================================================
# Google Doc section (detailed)
# =============================================================================

def format_gdoc_section(
    fatigue: List[Dict],
    campaign_risk: List[Dict],
    adset_risk: List[Dict],
    improvement: List[Dict],
) -> str:
    lines: List[str] = [
        "\n\nMETA CREATIVE & CAMPAIGN ALERTS\n",
        "=" * 50 + "\n\n",
    ]

    # ── 1. Creative fatigue ───────────────────────────────────────────────────
    lines.append("1. CREATIVE FATIGUE ALERTS\n")
    lines.append(
        f"   Threshold: CPA > ${CPF_FATIGUE_THRESHOLD:.0f} for {FATIGUE_MIN_DAYS}+ consecutive days.\n\n"
    )
    if fatigue:
        for i, a in enumerate(fatigue, 1):
            name = a["ad_name"] or "(unnamed ad)"
            lines.append(f"   [{i}] {name}\n")
            lines.append(f"       Campaign : {a['campaign_name']}\n")
            lines.append(f"       Ad Set   : {a['adset_name']}\n")
            lines.append(f"       CPA now  : {_fmt_cpa(a['latest_cpa'])}\n")
            lines.append(f"       CPA prior: {_fmt_cpa(a['prior_cpa'])}\n")
            lines.append(f"       Streak   : {a['streak_days']} consecutive days above threshold\n")
            lines.append(f"       Spend (7d)    : {_fmt_spend(a['recent_spend'])}\n")
            lines.append(f"       Purchases (7d): {int(a['recent_purchases'])}\n")
            lines.append(f"       Commentary: {a['commentary']}\n\n")
    else:
        lines.append("   No creatives triggered the fatigue threshold this period.\n\n")

    # ── 2. Campaign / ad-set risk ─────────────────────────────────────────────
    lines.append("2. CAMPAIGN & AD SET CPA RISK ALERTS\n")
    lines.append(
        f"   Trigger: CPA >{RISK_DETERIORATION_PCT:.0f}% worse than prior-week baseline "
        f"for {RISK_MIN_DAYS}+ days.\n\n"
    )
    all_risk = campaign_risk + adset_risk
    if all_risk:
        for i, a in enumerate(all_risk, 1):
            lines.append(f"   [{i}] [{a['grain'].upper()}] {a['entity_name']}\n")
            lines.append(f"       CPA (recent {RISK_MIN_DAYS}d) : {_fmt_cpa(a['recent_cpa'])}\n")
            lines.append(f"       CPA (baseline)     : {_fmt_cpa(a['baseline_cpa'])}\n")
            lines.append(f"       Deterioration      : +{a['deterioration_pct']:.0f}% vs baseline\n")
            lines.append(f"       Days above threshold: {a['days_above']}\n")
            lines.append(f"       Spend ({RISK_MIN_DAYS}d)  : {_fmt_spend(a['recent_spend'])}\n")
            lines.append(f"       Purchases ({RISK_MIN_DAYS}d): {int(a['recent_purchases'])}\n")
            lines.append(f"       Trend : {a['trend_summary']}\n\n")
    else:
        lines.append("   No campaigns or ad sets flagged for CPA risk.\n\n")

    # ── 3. Improvement ────────────────────────────────────────────────────────
    lines.append("3. CAMPAIGN CPA IMPROVEMENT\n")
    lines.append(
        f"   Trigger: CPA improved >{IMPROVEMENT_MIN_PCT:.0f}% vs prior 7-day baseline, "
        f"sustained {IMPROVEMENT_MIN_DAYS}+ days.\n\n"
    )
    if improvement:
        for i, a in enumerate(improvement, 1):
            lines.append(f"   [{i}] {a['campaign_name']}\n")
            lines.append(f"       CPA (current 7d) : {_fmt_cpa(a['curr_cpa'])}\n")
            lines.append(f"       CPA (prior 7d)   : {_fmt_cpa(a['prior_cpa'])}\n")
            lines.append(f"       Improvement      : {a['improvement_pct']:.0f}% better\n")
            lines.append(f"       Sustained for    : {a['improving_days']} days\n")
            lines.append(f"       Spend (7d)       : {_fmt_spend(a['curr_spend'])}\n")
            lines.append(f"       Purchases (7d)   : {int(a['curr_purchases'])}\n")
            lines.append(f"       Trend: {a['trend_summary']}\n\n")
    else:
        lines.append("   No campaigns showed meaningful sustained improvement this period.\n\n")

    return "".join(lines)


# =============================================================================
# Main entry point
# =============================================================================

def run_meta_alerts(mcp_rows: Callable) -> Dict:
    """
    mcp_rows(sql, database_id, schema, limit) -> List[dict]

    Fetches 28 days of Meta data at three grains, runs all alert logic,
    and returns formatted Slack + Google Doc sections alongside raw alert lists.
    """
    print("  [meta_alerts] Fetching creative-level data (28d)...")
    creative_rows = fetch_meta_data(mcp_rows, "creative")
    print(f"    -> {len(creative_rows)} rows")

    print("  [meta_alerts] Fetching ad-set-level data (28d)...")
    adset_rows = fetch_meta_data(mcp_rows, "adset")
    print(f"    -> {len(adset_rows)} rows")

    print("  [meta_alerts] Fetching campaign-level data (28d)...")
    campaign_rows = fetch_meta_data(mcp_rows, "campaign")
    print(f"    -> {len(campaign_rows)} rows")

    creative_series = build_entity_series(creative_rows,  "creative")
    adset_series    = build_entity_series(adset_rows,     "adset")
    campaign_series = build_entity_series(campaign_rows,  "campaign")

    print("  [meta_alerts] Detecting creative fatigue...")
    fatigue = detect_creative_fatigue(creative_series)
    print(f"    -> {len(fatigue)} flagged")

    print("  [meta_alerts] Detecting campaign CPA risk...")
    campaign_risk = detect_cpa_risk(campaign_series, "campaign")
    print(f"    -> {len(campaign_risk)} flagged")

    print("  [meta_alerts] Detecting ad-set CPA risk...")
    adset_risk = detect_cpa_risk(adset_series, "adset")
    print(f"    -> {len(adset_risk)} flagged")

    print("  [meta_alerts] Detecting campaign improvements...")
    improvement = detect_cpa_improvement(campaign_series)
    print(f"    -> {len(improvement)} flagged")

    return {
        "creative_fatigue": fatigue,
        "campaign_risk":    campaign_risk,
        "adset_risk":       adset_risk,
        "improvement":      improvement,
        "slack_section":    format_slack_section(fatigue, campaign_risk, adset_risk, improvement),
        "gdoc_section":     format_gdoc_section(fatigue, campaign_risk, adset_risk, improvement),
    }
