"""
process_plant_data.py

Reads the downloaded raw xlsx and extracts PV Yield (kWh).
Produces data/processed.json with today's total, hourly breakdown,
system status, and fires Telegram alerts every run when triggered.

Two independent alert checks run every hour:

  CHECK 1 — Hourly pace:
    Is cumulative generation keeping up with the sine-bell solar curve?
    Fires if actual < 30% of what the curve expects by this hour.
    Example: curve says 400 kWh by 13:00, actual is 80 kWh → alert.

  CHECK 2 — Projected daily total:
    If the system keeps generating at its current pace, will it finish
    the day below the known worst/low day on record?
    Fires if projected end-of-day < DAILY_LOW_KWH.
    Example: at 13:00 (50% through day) actual is 30 kWh → projected
    60 kWh end-of-day, below 304 kWh low → alert.

Both checks alert every run while triggered (no "change only" suppression),
so you get hourly updates on a problem until it resolves.

This script is IDENTICAL across all sites.
The only values that change per site:
  - DAILY_EXPECTED_KWH and DAILY_LOW_KWH (edit directly below)
  - PV_COLUMN_INDEX if the xlsx column differs
  - GitHub secrets: PLANT_NAME, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import json
import math
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# ✏️  SITE THRESHOLDS — only these two values change between sites
#     Edit directly here. Do NOT set as GitHub secrets.
# =============================================================================
DAILY_EXPECTED_KWH = 1400.0   # Average good day for this site (kWh)
DAILY_LOW_KWH      = 304.0    # Known worst/low production day (kWh)

# PV Yield column fallback — 0-based (A=0, B=1, C=2, D=3, E=4, F=5...)
# Auto-detected from header name first; only used as fallback.
PV_COLUMN_INDEX    = 4        # default = column E

# =============================================================================
# 🔒 SECRETS — set in GitHub repo Settings → Secrets → Actions
# =============================================================================
PLANT_NAME         = os.environ.get("PLANT_NAME", "Solar Plant")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# =============================================================================
# FIXED CONFIG — same for all sites
# =============================================================================
PACE_THRESHOLD_PCT = 0.30    # check 1: alert if actual < 30% of curve-expected
OFFLINE_THRESHOLD  = 0.01    # kWh — treat as offline below this

_HERE       = Path(__file__).parent
RAW_FILE    = _HERE / "data" / "raw_report.xlsx"
OUTPUT_FILE = _HERE / "data" / "processed.json"
STATE_FILE  = _HERE / "data" / "alert_state.json"

SAST = timezone(timedelta(hours=2))


# =============================================================================
# Solar curve — Johannesburg seasonal sunrise/sunset + sine bell
# =============================================================================

def solar_window(month: int) -> tuple:
    """
    Seasonal sunrise/sunset for Johannesburg (26°S).
    Summer (Dec): ~05:15 rise / 18:45 set | Winter (Jun): ~06:45 / 17:15
    """
    mid_day   = (month - 1) * 30 + 15
    amplitude = 0.75
    angle     = 2 * math.pi * (mid_day - 355) / 365
    shift     = amplitude * math.cos(angle)
    return 6.0 - shift, 18.0 + shift


def solar_curve_fraction(hour: int, month: int) -> float:
    """
    Fraction of the day's total PV energy expected by end of `hour`.
    Sine-bell curve — low at sunrise/sunset, peaks at solar noon.
    Returns 0.0–1.0.
    """
    sunrise, sunset = solar_window(month)
    solar_day = sunset - sunrise
    if solar_day <= 0:
        return 0.0
    elapsed = (hour + 1) - sunrise
    if elapsed <= 0:
        return 0.0
    if elapsed >= solar_day:
        return 1.0
    return (1 - math.cos(math.pi * elapsed / solar_day)) / 2


# =============================================================================
# Parse the xlsx
# =============================================================================

def parse_report(filepath: Path) -> dict:
    df      = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = [str(h).strip() if not pd.isna(h) else "" for h in df.iloc[1].tolist()]

    pv_col = next(
        (i for i, h in enumerate(headers) if "PV Yield" in h),
        PV_COLUMN_INDEX,
    )
    print(f"  ℹ️  PV Yield column: index {pv_col} — '{headers[pv_col]}'")

    hourly      = [0.0] * 24
    total       = 0.0
    last_hour   = 0
    row_count   = 0
    report_date = None

    for idx in range(2, len(df)):
        row    = df.iloc[idx]
        ts_raw = row.iloc[0]
        if pd.isna(ts_raw):
            continue
        try:
            ts   = pd.Timestamp(ts_raw)
            hour = ts.hour
            if report_date is None:
                report_date = ts.strftime("%Y-%m-%d")
        except Exception:
            continue

        pv_val       = float(row.iloc[pv_col]) if not pd.isna(row.iloc[pv_col]) else 0.0
        hourly[hour] = round(pv_val, 4)
        total       += pv_val
        last_hour    = hour
        row_count   += 1

    return {
        "date":       report_date or datetime.now(SAST).strftime("%Y-%m-%d"),
        "total_kwh":  round(total, 3),
        "hourly":     hourly,
        "last_hour":  last_hour,
        "row_count":  row_count,
    }


# =============================================================================
# Status checks
# =============================================================================

def determine_status(data: dict, month: int) -> tuple:
    """
    Returns (status, alerts, debug).

    status  — 'ok', 'low', or 'offline'
    alerts  — {
        'offline':   bool,
        'pace_low':  bool,   # check 1: behind the hourly curve
        'total_low': bool,   # check 2: projected day below known low
      }
    debug   — all the numbers, written to processed.json for transparency
    """
    total           = data["total_kwh"]
    hour            = data["last_hour"]
    sunrise, sunset = solar_window(month)
    alerts          = {"offline": False, "pace_low": False, "total_low": False}

    # Offline
    if total < OFFLINE_THRESHOLD:
        alerts["offline"] = True
        return "offline", alerts, {
            "reason": "no generation detected",
            "curve_fraction": 0.0, "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    curve_frac = solar_curve_fraction(hour, month)

    # Too early — less than 10% of day's energy expected yet
    if curve_frac < 0.10:
        return "ok", alerts, {
            "reason": "too early to assess",
            "curve_fraction": round(curve_frac, 3),
            "expected_by_now": round(DAILY_EXPECTED_KWH * curve_frac, 1),
            "pace_trigger": 0.0, "projected_total": 0.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    expected_by_now  = DAILY_EXPECTED_KWH * curve_frac
    pace_trigger     = expected_by_now * PACE_THRESHOLD_PCT
    projected_total  = total / curve_frac   # if pace stays constant all day

    # Check 1: hourly pace
    if total < pace_trigger:
        alerts["pace_low"] = True

    # Check 2: projected daily total below known low day
    if projected_total < DAILY_LOW_KWH:
        alerts["total_low"] = True

    debug = {
        "curve_fraction":  round(curve_frac, 3),
        "expected_by_now": round(expected_by_now, 1),
        "actual_kwh":      round(total, 2),
        "pace_trigger":    round(pace_trigger, 1),
        "projected_total": round(projected_total, 1),
        "low_day_kwh":     DAILY_LOW_KWH,
        "sunrise":         round(sunrise, 2),
        "sunset":          round(sunset, 2),
        "checks": {
            "pace_low":  alerts["pace_low"],
            "total_low": alerts["total_low"],
        },
    }

    status = "low" if (alerts["pace_low"] or alerts["total_low"]) else "ok"
    return status, alerts, debug


# =============================================================================
# Telegram
# =============================================================================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠️  Telegram not configured — skipping")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            print("  ✅ Telegram alert sent")
            return True
        print(f"  ❌ Telegram error {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"  ❌ Telegram request failed: {e}")
        return False


def send_alerts(status: str, alerts: dict, data: dict, debug: dict):
    """
    Fires Telegram messages every run when a check is triggered.
    No suppression — you get an alert each hourly run while the problem persists.
    Recovery message sent once when status returns to ok.
    """
    now_str          = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    total            = data["total_kwh"]
    hour             = data["last_hour"]
    expected_by_now  = debug.get("expected_by_now", 0)
    projected_total  = debug.get("projected_total", 0)

    # Load previous status for recovery detection
    prev_status = "ok"
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    if alerts["offline"]:
        send_telegram(
            f"🔴 <b>{PLANT_NAME} — OFFLINE</b>\n"
            f"No generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"🕐 {now_str}"
        )

    else:
        # Check 1: pace alert
        if alerts["pace_low"]:
            send_telegram(
                f"🟡 <b>{PLANT_NAME} — LOW PACE</b>\n"
                f"Generation is well behind the expected curve.\n"
                f"Actual so far:    <b>{total:.1f} kWh</b>\n"
                f"Expected by now:  <b>~{expected_by_now:.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )

        # Check 2: projected total alert
        if alerts["total_low"]:
            send_telegram(
                f"🟠 <b>{PLANT_NAME} — POOR DAY PROJECTED</b>\n"
                f"At current pace, today will finish below the known low day.\n"
                f"Actual so far:      <b>{total:.1f} kWh</b>\n"
                f"Projected end-day:  <b>~{projected_total:.0f} kWh</b>\n"
                f"Known low day:      <b>{DAILY_LOW_KWH:.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )

        # Recovery: was bad, now ok
        if status == "ok" and prev_status in ("low", "offline"):
            send_telegram(
                f"✅ <b>{PLANT_NAME} — RECOVERED</b>\n"
                f"System is back within normal range.\n"
                f"Total today: <b>{total:.1f} kWh</b> (as of {hour:02d}:00)\n"
                f"🕐 {now_str}"
            )

        # All clear — no alerts (just log, no Telegram)
        if not alerts["pace_low"] and not alerts["total_low"] and status == "ok":
            print(f"  ✅ All checks passed — no alert needed")

    # Save state
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing: {PLANT_NAME}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not RAW_FILE.exists():
        print(f"❌ Raw file not found: {RAW_FILE}")
        sys.exit(1)

    now             = datetime.now(SAST)
    month           = now.month
    sunrise, sunset = solar_window(month)

    print(f"📥 Reading: {RAW_FILE}")
    data                   = parse_report(RAW_FILE)
    status, alerts, debug  = determine_status(data, month)

    print(f"  📅 Date:               {data['date']}")
    print(f"  ⚡ PV Yield:           {data['total_kwh']:.3f} kWh")
    print(f"  🕐 Last hour:          {data['last_hour']:02d}:00")
    print(f"  🌅 Solar window:       {sunrise:.1f}h – {sunset:.1f}h  (month {month})")
    print(f"  📈 Curve fraction:     {debug.get('curve_fraction', 0.0):.1%}")
    print(f"  🎯 Expected by now:    {debug.get('expected_by_now', 0.0):.1f} kWh")
    print(f"  📉 Pace trigger:       {debug.get('pace_trigger', 0.0):.1f} kWh  → pace_low={alerts['pace_low']}")
    print(f"  📊 Projected total:    {debug.get('projected_total', 0.0):.1f} kWh  → total_low={alerts['total_low']}  (low day: {DAILY_LOW_KWH} kWh)")
    print(f"  🚦 Status:             {status.upper()}")

    send_alerts(status, alerts, data, debug)

    output = {
        "plant":        PLANT_NAME,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "date":         data["date"],
        "total_kwh":    data["total_kwh"],
        "last_hour":    data["last_hour"],
        "status":       status,
        "alerts":       alerts,
        "thresholds": {
            "expected_daily_kwh": DAILY_EXPECTED_KWH,
            "low_day_kwh":        DAILY_LOW_KWH,
            "pace_threshold_pct": PACE_THRESHOLD_PCT,
            "solar_window": {
                "sunrise": round(sunrise, 2),
                "sunset":  round(sunset,  2),
            },
        },
        "debug":     debug,
        "hourly_pv": data["hourly"],
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ Saved: {OUTPUT_FILE}")
    print("✅ Done!")


if __name__ == "__main__":
    main()
