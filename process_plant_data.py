"""
process_plant_data.py

Reads the downloaded raw xlsx and extracts PV Yield (kWh).
Produces data/processed.json with:
  - today's total PV yield
  - hourly breakdown
  - system status: "ok" | "low" | "offline"
  - Telegram alert if status changes

This script is IDENTICAL across all sites.
The only things that change per site are:
  - DAILY_EXPECTED_KWH and DAILY_LOW_KWH below (edit directly in this file)
  - PV_COLUMN_INDEX if the PV Yield column position differs in the xlsx
  - Secrets (PLANT_NAME, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) in GitHub
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
# ✏️  SITE THRESHOLDS — edit these directly, do NOT set as GitHub secrets
# =============================================================================
DAILY_EXPECTED_KWH = 215.0   # Average good day for this site (kWh)
DAILY_LOW_KWH      = 36.0    # Known low-production day for this site (kWh)

# PV Yield column fallback — 0-based index (A=0, B=1, C=2, D=3, E=4, F=5...)
# The script auto-detects by scanning for "PV Yield" in the header row first.
# This is only used if the header name is not found.
PV_COLUMN_INDEX    = 4        # default = column E

# =============================================================================
# 🔒 SECRETS — set in GitHub repo Settings → Secrets → Actions
#              Never hardcode credentials here
# =============================================================================
PLANT_NAME         = os.environ.get("PLANT_NAME", "Solar Plant")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# =============================================================================
# FIXED CONFIG — same for all sites, no need to change
# =============================================================================
LOW_THRESHOLD_PCT  = 0.30    # alert if actual < 30% of curve-expected so far
OFFLINE_THRESHOLD  = 0.01    # kWh — treat as offline if total is below this

# Paths — relative to this script so they work on GitHub Actions
_HERE       = Path(__file__).parent
RAW_FILE    = _HERE / "data" / "raw_report.xlsx"
OUTPUT_FILE = _HERE / "data" / "processed.json"
STATE_FILE  = _HERE / "data" / "alert_state.json"

# Timezone: SAST = UTC+2
SAST = timezone(timedelta(hours=2))


# =============================================================================
# Solar curve helpers — Johannesburg seasonal sunrise/sunset
# =============================================================================

def solar_window(month: int) -> tuple:
    """
    Returns (sunrise_hour, sunset_hour) for Johannesburg, season-aware.

    Johannesburg (26°S):
      Summer (Dec/Jan): ~05:15 rise, ~18:45 set  (~13.5h window)
      Winter (Jun/Jul): ~06:45 rise, ~17:15 set  (~10.5h window)

    Uses a cosine approximation around the southern hemisphere solstices.
    """
    mid_day   = (month - 1) * 30 + 15   # approximate day-of-year at mid-month
    amplitude = 0.75                      # ±45 min seasonal shift from mean

    # Southern hemisphere: summer solstice near day 355 (Dec 21)
    angle  = 2 * math.pi * (mid_day - 355) / 365
    shift  = amplitude * math.cos(angle)  # positive in summer, negative in winter

    sunrise = 6.0 - shift
    sunset  = 18.0 + shift
    return sunrise, sunset


def solar_curve_fraction(hour: int, month: int) -> float:
    """
    What fraction of the day's total PV energy should have been generated
    by the END of `hour`, using a sine-bell curve between sunrise and sunset.

    Sine bell = natural solar generation shape:
      - Low output at sunrise and sunset
      - Peak output around solar noon
      - Smooth ramp up and down

    Returns 0.0 to 1.0.
    """
    sunrise, sunset = solar_window(month)
    solar_day = sunset - sunrise

    if solar_day <= 0:
        return 0.0

    elapsed = (hour + 1) - sunrise   # hours elapsed by end of this hour

    if elapsed <= 0:
        return 0.0
    if elapsed >= solar_day:
        return 1.0

    # Integral of sine bell: (1 - cos(pi * t / T)) / 2
    return (1 - math.cos(math.pi * elapsed / solar_day)) / 2


# =============================================================================
# Parse the xlsx
# =============================================================================

def parse_report(filepath: Path) -> dict:
    """
    Reads the FusionSolar daily report xlsx.
    Layout:
      Row 0  — title
      Row 1  — headers
      Row 2+ — hourly rows (col 0 = timestamp, PV Yield col = auto-detected)
    """
    df      = pd.read_excel(filepath, header=None, sheet_name=0)
    headers = [str(h).strip() if not pd.isna(h) else "" for h in df.iloc[1].tolist()]

    # Auto-detect PV Yield column, fall back to configured index
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
# Determine system status using solar curve
# =============================================================================

def determine_status(data: dict, month: int) -> tuple:
    """
    Returns (status, debug_info).
    status is one of: 'ok', 'low', 'offline'

    Uses a sine-bell solar curve fitted to Johannesburg's seasonal window
    to calculate how much energy we expect by the current hour.
    This avoids false low-production alerts during early morning / late
    afternoon when output is naturally low regardless of system health.
    """
    total             = data["total_kwh"]
    hour              = data["last_hour"]
    sunrise, sunset   = solar_window(month)

    # Nothing generated at all
    if total < OFFLINE_THRESHOLD:
        return "offline", {"reason": "below offline threshold"}

    # Before generation starts — too early to assess
    if (hour + 1) <= sunrise:
        return "ok", {"reason": "before sunrise"}

    curve_frac   = solar_curve_fraction(hour, month)
    expected_now = DAILY_EXPECTED_KWH * curve_frac

    # Wait until at least 10% of expected daily energy should be in
    if curve_frac < 0.10:
        return "ok", {"reason": "too early in solar day to assess"}

    debug = {
        "curve_fraction":    round(curve_frac, 3),
        "expected_by_now":   round(expected_now, 1),
        "actual_kwh":        total,
        "low_trigger_below": round(expected_now * LOW_THRESHOLD_PCT, 1),
        "sunrise":           round(sunrise, 2),
        "sunset":            round(sunset,  2),
    }

    if total < expected_now * LOW_THRESHOLD_PCT:
        return "low", debug

    return "ok", debug


# =============================================================================
# Telegram notification
# =============================================================================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠️  Telegram not configured — skipping alert")
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


def maybe_alert(status: str, data: dict, debug: dict):
    """Send a Telegram alert only when the status changes — avoids spam."""
    prev_status = "ok"
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    now_str  = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    total    = data["total_kwh"]
    hour     = data["last_hour"]
    expected = debug.get("expected_by_now", 0)

    if status == "offline" and prev_status != "offline":
        send_telegram(
            f"🔴 <b>{PLANT_NAME} — OFFLINE</b>\n"
            f"No PV generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"Checked: {now_str}"
        )
    elif status == "low" and prev_status not in ("low", "offline"):
        send_telegram(
            f"🟡 <b>{PLANT_NAME} — LOW PRODUCTION</b>\n"
            f"Production is significantly below expected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"Expected by now: <b>~{expected:.0f} kWh</b>\n"
            f"Checked: {now_str}"
        )
    elif status == "ok" and prev_status in ("low", "offline"):
        send_telegram(
            f"✅ <b>{PLANT_NAME} — RECOVERED</b>\n"
            f"System is producing normally again.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"Checked: {now_str}"
        )

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

    now              = datetime.now(SAST)
    month            = now.month
    sunrise, sunset  = solar_window(month)

    print(f"📥 Reading: {RAW_FILE}")
    data          = parse_report(RAW_FILE)
    status, debug = determine_status(data, month)

    print(f"  📅 Date:             {data['date']}")
    print(f"  ⚡ PV Yield:         {data['total_kwh']:.3f} kWh")
    print(f"  🕐 Last hour:        {data['last_hour']:02d}:00")
    print(f"  🌅 Solar window:     {sunrise:.1f}h – {sunset:.1f}h  (month {month})")
    print(f"  📈 Curve fraction:   {debug.get('curve_fraction', 0.0):.1%}")
    print(f"  🎯 Expected by now:  {debug.get('expected_by_now', 0.0):.1f} kWh")
    print(f"  ⚠️  Low alert below:  {debug.get('low_trigger_below', 0.0):.1f} kWh")
    print(f"  🚦 Status:           {status.upper()}")

    maybe_alert(status, data, debug)

    output = {
        "plant":        PLANT_NAME,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "date":         data["date"],
        "total_kwh":    data["total_kwh"],
        "last_hour":    data["last_hour"],
        "status":       status,
        "thresholds": {
            "expected_daily_kwh": DAILY_EXPECTED_KWH,
            "low_day_kwh":        DAILY_LOW_KWH,
            "low_alert_pct":      LOW_THRESHOLD_PCT,
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
