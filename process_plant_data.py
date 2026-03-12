"""
process_plant_data.py

Reads the downloaded raw xlsx and extracts PV Yield (kWh) from column E.
Produces data/processed.json with:
  - today's total PV yield
  - hourly breakdown
  - system status: "ok" | "low" | "offline"
  - Telegram alert if status changes to low/offline

Thresholds (configurable below):
  DAILY_EXPECTED_KWH  = 128   (0.1 MWh average day)
  DAILY_LOW_KWH       = 36    (known low production day)
  LOW_THRESHOLD_PCT   = 0.30   (alert if < 30% of expected by end of day)
  OFFLINE_THRESHOLD   = 0.01   (alert if total < 0.01 kWh — nothing recorded)
"""

import json
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# ✏️  CONFIGURATION
# =============================================================================
PLANT_NAME          = os.environ.get("PLANT_NAME", "Addo Spar")

# Paths are relative to this script's directory so they work on GitHub Actions
_HERE      = Path(__file__).parent
RAW_FILE   = _HERE / Path(os.environ.get("RAW_FILE",    "data/raw_report.xlsx"))
OUTPUT_FILE= _HERE / Path(os.environ.get("OUTPUT_FILE", "data/processed.json"))
STATE_FILE = _HERE / "data/alert_state.json"

# PV Yield column — 0-based index (A=0, B=1, C=2, D=3, E=4, F=5...)
# Script first tries to find the column by header name "PV Yield" automatically.
# PV_COLUMN_INDEX is only used as a fallback if the header name isn't found.
# Override per-site via GitHub secret if needed.
PV_COLUMN_INDEX     = int(os.environ.get("PV_COLUMN_INDEX", "4"))  # default = column E

# Production thresholds
DAILY_EXPECTED_KWH  = float(os.environ.get("DAILY_EXPECTED_KWH", "128.0"))
DAILY_LOW_KWH       = float(os.environ.get("DAILY_LOW_KWH", "36.0"))
LOW_THRESHOLD_PCT   = 0.30     # alert when projected daily < 30% of expected
OFFLINE_THRESHOLD   = 0.01     # kWh — treat as offline if below this

# Telegram (set as secrets / env vars)
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID",   "")

# Timezone: SAST = UTC+2
SAST = timezone(timedelta(hours=2))

# =============================================================================
# Parse the xlsx
# =============================================================================

def parse_report(filepath: Path) -> dict:
    """
    Reads the FusionSolar daily report xlsx.
    Layout:
      Row 0  - title
      Row 1  - headers  (col 4 = 'PV Yield (kWh)')
      Row 2+ - hourly rows, col 0 = timestamp, col 4 = PV kWh
    Returns:
      {
        'date':        '2026-03-11',
        'total_kwh':   float,
        'hourly':      [24 floats],   # index = hour 0-23
        'last_hour':   int,
        'row_count':   int,
      }
    """
    df = pd.read_excel(filepath, header=None, sheet_name=0)

    # Identify PV Yield column (should be col index 4, but search to be safe)
    headers = [str(h).strip() if not pd.isna(h) else "" for h in df.iloc[1].tolist()]
    pv_col = next(
        (i for i, h in enumerate(headers) if "PV Yield" in h),
        PV_COLUMN_INDEX,  # fallback to configured index (default col E = 4)
    )
    print(f"  ℹ️  PV Yield column index: {pv_col}  (header: '{headers[pv_col]}')")

    hourly  = [0.0] * 24
    total   = 0.0
    last_hour  = 0
    row_count  = 0
    report_date = None

    for idx in range(2, len(df)):
        row = df.iloc[idx]
        ts_raw = row.iloc[0]
        if pd.isna(ts_raw):
            continue

        try:
            ts = pd.Timestamp(ts_raw)
            hour = ts.hour
            if report_date is None:
                report_date = ts.strftime("%Y-%m-%d")
        except Exception:
            continue

        pv_val = float(row.iloc[pv_col]) if not pd.isna(row.iloc[pv_col]) else 0.0
        hourly[hour] = round(pv_val, 4)
        total += pv_val
        last_hour = hour
        row_count += 1

    return {
        "date":       report_date or datetime.now(SAST).strftime("%Y-%m-%d"),
        "total_kwh":  round(total, 3),
        "hourly":     hourly,
        "last_hour":  last_hour,
        "row_count":  row_count,
    }


# =============================================================================
# Determine system status
# =============================================================================

def determine_status(data: dict) -> str:
    """
    Returns 'ok', 'low', or 'offline' based on PV yield.
    Uses a scaled expected value based on how far through the solar day we are.
    Solar hours are roughly 06:00–18:00 (12 hours).
    """
    total   = data["total_kwh"]
    hour    = data["last_hour"]

    if total < OFFLINE_THRESHOLD:
        return "offline"

    # Only assess low-production during / after solar hours
    if hour < 6:
        return "ok"   # too early, don't panic

    # Scale expected output by solar hour fraction completed
    solar_hours_elapsed = max(0, hour - 6)  # hours since solar start
    solar_day_hours     = 12                 # 06:00–18:00
    fraction            = min(solar_hours_elapsed / solar_day_hours, 1.0)

    if fraction < 0.15:
        return "ok"   # less than ~2 solar hours in, too early to judge

    expected_so_far = DAILY_EXPECTED_KWH * fraction
    if total < expected_so_far * LOW_THRESHOLD_PCT:
        return "low"

    return "ok"


# =============================================================================
# Telegram notification
# =============================================================================

def send_telegram(message: str) -> bool:
    """Send a Telegram message. Returns True if successful."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠️  Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing)")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            print(f"  ✅ Telegram alert sent")
            return True
        else:
            print(f"  ❌ Telegram error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"  ❌ Telegram request failed: {e}")
        return False


def maybe_alert(status: str, data: dict):
    """
    Send a Telegram alert only when status changes (avoids spam).
    State is persisted in STATE_FILE.
    """
    # Load previous state
    prev_status = "ok"
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    # Build message
    now_str = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    total   = data["total_kwh"]
    hour    = data["last_hour"]

    if status == "offline" and prev_status != "offline":
        msg = (
            f"🔴 <b>{PLANT_NAME} — OFFLINE</b>\n"
            f"No PV generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"Checked: {now_str}"
        )
        send_telegram(msg)

    elif status == "low" and prev_status not in ("low", "offline"):
        msg = (
            f"🟡 <b>{PLANT_NAME} — LOW PRODUCTION</b>\n"
            f"Production is significantly below expected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"Expected (scaled): ~{DAILY_EXPECTED_KWH * min(max(0, hour-6)/12, 1.0):.0f} kWh by now\n"
            f"Checked: {now_str}"
        )
        send_telegram(msg)

    elif status == "ok" and prev_status in ("low", "offline"):
        msg = (
            f"✅ <b>{PLANT_NAME} — RECOVERED</b>\n"
            f"System is producing normally again.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"Checked: {now_str}"
        )
        send_telegram(msg)

    # Save new state
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing data for: {PLANT_NAME}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not RAW_FILE.exists():
        print(f"❌ Raw file not found: {RAW_FILE}")
        sys.exit(1)

    print(f"📥 Reading: {RAW_FILE}")
    data   = parse_report(RAW_FILE)
    status = determine_status(data)

    print(f"  📅 Date:        {data['date']}")
    print(f"  ⚡ PV Yield:    {data['total_kwh']:.3f} kWh")
    print(f"  🕐 Last hour:   {data['last_hour']:02d}:00")
    print(f"  📊 Rows parsed: {data['row_count']}")
    print(f"  🚦 Status:      {status.upper()}")

    # Alert if needed
    maybe_alert(status, data)

    # Build output
    now = datetime.now(SAST)
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
        },
        "hourly_pv": data["hourly"],
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ Saved: {OUTPUT_FILE}")
    print("✅ Processing complete!")


if __name__ == "__main__":
    main()
