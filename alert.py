#!/usr/bin/env python3
"""Post a Discord summary when a new enriched snapshot is detected.

Reads the latest data/enriched/*.json, formats a compact message with
share of supply, role distribution and the top movers, and POSTs it
to the Discord webhook from the DISCORD_WEBHOOK env var.

Silent no-op if DISCORD_WEBHOOK is not set. Safe to run on every
workflow invocation — we only post the latest snapshot once per day
because the workflow runs daily.
"""

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
ENRICHED_DIR = DATA_DIR / "enriched"

REPO_WEB = "https://github.com/MatiasJF/exchange_addresses"
MAX_MOVERS_LISTED = 5


def latest_enriched_file() -> Path | None:
    if not ENRICHED_DIR.exists():
        return None
    files = sorted(ENRICHED_DIR.glob("*.json"))
    return files[-1] if files else None


def format_message(e: dict) -> str:
    date = e.get("date", "?")
    share = e.get("top1000_share_of_supply")
    rate = e.get("bsv_usd")
    total_bsv = e.get("total_top1000_bsv") or 0
    role_counts = e.get("role_counts") or {}
    movers = e.get("movers") or []

    lines = [f"**📊 BSV Top 1000 — {date}**"]
    usd_total = total_bsv * rate if rate else None
    line2 = f"Total: {total_bsv:,.0f} BSV"
    if usd_total:
        line2 += f" (${usd_total / 1e6:,.1f}M)"
    if share is not None:
        line2 += f" · {share * 100:.2f}% of supply"
    if rate:
        line2 += f" · BSV/USD ${rate:,.2f}"
    lines.append(line2)

    if role_counts:
        role_str = " · ".join(
            f"{k}: {v}" for k, v in sorted(role_counts.items(), key=lambda kv: -kv[1])
        )
        lines.append(f"Roles — {role_str}")

    if movers:
        prev = e.get("prev_date") or "prev"
        lines.append("")
        lines.append(f"**{len(movers)} movers** vs {prev} "
                     f"(threshold {e.get('mover_threshold_bsv')} BSV)")
        for m in movers[:MAX_MOVERS_LISTED]:
            addr = m.get("address", "?")
            delta_bsv = (m.get("delta_sats") or 0) / 1e8
            status = m.get("status", "")
            lines.append(f"• `{addr[:16]}…` {delta_bsv:+,.1f} BSV ({status})")
        if len(movers) > MAX_MOVERS_LISTED:
            lines.append(f"• …and {len(movers) - MAX_MOVERS_LISTED} more")

    lines.append("")
    lines.append(f"<{REPO_WEB}/blob/main/data/enriched/{date}.json>")
    return "\n".join(lines)


def post(webhook: str, content: str) -> None:
    body = json.dumps({"content": content, "username": "BSV Monitor"}).encode()
    req = urllib.request.Request(
        webhook, data=body,
        headers={"Content-Type": "application/json", "User-Agent": "BSV-Monitor/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"webhook returned {resp.status}")


def main():
    webhook = os.environ.get("DISCORD_WEBHOOK")
    if not webhook:
        print("DISCORD_WEBHOOK not set — skipping alert.")
        return 0

    ef = latest_enriched_file()
    if not ef:
        print("No enriched snapshot found — nothing to alert on.")
        return 0

    try:
        e = json.loads(ef.read_text())
    except Exception as exc:
        print(f"Failed to parse {ef}: {exc}")
        return 1

    msg = format_message(e)
    print(msg)
    print("---")
    try:
        post(webhook, msg)
        print("Posted to Discord.")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace") if exc.fp else ""
        print(f"Discord HTTP {exc.code}: {body[:500]}")
        return 1
    except Exception as exc:
        print(f"Discord post failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
