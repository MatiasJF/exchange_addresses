#!/usr/bin/env python3
"""Daily snapshot fetcher for BSV top 1000 richest addresses from Bitails API."""

import argparse
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
TIMESERIES_PATH = DATA_DIR / "timeseries.parquet"
API_URL = "https://api.bitails.io/analytics/address/rich"


def fetch_rich_list(retries: int = 3, backoff: float = 2.0) -> list[dict]:
    """Fetch top 1000 richest BSV addresses from Bitails API."""
    req = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "BSV-Address-Monitor/1.0"},
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            if isinstance(data, list):
                return data
            # Some API responses wrap in an object
            if isinstance(data, dict) and "addresses" in data:
                return data["addresses"]
            return data
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                print(f"  Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed to fetch rich list after {retries} attempts: {e}")


def save_snapshot(addresses: list[dict], date_str: str) -> Path:
    """Save a dated JSON snapshot."""
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "date": date_str,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "count": len(addresses),
        "addresses": addresses,
    }
    path = SNAPSHOTS_DIR / f"{date_str}.json"
    path.write_text(json.dumps(snapshot, indent=2))
    return path


def rebuild_timeseries() -> pd.DataFrame:
    """Rebuild the consolidated timeseries parquet from all snapshots."""
    rows = []
    for snapshot_file in sorted(SNAPSHOTS_DIR.glob("*.json")):
        with open(snapshot_file) as f:
            snap = json.load(f)
        date = snap["date"]
        for addr in snap["addresses"]:
            key = addr.get("address") or addr.get("scripthash")
            if key:
                rows.append({"date": date, "address": key, "balance": addr["balance"]})

    if not rows:
        print("  No snapshots found, skipping timeseries rebuild.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Pivot to wide format: rows=dates, columns=addresses, values=balances
    ts = df.pivot_table(index="date", columns="address", values="balance", aggfunc="first")
    ts.index = pd.to_datetime(ts.index)
    ts.sort_index(inplace=True)
    ts.to_parquet(TIMESERIES_PATH, engine="pyarrow")
    print(f"  Timeseries rebuilt: {ts.shape[0]} dates x {ts.shape[1]} addresses")
    return ts


def print_summary(addresses: list[dict], date_str: str):
    """Print a summary of the snapshot."""
    total_sats = sum(a["balance"] for a in addresses)
    total_bsv = total_sats / 1e8
    top = addresses[0] if addresses else {}
    top_addr = top.get("address", top.get("scripthash", "?"))
    top_bsv = top.get("balance", 0) / 1e8

    print(f"  Date: {date_str}")
    print(f"  Addresses: {len(addresses)}")
    print(f"  Total BSV in top 1000: {total_bsv:,.2f}")
    print(f"  #1 address: {top_addr[:16]}... ({top_bsv:,.2f} BSV)")

    # Compare with previous snapshot
    snapshots = sorted(SNAPSHOTS_DIR.glob("*.json"))
    prev_files = [s for s in snapshots if s.stem < date_str]
    if prev_files:
        with open(prev_files[-1]) as f:
            prev = json.load(f)
        prev_addrs = {a.get("address") or a.get("scripthash") for a in prev["addresses"]}
        curr_addrs = {a.get("address") or a.get("scripthash") for a in addresses}
        new = curr_addrs - prev_addrs
        dropped = prev_addrs - curr_addrs
        prev_total = sum(a["balance"] for a in prev["addresses"]) / 1e8
        delta = total_bsv - prev_total
        print(f"  vs {prev_files[-1].stem}: {delta:+,.2f} BSV, {len(new)} new, {len(dropped)} dropped")


def main():
    parser = argparse.ArgumentParser(description="Fetch BSV top 1000 rich list snapshot")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Snapshot date (default: today)")
    args = parser.parse_args()

    print(f"BSV Top 1000 Collector")
    print(f"{'=' * 40}")

    # Check if snapshot already exists
    target = SNAPSHOTS_DIR / f"{args.date}.json"
    if target.exists():
        print(f"  Snapshot for {args.date} already exists, skipping fetch.")
        print(f"  Rebuilding timeseries...")
        rebuild_timeseries()
        return

    print(f"  Fetching rich list from Bitails...")
    addresses = fetch_rich_list()
    print(f"  Fetched {len(addresses)} addresses.")

    path = save_snapshot(addresses, args.date)
    print(f"  Saved snapshot: {path}")

    print_summary(addresses, args.date)

    print(f"  Rebuilding timeseries...")
    rebuild_timeseries()
    print(f"Done.")


if __name__ == "__main__":
    main()
