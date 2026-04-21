#!/usr/bin/env python3
"""Enrich a daily snapshot with WhatsOnChain data.

Produces data/enriched/YYYY-MM-DD.json containing:
  - BSV/USD rate
  - Circulating BSV supply
  - Top-1000 share of circulating supply
  - Big movers between this snapshot and the previous one
  - For each big mover: recent tx hashes with their related addresses
    (counterparties; sender/receiver distinction is deferred to clustering)

Designed to be cheap and deterministic. API usage per day is bounded:
  3 constant calls (rate, supply, nothing else)
  + up to INVESTIGATE_TOP × (1 history + MAX_TXS_PER_MOVER tx details)
"""

import argparse
import json
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
ENRICHED_DIR = DATA_DIR / "enriched"

WOC_BASE = "https://api.whatsonchain.com/v1/bsv/main"
USER_AGENT = "BSV-Address-Monitor/1.0"

# Tunables
MOVER_THRESHOLD_BSV = 100.0            # addresses with |delta| ≥ this get flagged
INVESTIGATE_TOP = 20                    # cap on movers whose txs we fetch
MAX_TXS_PER_MOVER = 5                   # cap on txs inspected per mover
MAX_RELATED_PER_TX = 25                 # cap on addresses recorded per tx
DEFAULT_LOOKBACK_BLOCKS = 200           # ~1.4 days at 10-min blocks
REQ_SLEEP = 0.1                         # throttle between requests (s)

# Profiling tunables
BULK_BATCH = 20                         # WOC bulk endpoint cap
BATCH_SLEEP = 0.3                       # throttle between bulk POSTs
BLOCKS_PER_DAY = 144                    # approx (10-minute blocks)
HOT_UTXO_HINT = 20                      # ≥ this (or paginated) → many UTXOs
COLD_UTXO_HINT = 5                      # ≤ this → few UTXOs
COLD_AGE_DAYS = 30                      # newest UTXO older than this → dormant
ACTIVE_BALANCE_CV = 0.05                # CV above this → active balance
STABLE_BALANCE_CV = 0.005               # CV below this → stable balance


def woc_get(path: str, retries: int = 3, backoff: float = 2.0):
    """GET a WOC JSON endpoint. Returns parsed JSON, or None on 404."""
    url = f"{WOC_BASE}{path}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last_err = e
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
        if attempt < retries - 1:
            time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"WOC GET {path} failed: {last_err}")


def woc_post(path: str, body: dict, retries: int = 3, backoff: float = 2.0):
    """POST JSON to a WOC endpoint. Returns parsed JSON."""
    url = f"{WOC_BASE}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
        method="POST",
    )
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last_err = e
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
        if attempt < retries - 1:
            time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"WOC POST {path} failed: {last_err}")


def fetch_tip_height() -> int | None:
    data = woc_get("/chain/info")
    if isinstance(data, dict):
        for k in ("blocks", "height"):
            v = data.get(k)
            if isinstance(v, int):
                return v
    return None


def load_snapshot(date_str: str) -> dict | None:
    path = SNAPSHOTS_DIR / f"{date_str}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def previous_snapshot_date(date_str: str) -> str | None:
    """Return the most recent snapshot date strictly before date_str, or None."""
    dates = sorted(p.stem for p in SNAPSHOTS_DIR.glob("*.json") if p.stem < date_str)
    return dates[-1] if dates else None


def balance_map(snapshot: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    for a in snapshot["addresses"]:
        key = a.get("address") or a.get("scripthash")
        if key:
            out[key] = a["balance"]
    return out


def scripthash_map(snapshot: dict) -> dict[str, str]:
    """Return {address: scripthash} for every entry that has both."""
    out: dict[str, str] = {}
    for a in snapshot["addresses"]:
        addr = a.get("address")
        sh = a.get("scripthash")
        if addr and sh:
            out[addr] = sh
    return out


def woc_history(addr: str, scripthash: str | None) -> list[dict]:
    """Fetch confirmed tx history for an address. Prefers the script-hash path.

    Returns a list of {tx_hash, height} entries. WOC wraps the response in
    {address, script, result, error}; this unwraps to the `result` list.
    """
    paths = []
    if scripthash:
        paths.append(f"/script/{scripthash}/confirmed/history")
    paths.append(f"/address/{addr}/confirmed/history")
    for p in paths:
        try:
            data = woc_get(p)
        except Exception:
            continue
        if data is None:
            continue
        if isinstance(data, dict):
            if data.get("error"):
                continue
            res = data.get("result")
            if isinstance(res, list):
                return res
        elif isinstance(data, list):
            return data
    return []


def compute_movers(today: dict, prev: dict, threshold_sats: float) -> list[dict]:
    """Find addresses with significant balance change between two snapshots.

    Status:
      "present" — in both top-1000 snapshots
      "entered" — only in today's (previous balance unknown but was below cutoff)
      "dropped" — only in previous (current balance unknown but likely below cutoff)
    """
    tb = balance_map(today)
    yb = balance_map(prev)
    movers: list[dict] = []
    for addr in set(tb) | set(yb):
        cur = tb.get(addr)
        old = yb.get(addr)
        if cur is not None and old is not None:
            delta = cur - old
            status = "present"
        elif cur is None and old is not None:
            delta = -old
            status = "dropped"
        elif cur is not None and old is None:
            delta = cur
            status = "entered"
        else:
            continue
        if abs(delta) >= threshold_sats:
            movers.append({
                "address": addr,
                "delta_sats": int(delta),
                "balance_now_sats": cur,
                "balance_prev_sats": old,
                "status": status,
            })
    movers.sort(key=lambda m: abs(m["delta_sats"]), reverse=True)
    return movers


def extract_vout(tx: dict) -> list[tuple[str, float]]:
    """Return [(address, value_bsv)] from a WOC tx JSON's outputs.

    WOC's `vin` entries carry only {txid, vout, scriptSig} without resolved
    addresses, so input-side counterparties require prev-tx lookups (deferred
    to a later sprint). We record vout-side counterparties only and infer
    direction by whether our address is among the outputs.
    """
    out: list[tuple[str, float]] = []
    for vout in tx.get("vout", []) or []:
        value = vout.get("value")
        try:
            value_bsv = float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            value_bsv = 0.0
        spk = vout.get("scriptPubKey") or {}
        for a in spk.get("addresses") or []:
            if isinstance(a, str):
                out.append((a, value_bsv))
    return out


def investigate(addr: str, scripthash: str | None, lookback_blocks: int) -> list[dict]:
    """Fetch recent txs for addr and summarize counterparties per tx."""
    history = woc_history(addr, scripthash)
    time.sleep(REQ_SLEEP)
    if not history:
        return []

    heights = [int(h.get("height", 0)) for h in history if isinstance(h, dict)]
    if not heights:
        return []
    max_h = max(heights)
    cutoff = max_h - lookback_blocks

    recent = [
        h for h in history
        if isinstance(h, dict) and int(h.get("height", 0)) >= cutoff
    ]
    recent.sort(key=lambda h: int(h.get("height", 0)), reverse=True)
    recent = recent[:MAX_TXS_PER_MOVER]

    reports: list[dict] = []
    for entry in recent:
        txid = entry.get("tx_hash") or entry.get("txid")
        if not txid:
            continue
        try:
            tx = woc_get(f"/tx/hash/{txid}")
        except Exception as e:
            reports.append({"txid": txid, "error": f"tx fetch failed: {e}"})
            continue
        time.sleep(REQ_SLEEP)
        if not tx:
            continue

        vout = extract_vout(tx)
        value_to_us = sum(v for a, v in vout if a == addr)
        counterparties: list[str] = []
        value_to_others = 0.0
        for a, v in vout:
            if a == addr:
                continue
            counterparties.append(a)
            value_to_others += v
        # Dedupe while preserving order and cap size.
        counterparties = list(dict.fromkeys(counterparties))[:MAX_RELATED_PER_TX]

        # Direction heuristic: if none of the vout addresses equal ours, the
        # history inclusion implies we were a vin → this tx was a send.
        direction = "received" if value_to_us > 0 else "sent"

        reports.append({
            "txid": txid,
            "height": int(entry.get("height", 0)),
            "direction": direction,
            "value_to_us_bsv": round(value_to_us, 8),
            "value_to_others_bsv": round(value_to_others, 8),
            "counterparty_addresses": counterparties,
            "vin_count": len(tx.get("vin", []) or []),
            "vout_count": len(tx.get("vout", []) or []),
        })
    return reports


def fetch_bulk_utxos(addresses: list[str]) -> dict[str, dict]:
    """Fetch page-1 confirmed UTXOs for many addresses via WOC bulk endpoint.

    Returns {address: {"utxos": [...], "has_more": bool, "error": str|None}}.
    We intentionally don't follow `nextPageToken` — first page is enough for
    profiling, and walking pagination for 10k+ UTXO hot wallets would be
    expensive for marginal signal gain.
    """
    out: dict[str, dict] = {}
    for i in range(0, len(addresses), BULK_BATCH):
        batch = addresses[i:i + BULK_BATCH]
        try:
            data = woc_post("/addresses/confirmed/unspent", {"addresses": batch})
        except Exception as e:
            for a in batch:
                out[a] = {"utxos": [], "has_more": False, "error": str(e)}
            time.sleep(BATCH_SLEEP)
            continue
        if not isinstance(data, list):
            for a in batch:
                out[a] = {"utxos": [], "has_more": False, "error": "unexpected response"}
            time.sleep(BATCH_SLEEP)
            continue
        for entry in data:
            addr = entry.get("address")
            if not addr:
                continue
            err = entry.get("error") or None
            utxos = entry.get("result") or []
            has_more = bool(entry.get("nextPageToken"))
            out[addr] = {"utxos": utxos, "has_more": has_more, "error": err}
        # Fill in any addresses missing from response (rare)
        for a in batch:
            out.setdefault(a, {"utxos": [], "has_more": False, "error": "missing from response"})
        time.sleep(BATCH_SLEEP)
    return out


def balance_history(addr: str, snapshots: list[dict]) -> list[int]:
    """Return balance (sats) for `addr` across a chronological list of snapshots.

    Missing entries are skipped — the series is only days the address was in
    the top 1000 (churn into/out of top 1000 is signal, not missing data).
    """
    series: list[int] = []
    for snap in snapshots:
        for a in snap.get("addresses", []):
            if (a.get("address") or a.get("scripthash")) == addr:
                series.append(a.get("balance", 0))
                break
    return series


def coefficient_of_variation(values: list[int]) -> float | None:
    """CV = stddev/mean over positive values. Returns None when undefined."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    if mean <= 0:
        return None
    return statistics.pstdev(values) / mean


def profile_address(
    addr: str,
    utxo_info: dict,
    hist: list[int],
    tip_height: int | None,
) -> dict:
    """Compute a behavioral profile for one address."""
    utxos = utxo_info.get("utxos", []) or []
    has_more = utxo_info.get("has_more", False)
    err = utxo_info.get("error")

    heights = [int(u.get("height", 0)) for u in utxos if isinstance(u, dict)]
    newest_h = max(heights) if heights else None
    oldest_h = min(heights) if heights else None

    # Value-weighted avg UTXO height (within the page we saw).
    total_value = sum(int(u.get("value", 0) or 0) for u in utxos)
    weighted_h: float | None = None
    if total_value > 0 and heights:
        weighted_h = sum(
            int(u.get("height", 0)) * int(u.get("value", 0) or 0)
            for u in utxos
        ) / total_value

    newest_age_days = (tip_height - newest_h) / BLOCKS_PER_DAY if (tip_height and newest_h) else None
    oldest_age_days = (tip_height - oldest_h) / BLOCKS_PER_DAY if (tip_height and oldest_h) else None
    weighted_age_days = (tip_height - weighted_h) / BLOCKS_PER_DAY if (tip_height and weighted_h) else None

    cv = coefficient_of_variation(hist)
    mean_balance_bsv = (sum(hist) / len(hist) / 1e8) if hist else None

    return {
        "utxo_count_seen": len(utxos),
        "has_more_utxos": has_more,
        "utxo_fetch_error": err,
        "newest_utxo_height": newest_h,
        "oldest_utxo_height": oldest_h,
        "newest_utxo_age_days": round(newest_age_days, 2) if newest_age_days is not None else None,
        "oldest_utxo_age_days": round(oldest_age_days, 2) if oldest_age_days is not None else None,
        "weighted_utxo_age_days": round(weighted_age_days, 2) if weighted_age_days is not None else None,
        "snapshots_seen": len(hist),
        "mean_balance_bsv": round(mean_balance_bsv, 4) if mean_balance_bsv is not None else None,
        "balance_cv": round(cv, 4) if cv is not None else None,
    }


def classify(profile: dict) -> tuple[str, str]:
    """Rule-based behavioral role + confidence, derived from a profile.

    Labels describe behavior, not identity. "hot_wallet" means the address
    behaves like one (fragmented UTXOs + recent activity), not that we know
    it's an exchange.

    Rule order matters — dormancy is checked first so that long-idle
    addresses are never misclassified as hot just because they happen to
    have many old UTXOs.
    """
    utxos = profile.get("utxo_count_seen") or 0
    has_more = profile.get("has_more_utxos")
    newest_age = profile.get("newest_utxo_age_days")
    oldest_age = profile.get("oldest_utxo_age_days")
    cv = profile.get("balance_cv")
    snapshots_seen = profile.get("snapshots_seen") or 0

    # 1. Truly dormant → cold, regardless of UTXO count. Newest UTXO older
    #    than ~6 months means nothing has touched this address recently.
    if newest_age is not None and newest_age >= 180:
        return ("cold_storage", "high")

    # 2. Hot wallet: fragmented AND recent activity.
    looks_fragmented = has_more or utxos >= HOT_UTXO_HINT
    recently_touched = newest_age is not None and newest_age <= 7
    if looks_fragmented and recently_touched:
        # Balance also oscillating → very strong hot signal.
        if cv is not None and cv >= ACTIVE_BALANCE_CV:
            return ("hot_wallet", "high")
        # Balance stable despite UTXO churn is the classic exchange
        # consolidation pattern — still hot, medium confidence.
        return ("hot_wallet", "medium")

    # 3. Moderately idle few-UTXO address → cold storage.
    moderately_idle = newest_age is not None and newest_age >= COLD_AGE_DAYS
    if utxos > 0 and utxos <= HOT_UTXO_HINT and moderately_idle:
        return ("cold_storage", "medium")

    # 4. Stable balance over multiple snapshots with no recent activity → cold.
    if (cv is not None and cv <= STABLE_BALANCE_CV and snapshots_seen >= 2
            and (newest_age is None or newest_age > 7)):
        return ("cold_storage", "low")

    # 5. Active: balance moves notably but doesn't look like a hot wallet.
    if cv is not None and cv >= ACTIVE_BALANCE_CV:
        return ("active", "medium")

    return ("unknown", "low")


def fetch_rate() -> float | None:
    data = woc_get("/exchangerate")
    if isinstance(data, dict) and "rate" in data:
        try:
            return float(data["rate"])
        except (TypeError, ValueError):
            return None
    return None


def fetch_supply_bsv() -> float | None:
    data = woc_get("/circulatingsupply")
    # WOC returns a plain number or {"circulatingSupply": N}; handle both.
    if isinstance(data, (int, float)):
        return float(data)
    if isinstance(data, dict):
        for k in ("circulatingSupply", "circulating_supply", "supply"):
            if k in data:
                try:
                    return float(data[k])
                except (TypeError, ValueError):
                    pass
    return None


def main():
    parser = argparse.ArgumentParser(description="Enrich BSV snapshot with WOC data")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Snapshot date to enrich (default: today)")
    parser.add_argument("--threshold", type=float, default=MOVER_THRESHOLD_BSV,
                        help=f"Mover threshold in BSV (default: {MOVER_THRESHOLD_BSV})")
    parser.add_argument("--skip-investigation", action="store_true",
                        help="Skip the per-mover tx history fetch")
    parser.add_argument("--skip-profiling", action="store_true",
                        help="Skip the per-address UTXO / behavioral profiling")
    args = parser.parse_args()

    print(f"BSV Snapshot Enricher")
    print(f"{'=' * 40}")

    today = load_snapshot(args.date)
    if today is None:
        print(f"  No snapshot for {args.date}. Run collector.py first.")
        return

    prev_date = previous_snapshot_date(args.date)
    prev = load_snapshot(prev_date) if prev_date else None

    # Constant calls: price + supply
    print("  Fetching BSV/USD rate...")
    rate = fetch_rate()
    print(f"    rate: {rate}")
    time.sleep(REQ_SLEEP)

    print("  Fetching circulating supply...")
    supply = fetch_supply_bsv()
    print(f"    supply: {supply}")
    time.sleep(REQ_SLEEP)

    total_sats = sum(a["balance"] for a in today["addresses"])
    total_bsv = total_sats / 1e8
    share = (total_bsv / supply) if supply and supply > 0 else None

    # Movers + optional investigation
    threshold_sats = args.threshold * 1e8
    movers: list[dict] = []
    if prev is not None:
        movers = compute_movers(today, prev, threshold_sats)
        print(f"  Detected {len(movers)} movers vs {prev_date} "
              f"(threshold {args.threshold} BSV)")

        if not args.skip_investigation and movers:
            # Prefer scripthash lookups (more reliable than address path).
            # Merge scripthash info from both snapshots (dropped addresses are
            # absent from today's; entered addresses absent from yesterday's).
            sh_map = {**scripthash_map(prev), **scripthash_map(today)}

            # Scale lookback with date gap to capture the whole period.
            try:
                d_today = datetime.strptime(args.date, "%Y-%m-%d").date()
                d_prev = datetime.strptime(prev_date, "%Y-%m-%d").date()
                days_gap = max(1, (d_today - d_prev).days)
            except ValueError:
                days_gap = 1
            lookback = max(DEFAULT_LOOKBACK_BLOCKS, days_gap * 144 + 100)
            print(f"  Investigating top {min(len(movers), INVESTIGATE_TOP)} movers "
                  f"(lookback {lookback} blocks)...")
            for m in movers[:INVESTIGATE_TOP]:
                m["txs"] = investigate(m["address"], sh_map.get(m["address"]), lookback)
    else:
        print("  No previous snapshot, skipping mover detection.")

    # Behavioral profiling (no identity claims — classifies by on-chain behavior).
    profiles: dict[str, dict] = {}
    role_counts: dict[str, int] = {}
    tip_height = None
    if not args.skip_profiling:
        print("  Fetching chain tip for UTXO age calc...")
        tip_height = fetch_tip_height()
        print(f"    tip: {tip_height}")
        time.sleep(REQ_SLEEP)

        today_addrs = [
            a.get("address") or a.get("scripthash")
            for a in today["addresses"]
        ]
        today_addrs = [a for a in today_addrs if a]

        print(f"  Fetching UTXO pages for {len(today_addrs)} addresses "
              f"({(len(today_addrs) + BULK_BATCH - 1) // BULK_BATCH} bulk calls)...")
        utxo_map = fetch_bulk_utxos(today_addrs)

        # Build chronological list of all snapshots for balance-history lookup.
        all_snapshots: list[dict] = []
        for p in sorted(SNAPSHOTS_DIR.glob("*.json")):
            if p.stem > args.date:
                continue
            try:
                all_snapshots.append(json.loads(p.read_text()))
            except Exception:
                continue

        print(f"  Profiling from {len(all_snapshots)} snapshot(s)...")
        for addr in today_addrs:
            hist = balance_history(addr, all_snapshots)
            profile = profile_address(addr, utxo_map.get(addr, {}), hist, tip_height)
            role, confidence = classify(profile)
            profile["role"] = role
            profile["role_confidence"] = confidence
            profiles[addr] = profile
            role_counts[role] = role_counts.get(role, 0) + 1

    result = {
        "date": args.date,
        "prev_date": prev_date,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "tip_height": tip_height,
        "bsv_usd": rate,
        "circulating_supply_bsv": supply,
        "total_top1000_bsv": total_bsv,
        "top1000_share_of_supply": share,
        "mover_threshold_bsv": args.threshold,
        "mover_count": len(movers),
        "movers": movers,
        "role_counts": role_counts,
        "profiles": profiles,
    }

    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ENRICHED_DIR / f"{args.date}.json"
    out_path.write_text(json.dumps(result, indent=2))
    print(f"  Wrote {out_path}")

    # Quick summary for the log
    if supply:
        print(f"  Top 1000 share of supply: {share * 100:.2f}%")
    if rate:
        print(f"  Top 1000 USD value: ${total_bsv * rate:,.0f}")
    if movers:
        top3 = movers[:3]
        for m in top3:
            print(f"    {m['address'][:16]}...  Δ {m['delta_sats'] / 1e8:+,.2f} BSV  ({m['status']})")
    if role_counts:
        print("  Behavioral roles:")
        for role, count in sorted(role_counts.items(), key=lambda kv: -kv[1]):
            print(f"    {role}: {count}")
    print("Done.")


if __name__ == "__main__":
    main()
