"""Fluctuation detection and aggregation for BSV top 1000 address monitoring."""

import json
from pathlib import Path

import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).parent / "data"
TIMESERIES_PATH = DATA_DIR / "timeseries.parquet"
LABELS_PATH = DATA_DIR / "labels.json"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
ENRICHED_DIR = DATA_DIR / "enriched"
CLUSTERS_PATH = DATA_DIR / "clusters.json"
ENTITIES_PATH = DATA_DIR / "entities.json"
LABELS_RESOLVED_PATH = DATA_DIR / "labels_resolved.json"


def sat_to_bsv(sats) -> float:
    """Convert satoshis to BSV."""
    return sats / 1e8


def load_timeseries() -> pd.DataFrame:
    """Load the consolidated timeseries. Falls back to rebuilding from snapshots
    when the parquet cache is absent (e.g., fresh Streamlit Cloud deploy)."""
    if TIMESERIES_PATH.exists():
        ts = pd.read_parquet(TIMESERIES_PATH, engine="pyarrow")
    else:
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
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        ts = df.pivot_table(index="date", columns="address", values="balance", aggfunc="first")

    ts.index = pd.to_datetime(ts.index)
    ts.sort_index(inplace=True)
    return ts


def load_labels() -> dict[str, str]:
    """Load known address labels."""
    if not LABELS_PATH.exists():
        return {}
    with open(LABELS_PATH) as f:
        return json.load(f)


def save_labels(labels: dict[str, str]):
    """Save address labels."""
    with open(LABELS_PATH, "w") as f:
        json.dump(labels, f, indent=2)


def latest_snapshot() -> tuple[str, list[dict]] | None:
    """Return the most recent snapshot (date, addresses) or None."""
    snapshots = sorted(SNAPSHOTS_DIR.glob("*.json"))
    if not snapshots:
        return None
    with open(snapshots[-1]) as f:
        snap = json.load(f)
    return snap["date"], snap["addresses"]


def snapshot_dates() -> list[str]:
    """Return all available snapshot dates sorted."""
    return sorted(s.stem for s in SNAPSHOTS_DIR.glob("*.json"))


def load_snapshot(date_str: str) -> dict | None:
    """Load a specific snapshot by date."""
    path = SNAPSHOTS_DIR / f"{date_str}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def compute_changes(ts: pd.DataFrame, window: int = 1) -> pd.DataFrame:
    """Compute balance changes over a window of days for all addresses.

    Returns DataFrame with: address, balance_now, balance_prev,
    change_abs, change_pct, rank_now, rank_prev.
    """
    if len(ts) < 2 or len(ts) < window + 1:
        return pd.DataFrame()

    now = ts.iloc[-1]
    prev = ts.iloc[-(window + 1)]

    # Only addresses present in both snapshots
    common = now.dropna().index.intersection(prev.dropna().index)
    if common.empty:
        return pd.DataFrame()

    balance_now = now[common]
    balance_prev = prev[common]
    change_abs = balance_now - balance_prev
    change_pct = (change_abs / balance_prev.replace(0, np.nan)) * 100

    # Rankings (1 = highest balance) — computed over the full snapshot so the
    # number reflects global position, not position within the common subset.
    rank_now = now.dropna().rank(ascending=False, method="min").astype(int).reindex(common)
    rank_prev = prev.dropna().rank(ascending=False, method="min").astype(int).reindex(common)

    result = pd.DataFrame({
        "address": common,
        "balance_now": balance_now.values,
        "balance_prev": balance_prev.values,
        "change_abs": change_abs.values,
        "change_pct": change_pct.values,
        "rank_now": rank_now.values,
        "rank_prev": rank_prev.values,
    }).reset_index(drop=True)

    result.sort_values("change_abs", key=abs, ascending=False, inplace=True)
    return result


def detect_big_movers(
    changes: pd.DataFrame,
    abs_threshold_bsv: float = 100.0,
    pct_threshold: float = 10.0,
) -> pd.DataFrame:
    """Filter to addresses with significant balance changes."""
    if changes.empty:
        return changes

    abs_threshold_sats = abs_threshold_bsv * 1e8
    mask = (changes["change_abs"].abs() >= abs_threshold_sats) | (
        changes["change_pct"].abs() >= pct_threshold
    )
    return changes[mask].copy()


def aggregate_metrics(ts: pd.DataFrame) -> pd.DataFrame:
    """Compute per-date aggregate metrics across the top 1000.

    Returns DataFrame with columns: date, total_bsv, herfindahl,
    count_10k, count_100k, count_1m.
    """
    if ts.empty:
        return pd.DataFrame()

    records = []
    for date, row in ts.iterrows():
        balances = row.dropna()
        if balances.empty:
            continue

        total_sats = balances.sum()
        total_bsv = sat_to_bsv(total_sats)
        bsv_values = balances / 1e8

        # Herfindahl index: sum of squared market shares
        if total_sats > 0:
            shares = balances / total_sats
            hhi = (shares ** 2).sum()
        else:
            hhi = 0.0

        records.append({
            "date": date,
            "total_bsv": total_bsv,
            "herfindahl": hhi,
            "count_10k": int((bsv_values >= 10_000).sum()),
            "count_100k": int((bsv_values >= 100_000).sum()),
            "count_1m": int((bsv_values >= 1_000_000).sum()),
            "address_count": len(balances),
        })

    return pd.DataFrame(records)


def address_history(ts: pd.DataFrame, address: str) -> pd.DataFrame:
    """Get the balance history for a specific address."""
    if address not in ts.columns:
        return pd.DataFrame()
    series = ts[address].dropna()
    return pd.DataFrame({
        "date": series.index,
        "balance_bsv": series.values / 1e8,
    })


# --- Sprint 5: enriched / cluster / entity loaders ---------------------------

def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def latest_enriched() -> dict:
    """Return the most recent enriched snapshot, or {} if none."""
    files = sorted(ENRICHED_DIR.glob("*.json")) if ENRICHED_DIR.exists() else []
    if not files:
        return {}
    return _load_json(files[-1], {})


def load_enriched(date_str: str) -> dict:
    """Return the enriched snapshot for a specific date, or {}."""
    path = ENRICHED_DIR / f"{date_str}.json"
    return _load_json(path, {})


def enriched_dates() -> list[str]:
    if not ENRICHED_DIR.exists():
        return []
    return sorted(p.stem for p in ENRICHED_DIR.glob("*.json"))


def enriched_timeseries() -> pd.DataFrame:
    """Return a DataFrame with one row per enriched snapshot, columns:
    date, bsv_usd, circulating_supply_bsv, total_top1000_bsv,
    top1000_share_of_supply, total_top1000_usd."""
    rows: list[dict] = []
    for date in enriched_dates():
        e = load_enriched(date)
        if not e:
            continue
        total_bsv = e.get("total_top1000_bsv")
        rate = e.get("bsv_usd")
        rows.append({
            "date": date,
            "bsv_usd": rate,
            "circulating_supply_bsv": e.get("circulating_supply_bsv"),
            "total_top1000_bsv": total_bsv,
            "top1000_share_of_supply": e.get("top1000_share_of_supply"),
            "total_top1000_usd": (total_bsv * rate) if total_bsv and rate else None,
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    return df


def load_clusters() -> dict:
    """Return {parent, clusters, cluster_count, ...} or empty scaffold."""
    return _load_json(CLUSTERS_PATH, {"parent": {}, "clusters": {}})


def load_resolved_labels() -> dict[str, dict]:
    """Return {address: {entity_id, entity_name, category, confidence, via}}."""
    doc = _load_json(LABELS_RESOLVED_PATH, {"resolved": {}})
    return doc.get("resolved") or {}


def cluster_for_address(clusters_doc: dict, address: str) -> tuple[str | None, list[str]]:
    """Return (cluster_id, [members]) for an address, or (None, [])."""
    parent = clusters_doc.get("parent") or {}
    clusters = clusters_doc.get("clusters") or {}
    if address not in parent:
        return (None, [])
    root = parent[address]
    # Find the cluster id whose root matches.
    for cid, members in clusters.items():
        if not members:
            continue
        # Any member's parent is the root.
        if parent.get(members[0]) == root:
            return (cid, members)
    return (None, [])
