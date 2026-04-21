#!/usr/bin/env python3
"""Common-input clustering for top-1000 BSV addresses.

Applies the Nakamoto common-input heuristic (BTC whitepaper, 2008): when
multiple addresses are inputs to a single transaction, they are almost
certainly controlled by the same entity (you need the private key for
every input to sign a tx). CoinJoin breaks this assumption in theory
but is essentially non-existent on BSV.

Input: mover txids referenced in data/enriched/*.json.
Output: data/clusters.json with a union-find parent dict (for incremental
reloads) and a human-readable cluster map.

Runs incrementally: each invocation skips txs already processed. Safe to
re-run; unioning already-connected nodes is a no-op.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
ENRICHED_DIR = DATA_DIR / "enriched"
CLUSTERS_PATH = DATA_DIR / "clusters.json"

WOC_BASE = "https://api.whatsonchain.com/v1/bsv/main"
USER_AGENT = "BSV-Address-Monitor/1.0"

MAX_VIN_RESOLVE = 15
MAX_TXS_PER_RUN = 100
REQ_SLEEP = 0.1


def woc_get(path: str, retries: int = 3, backoff: float = 2.0):
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


class UnionFind:
    """Minimal union-find with path compression."""

    def __init__(self, parent: dict[str, str] | None = None):
        self.parent: dict[str, str] = dict(parent) if parent else {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
            return x
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # Path compression.
        while self.parent[x] != root:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def load_state() -> dict:
    if not CLUSTERS_PATH.exists():
        return {"parent": {}, "processed_txs": []}
    try:
        return json.loads(CLUSTERS_PATH.read_text())
    except Exception:
        return {"parent": {}, "processed_txs": []}


def save_state(uf: UnionFind, processed: set[str]) -> None:
    # Normalise parent dict (point everything to its root for a tidy file).
    compressed: dict[str, str] = {}
    for addr in list(uf.parent.keys()):
        compressed[addr] = uf.find(addr)

    groups: dict[str, list[str]] = {}
    for addr, root in compressed.items():
        groups.setdefault(root, []).append(addr)

    # Assign stable cluster IDs by size desc, then lexicographic for ties.
    sorted_groups = sorted(groups.values(), key=lambda g: (-len(g), g[0]))
    clusters = {f"cluster_{i:04d}": sorted(g) for i, g in enumerate(sorted_groups)}

    state = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "address_count": len(compressed),
        "cluster_count": len(clusters),
        "multi_member_cluster_count": sum(1 for g in sorted_groups if len(g) > 1),
        "parent": compressed,
        "processed_txs": sorted(processed),
        "clusters": clusters,
    }
    CLUSTERS_PATH.write_text(json.dumps(state, indent=2))


def resolve_inputs(tx: dict, tx_cache: dict[str, dict | None]) -> list[str]:
    """Return addresses of the first MAX_VIN_RESOLVE inputs of a tx."""
    addrs: list[str] = []
    vins = (tx.get("vin") or [])[:MAX_VIN_RESOLVE]
    for vin in vins:
        if vin.get("coinbase"):
            continue  # miner reward — no prior output to resolve
        prev_txid = vin.get("txid")
        prev_idx = vin.get("vout")
        if not prev_txid or prev_idx is None:
            continue
        prev_tx = tx_cache.get(prev_txid)
        if prev_tx is None and prev_txid not in tx_cache:
            try:
                prev_tx = woc_get(f"/tx/hash/{prev_txid}")
            except Exception:
                prev_tx = None
            tx_cache[prev_txid] = prev_tx
            time.sleep(REQ_SLEEP)
        if not prev_tx:
            continue
        vouts = prev_tx.get("vout") or []
        if prev_idx >= len(vouts):
            continue
        spk = vouts[prev_idx].get("scriptPubKey") or {}
        for a in spk.get("addresses") or []:
            if isinstance(a, str):
                addrs.append(a)
    # Preserve order, dedupe.
    return list(dict.fromkeys(addrs))


def gather_new_txids(processed: set[str]) -> list[str]:
    """Collect mover txids from every enriched file that haven't been
    processed yet. Preserves chronological order (newest last)."""
    seen: dict[str, None] = {}
    for efile in sorted(ENRICHED_DIR.glob("*.json")):
        try:
            data = json.loads(efile.read_text())
        except Exception:
            continue
        for m in data.get("movers", []) or []:
            for t in m.get("txs", []) or []:
                txid = t.get("txid")
                if txid and txid not in processed and txid not in seen:
                    seen[txid] = None
    return list(seen.keys())


def main():
    parser = argparse.ArgumentParser(description="Build co-spend clusters")
    parser.add_argument("--max-txs", type=int, default=MAX_TXS_PER_RUN)
    args = parser.parse_args()
    max_txs = args.max_txs

    print(f"Cluster builder")
    print(f"{'=' * 40}")

    state = load_state()
    uf = UnionFind(state.get("parent") or {})
    processed: set[str] = set(state.get("processed_txs") or [])
    print(f"  Loaded state: {len(uf.parent)} addrs, {len(processed)} txs processed")

    new_txids = gather_new_txids(processed)[:max_txs]
    if not new_txids:
        print("  No new txs to process.")
        save_state(uf, processed)
        return

    print(f"  {len(new_txids)} new tx(s) to process (cap {max_txs})")

    tx_cache: dict[str, dict | None] = {}
    unions_added = 0
    for idx, txid in enumerate(new_txids, 1):
        try:
            tx = woc_get(f"/tx/hash/{txid}")
        except Exception as e:
            print(f"  [{idx}/{len(new_txids)}] {txid[:16]}...  fetch error: {e}")
            continue
        time.sleep(REQ_SLEEP)
        processed.add(txid)
        if not tx:
            continue

        inputs = resolve_inputs(tx, tx_cache)
        if len(inputs) < 2:
            continue  # single-input or unresolved — no clustering info
        anchor = inputs[0]
        for other in inputs[1:]:
            if uf.find(anchor) != uf.find(other):
                unions_added += 1
            uf.union(anchor, other)

    save_state(uf, processed)
    n_clusters = len({uf.find(a) for a in uf.parent})
    multi = sum(1 for a in set(uf.find(x) for x in uf.parent) if
                sum(1 for x in uf.parent if uf.find(x) == a) > 1)
    print(f"  New unions: {unions_added}")
    print(f"  Totals: {len(uf.parent)} addrs, {n_clusters} clusters "
          f"({multi} with >1 member)")
    print(f"  Cached prev-txs: {len(tx_cache)}")
    print("Done.")


if __name__ == "__main__":
    main()
