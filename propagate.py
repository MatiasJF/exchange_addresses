#!/usr/bin/env python3
"""Propagate entity labels through common-input clusters.

Reads:
  data/entities.json   — manually curated seeds (entity → addresses)
  data/clusters.json   — co-spend clusters from cluster.py

Writes:
  data/labels_resolved.json — every address whose cluster contains a seed
                              inherits the entity label, with a `via` field
                              recording whether it came from the seed list
                              directly or via cluster expansion.

Conflict handling: if two entities seed addresses into the same cluster,
the first entity in entities.json wins. Conflicts are logged.

No network calls. Cheap. Safe to re-run.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
ENTITIES_PATH = DATA_DIR / "entities.json"
CLUSTERS_PATH = DATA_DIR / "clusters.json"
LABELS_OUT = DATA_DIR / "labels_resolved.json"


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def main():
    print("Label propagation")
    print("=" * 40)

    entities_doc = load_json(ENTITIES_PATH, {"entities": []})
    clusters_doc = load_json(CLUSTERS_PATH, {"parent": {}, "clusters": {}})

    entities = entities_doc.get("entities") or []
    parent: dict[str, str] = clusters_doc.get("parent") or {}
    clusters: dict[str, list[str]] = clusters_doc.get("clusters") or {}

    # Map root → cluster_id (for readable reporting).
    root_to_cid: dict[str, str] = {}
    for cid, addrs in clusters.items():
        if not addrs:
            continue
        root = parent.get(addrs[0], addrs[0])
        root_to_cid[root] = cid

    resolved: dict[str, dict] = {}
    conflicts: list[dict] = []

    # Walk seeds in document order so earlier entities win on conflict.
    for ent in entities:
        eid = ent.get("id") or ent.get("name") or "?"
        name = ent.get("name", eid)
        seeds = ent.get("addresses") or []
        label_info_template = {
            "entity_id": eid,
            "entity_name": name,
            "category": ent.get("category"),
            "confidence": ent.get("confidence"),
            "source": ent.get("source"),
        }

        # For each seed address, expand through its cluster.
        expanded_roots: set[str] = set()
        for seed in seeds:
            # Seed itself gets labeled as "via: seed" even if not in any cluster.
            if seed not in resolved:
                resolved[seed] = {**label_info_template, "via": "seed"}
            else:
                conflicts.append({
                    "address": seed,
                    "existing": resolved[seed]["entity_id"],
                    "attempted": eid,
                    "kept": resolved[seed]["entity_id"],
                })
            if seed in parent:
                expanded_roots.add(parent[seed])

        # Expand through each cluster we touched.
        for root in expanded_roots:
            cid = root_to_cid.get(root)
            if not cid:
                continue
            for addr in clusters.get(cid, []):
                if addr in resolved:
                    if resolved[addr]["entity_id"] != eid:
                        conflicts.append({
                            "address": addr,
                            "existing": resolved[addr]["entity_id"],
                            "attempted": eid,
                            "kept": resolved[addr]["entity_id"],
                        })
                    continue
                resolved[addr] = {**label_info_template, "via": cid}

    out = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "entity_count": len(entities),
        "resolved_address_count": len(resolved),
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "resolved": resolved,
    }
    LABELS_OUT.write_text(json.dumps(out, indent=2))

    print(f"  Entities seeded: {len(entities)}")
    print(f"  Addresses resolved: {len(resolved)}")
    print(f"  Conflicts: {len(conflicts)}")

    # Per-entity summary
    by_entity: dict[str, int] = {}
    for info in resolved.values():
        by_entity[info["entity_id"]] = by_entity.get(info["entity_id"], 0) + 1
    for eid, n in sorted(by_entity.items(), key=lambda kv: -kv[1]):
        print(f"    {eid}: {n} addrs")
    print(f"  Wrote {LABELS_OUT}")
    print("Done.")


if __name__ == "__main__":
    main()
