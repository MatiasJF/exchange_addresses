#!/usr/bin/env python3
"""Streamlit dashboard for BSV top 1000 address monitoring."""

from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import json

from analysis import (
    load_timeseries, load_labels, save_labels, latest_snapshot,
    snapshot_dates, load_snapshot, compute_changes, detect_big_movers,
    aggregate_metrics, address_history, sat_to_bsv,
    latest_enriched, enriched_timeseries, load_clusters,
    load_resolved_labels, cluster_for_address,
)

st.set_page_config(page_title="BSV Top 1000 Monitor", layout="wide")

# --- Data loading ---
@st.cache_data(ttl=3600)
def load_data():
    ts = load_timeseries()
    labels = load_labels()
    snap = latest_snapshot()
    dates = snapshot_dates()
    enriched = latest_enriched()
    ets = enriched_timeseries()
    clusters_doc = load_clusters()
    resolved_labels = load_resolved_labels()
    return ts, labels, snap, dates, enriched, ets, clusters_doc, resolved_labels

ts, labels, snap, dates, enriched, ets, clusters_doc, resolved_labels = load_data()

has_data = snap is not None
has_timeseries = not ts.empty and len(ts) >= 2
has_enrichment = bool(enriched)
profiles = (enriched.get("profiles") if enriched else {}) or {}

# --- Helpers ---
def label_for(addr: str) -> str:
    return labels.get(addr, "")

def entity_for(addr: str) -> dict | None:
    return resolved_labels.get(addr)

def role_for(addr: str) -> tuple[str, str]:
    p = profiles.get(addr) or {}
    return (p.get("role") or "", p.get("role_confidence") or "")

def addr_display(addr: str, max_len: int = 16) -> str:
    lbl = label_for(addr)
    ent = entity_for(addr)
    short = addr[:max_len] + "..." if len(addr) > max_len else addr
    if ent:
        return f"{short} [{ent.get('entity_name')}]"
    if lbl:
        return f"{short} ({lbl})"
    return short

# --- Sidebar ---
st.sidebar.title("BSV Top 1000 Monitor")

if has_timeseries:
    min_date = ts.index.min().date()
    max_date = ts.index.max().date()
    st.sidebar.subheader("Filters")
    date_range = st.sidebar.date_input(
        "Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )
    abs_threshold = st.sidebar.slider("Big mover threshold (BSV)", 10, 10000, 100, step=10)
    pct_threshold = st.sidebar.slider("Big mover threshold (%)", 1.0, 50.0, 10.0, step=1.0)
else:
    abs_threshold = 100
    pct_threshold = 10.0

if has_data:
    snap_date, snap_addresses = snap
    st.sidebar.metric("Latest snapshot", snap_date)
    st.sidebar.metric("Snapshots collected", len(dates))

if has_enrichment:
    enriched_at = enriched.get("enriched_at", "")
    if enriched_at:
        st.sidebar.caption(f"Enriched at {enriched_at[:19]}Z")
    n_multi = (clusters_doc or {}).get("multi_member_cluster_count", 0)
    if n_multi:
        st.sidebar.caption(f"Clusters: {n_multi} multi-member")
    if resolved_labels:
        st.sidebar.caption(f"Attributed entities: {len(resolved_labels)} addrs")

st.sidebar.markdown("---")
st.sidebar.caption(
    "Roles describe behavior, not identity. Clusters are derived from "
    "the Nakamoto common-input heuristic. Entity names only appear when "
    "seeded in `data/entities.json`."
)

# --- Tabs ---
(tab_overview, tab_movers, tab_address, tab_classify, tab_clusters,
 tab_trends, tab_labels, tab_raw) = st.tabs([
    "Overview", "Big Movers", "Address Detail", "Classification", "Clusters",
    "Aggregate Trends", "Labels", "Raw Data",
])

# ===== TAB 1: OVERVIEW =====
with tab_overview:
    if not has_data:
        st.warning("No snapshots yet. Run `python collector.py` to fetch your first snapshot.")
        st.stop()

    st.header(f"Top 1000 BSV Addresses — {snap_date}")

    # KPIs
    balances = [a["balance"] for a in snap_addresses]
    total_bsv = sum(balances) / 1e8
    largest_bsv = max(balances) / 1e8
    median_bsv = float(np.median(balances)) / 1e8

    rate = enriched.get("bsv_usd") if enriched else None
    share = enriched.get("top1000_share_of_supply") if enriched else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total BSV (Top 1000)", f"{total_bsv:,.0f}")
    if rate:
        col2.metric("Total USD Value", f"${total_bsv * rate / 1e6:,.1f}M")
    else:
        col2.metric("Largest Balance", f"{largest_bsv:,.0f} BSV")
    if share is not None:
        col3.metric("Share of Supply", f"{share * 100:.2f}%")
    else:
        col3.metric("Median Balance", f"{median_bsv:,.0f} BSV")
    col4.metric("Addresses", len(snap_addresses))

    # Secondary KPI row — role distribution + price
    if has_enrichment:
        role_counts = enriched.get("role_counts") or {}
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("BSV/USD", f"${rate:,.2f}" if rate else "—")
        c2.metric("Cold storage", role_counts.get("cold_storage", 0))
        c3.metric("Hot wallet", role_counts.get("hot_wallet", 0))
        c4.metric("Active", role_counts.get("active", 0))
        c5.metric("Unknown", role_counts.get("unknown", 0))

    # Table
    table_data = []
    for i, a in enumerate(sorted(snap_addresses, key=lambda x: x["balance"], reverse=True)):
        addr = a.get("address") or a.get("scripthash", "?")
        role, confidence = role_for(addr)
        ent = entity_for(addr)
        table_data.append({
            "Rank": i + 1,
            "Address": addr,
            "Entity": ent.get("entity_name") if ent else "",
            "Label": label_for(addr),
            "Role": role,
            "Confidence": confidence,
            "Balance (BSV)": sat_to_bsv(a["balance"]),
            "Type": a.get("type", ""),
        })
    df_table = pd.DataFrame(table_data)

    # Add change columns if timeseries available
    if has_timeseries:
        changes_1d = compute_changes(ts, window=1)
        if not changes_1d.empty:
            change_map = dict(zip(changes_1d["address"], changes_1d["change_abs"]))
            df_table["Change 24h (BSV)"] = df_table["Address"].map(
                lambda a: sat_to_bsv(change_map.get(a, 0))
            )

        if len(ts) >= 8:
            changes_7d = compute_changes(ts, window=7)
            if not changes_7d.empty:
                change_map_7d = dict(zip(changes_7d["address"], changes_7d["change_abs"]))
                df_table["Change 7d (BSV)"] = df_table["Address"].map(
                    lambda a: sat_to_bsv(change_map_7d.get(a, 0))
                )

    st.dataframe(df_table, use_container_width=True, height=600)

    # Top 20 bar chart
    st.subheader("Top 20 by Balance")
    top20 = df_table.head(20).set_index("Address")["Balance (BSV)"]
    st.bar_chart(top20)

# ===== TAB 2: BIG MOVERS =====
with tab_movers:
    st.header("Big Movers")

    if not has_timeseries:
        st.info("Need at least 2 snapshots to detect movements. Collect more data.")
    else:
        window = st.radio("Time window", [1, 7, 30], format_func=lambda x: f"{x}d", horizontal=True)

        if len(ts) < window + 1:
            st.warning(f"Not enough data for {window}-day window. Have {len(ts)} snapshots.")
        else:
            changes = compute_changes(ts, window=window)
            movers = detect_big_movers(changes, abs_threshold, pct_threshold)

            if movers.empty:
                st.success("No big movers detected with current thresholds.")
            else:
                st.metric("Big movers found", len(movers))

                display = movers.copy()
                display["Label"] = display["address"].map(label_for)
                display["Balance Before (BSV)"] = display["balance_prev"] / 1e8
                display["Balance After (BSV)"] = display["balance_now"] / 1e8
                display["Change (BSV)"] = display["change_abs"] / 1e8
                display["Change (%)"] = display["change_pct"].round(2)
                display["Direction"] = display["change_abs"].apply(
                    lambda x: "Inflow" if x > 0 else "Outflow"
                )
                display["Rank"] = display["rank_now"]

                show_cols = ["address", "Label", "Rank", "Balance Before (BSV)",
                             "Balance After (BSV)", "Change (BSV)", "Change (%)", "Direction"]
                st.dataframe(
                    display[show_cols].reset_index(drop=True),
                    use_container_width=True,
                    height=500,
                )

                # Chart selected mover
                selected = st.selectbox(
                    "View history for mover",
                    movers["address"].tolist(),
                    format_func=lambda a: addr_display(a),
                )
                if selected:
                    hist = address_history(ts, selected)
                    if not hist.empty:
                        st.line_chart(hist.set_index("date")["balance_bsv"])

# ===== TAB 3: ADDRESS DETAIL =====
with tab_address:
    st.header("Address Detail")

    if not has_data:
        st.warning("No data available.")
    else:
        # Build address list from latest snapshot
        all_addrs = [a.get("address") or a.get("scripthash", "") for a in snap_addresses]
        all_addrs = [a for a in all_addrs if a]

        search = st.text_input("Search address (paste full or partial)")
        if search:
            matches = [a for a in all_addrs if search.lower() in a.lower()]
        else:
            matches = all_addrs[:20]

        if matches:
            selected_addr = st.selectbox(
                "Select address",
                matches,
                format_func=lambda a: addr_display(a, 32),
            )

            if selected_addr:
                # Current stats
                addr_data = next(
                    (a for a in snap_addresses
                     if (a.get("address") or a.get("scripthash")) == selected_addr),
                    None,
                )
                if addr_data:
                    bal_bsv = sat_to_bsv(addr_data["balance"])
                    rank = next(
                        (i + 1 for i, a in enumerate(
                            sorted(snap_addresses, key=lambda x: x["balance"], reverse=True)
                        ) if (a.get("address") or a.get("scripthash")) == selected_addr),
                        None,
                    )
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Current Balance", f"{bal_bsv:,.2f} BSV")
                    c2.metric("Current Rank", f"#{rank}")
                    if enriched.get("bsv_usd"):
                        c3.metric("USD Value", f"${bal_bsv * enriched['bsv_usd']:,.0f}")
                    else:
                        c3.metric("Type", addr_data.get("type", "unknown"))
                    role, confidence = role_for(selected_addr)
                    if role:
                        c4.metric("Behavioral role", f"{role}", delta=f"confidence: {confidence}",
                                  delta_color="off")
                    else:
                        c4.metric("Type", addr_data.get("type", "unknown"))

                    ent = entity_for(selected_addr)
                    if ent:
                        st.success(
                            f"Attributed entity: **{ent.get('entity_name')}** "
                            f"({ent.get('category')}, confidence={ent.get('confidence')}) — "
                            f"via {ent.get('via')}"
                        )
                    lbl = label_for(selected_addr)
                    if lbl:
                        st.info(f"Manual label: **{lbl}**")

                # Profile details
                prof = profiles.get(selected_addr)
                if prof:
                    st.subheader("Behavioral profile")
                    p1, p2, p3, p4 = st.columns(4)
                    utxos = prof.get("utxo_count_seen", 0)
                    more = " +" if prof.get("has_more_utxos") else ""
                    p1.metric("UTXOs (page 1)", f"{utxos}{more}")
                    na = prof.get("newest_utxo_age_days")
                    p2.metric("Newest UTXO age", f"{na:.1f} d" if na is not None else "—")
                    oa = prof.get("oldest_utxo_age_days")
                    p3.metric("Oldest UTXO age", f"{oa:.1f} d" if oa is not None else "—")
                    cv = prof.get("balance_cv")
                    p4.metric("Balance CV",
                              f"{cv:.3f}" if cv is not None else "—",
                              help="Coefficient of variation across snapshots "
                                   "(stddev / mean). Higher = more volatile.")

                # Cluster membership
                cid, cluster_members = cluster_for_address(clusters_doc, selected_addr)
                if cid and len(cluster_members) > 1:
                    st.subheader(f"Co-spend cluster: {cid}")
                    st.caption(
                        f"{len(cluster_members)} addresses have appeared as co-inputs in the "
                        "same transaction(s) — almost certainly controlled by the same entity "
                        "(Nakamoto common-input heuristic)."
                    )
                    cluster_df = pd.DataFrame(
                        [{"Address": a, "Same as selected?": a == selected_addr}
                         for a in cluster_members]
                    )
                    st.dataframe(cluster_df, use_container_width=True, hide_index=True)

                # Historical chart
                if has_timeseries:
                    hist = address_history(ts, selected_addr)
                    if not hist.empty:
                        st.subheader("Balance History")
                        st.line_chart(hist.set_index("date")["balance_bsv"])

                        c1, c2, c3 = st.columns(3)
                        c1.metric("All-Time High", f"{hist['balance_bsv'].max():,.2f} BSV")
                        c2.metric("All-Time Low", f"{hist['balance_bsv'].min():,.2f} BSV")
                        c3.metric("Days Tracked", len(hist))
                    else:
                        st.info("No historical data for this address yet.")

                # Counterparty tx investigation from latest enriched snapshot
                movers_by_addr = {m["address"]: m for m in (enriched.get("movers") or [])}
                mover_rec = movers_by_addr.get(selected_addr)
                if mover_rec and mover_rec.get("txs"):
                    st.subheader("Recent investigated transactions")
                    st.caption(
                        "Sourced from the latest enrichment run. Counterparty list is vout "
                        "addresses only — senders (vin side) are resolved during clustering."
                    )
                    for t in mover_rec["txs"][:5]:
                        if "error" in t:
                            st.warning(f"tx {t.get('txid','?')[:16]}... fetch error: {t['error']}")
                            continue
                        with st.expander(
                            f"tx {t['txid'][:16]}…  h={t['height']}  "
                            f"{t['direction']}  "
                            f"to us: {t['value_to_us_bsv']:.2f} BSV  "
                            f"to others: {t['value_to_others_bsv']:.2f} BSV"
                        ):
                            for c in t.get("counterparty_addresses") or []:
                                ent = entity_for(c)
                                role, _ = role_for(c)
                                suffix = []
                                if ent:
                                    suffix.append(f"entity: {ent.get('entity_name')}")
                                if role:
                                    suffix.append(f"role: {role}")
                                if c in profiles:
                                    suffix.append("top-1000")
                                extra = f" — {', '.join(suffix)}" if suffix else ""
                                st.code(c + extra, language=None)

                # Label editor
                st.subheader("Edit Label")
                new_label = st.text_input("Label", value=label_for(selected_addr), key="label_edit")
                if st.button("Save Label"):
                    labels[selected_addr] = new_label
                    save_labels(labels)
                    st.success(f"Label saved: {new_label}")
                    st.cache_data.clear()
        else:
            st.info("No matching addresses found.")

# ===== TAB: CLASSIFICATION =====
with tab_classify:
    st.header("Behavioral Classification")
    st.caption(
        "Each top-1000 address is tagged with a behavioral role based on its UTXO "
        "shape and balance history. Labels describe behavior, not identity — "
        "\"hot_wallet\" means the address behaves like one (many fragmented UTXOs, "
        "recent activity); it does **not** mean we know it's an exchange."
    )

    if not has_enrichment or not profiles:
        st.info("No enrichment data yet. Run `python enrich.py` to populate profiles.")
    else:
        role_counts = enriched.get("role_counts") or {}
        cols = st.columns(len(role_counts) or 1)
        for i, (role, count) in enumerate(sorted(role_counts.items(), key=lambda kv: -kv[1])):
            cols[i].metric(role, count)

        # Balance share per role
        role_bsv: dict[str, float] = {}
        for a in snap_addresses:
            addr = a.get("address") or a.get("scripthash")
            role, _ = role_for(addr)
            bsv = sat_to_bsv(a["balance"])
            role_bsv[role or "unknown"] = role_bsv.get(role or "unknown", 0) + bsv
        st.subheader("BSV held by role")
        st.bar_chart(pd.Series(role_bsv).sort_values(ascending=False))

        # Filterable table
        st.subheader("Browse addresses")
        role_options = ["(all)"] + sorted(role_counts.keys())
        selected_role = st.selectbox("Filter by role", role_options)

        rows = []
        for a in snap_addresses:
            addr = a.get("address") or a.get("scripthash")
            prof = profiles.get(addr) or {}
            if selected_role != "(all)" and prof.get("role") != selected_role:
                continue
            ent = entity_for(addr)
            rows.append({
                "Address": addr,
                "Balance (BSV)": sat_to_bsv(a["balance"]),
                "Role": prof.get("role") or "",
                "Confidence": prof.get("role_confidence") or "",
                "UTXOs (page 1)": prof.get("utxo_count_seen"),
                "Newest UTXO age (d)": prof.get("newest_utxo_age_days"),
                "Balance CV": prof.get("balance_cv"),
                "Entity": ent.get("entity_name") if ent else "",
            })
        rows.sort(key=lambda r: -r["Balance (BSV)"])
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=500,
                     hide_index=True)


# ===== TAB: CLUSTERS =====
with tab_clusters:
    st.header("Co-spend Clusters")
    st.caption(
        "Groups of addresses that have appeared as co-inputs in the same transaction. "
        "By the Nakamoto common-input heuristic, co-spenders almost certainly share "
        "one owner. Clusters are anonymous until seeds are added to `data/entities.json`."
    )

    clusters = clusters_doc.get("clusters") or {}
    multi = {cid: addrs for cid, addrs in clusters.items() if len(addrs) > 1}

    if not multi:
        st.info(
            "No multi-member clusters yet. The clustering process grows daily from "
            "processed mover transactions — more data means more clusters."
        )
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total addresses clustered", clusters_doc.get("address_count", 0))
        c2.metric("Clusters", clusters_doc.get("cluster_count", 0))
        c3.metric("Multi-member", len(multi))

        # Per-cluster summary with aggregated balance
        snap_bal = {
            (a.get("address") or a.get("scripthash")): a.get("balance", 0)
            for a in snap_addresses
        }
        rows = []
        for cid, addrs in multi.items():
            total_sats = sum(snap_bal.get(a, 0) for a in addrs)
            in_top = sum(1 for a in addrs if a in snap_bal)
            ent = entity_for(addrs[0]) if addrs else None
            rows.append({
                "Cluster": cid,
                "Members": len(addrs),
                "In top 1000": in_top,
                "Aggregate BSV (known)": sat_to_bsv(total_sats),
                "Entity": ent.get("entity_name") if ent else "",
            })
        rows.sort(key=lambda r: -r["Aggregate BSV (known)"])
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.subheader("Inspect cluster")
        pick = st.selectbox("Select cluster", sorted(multi.keys()))
        if pick:
            addrs = multi[pick]
            for a in addrs:
                bal = sat_to_bsv(snap_bal.get(a, 0))
                ent = entity_for(a)
                role, conf = role_for(a)
                note = []
                if a in snap_bal:
                    note.append(f"{bal:,.2f} BSV in top-1000")
                if role:
                    note.append(f"role={role}/{conf}")
                if ent:
                    note.append(f"entity={ent.get('entity_name')}")
                st.code(f"{a}  —  {', '.join(note) if note else 'not in current top-1000'}",
                        language=None)


# ===== TAB: AGGREGATE TRENDS =====
with tab_trends:
    st.header("Aggregate Trends")

    if has_enrichment and not ets.empty:
        ets_indexed = ets.set_index("date")
        if "total_top1000_usd" in ets_indexed.columns and ets_indexed["total_top1000_usd"].notna().any():
            st.subheader("Total USD value in Top 1000")
            st.line_chart(ets_indexed["total_top1000_usd"])
        if "top1000_share_of_supply" in ets_indexed.columns and ets_indexed["top1000_share_of_supply"].notna().any():
            st.subheader("Top-1000 share of circulating BSV supply")
            st.line_chart(ets_indexed["top1000_share_of_supply"])
            st.caption("Share trending up = concentration growing, trending down = dispersal.")
        if "bsv_usd" in ets_indexed.columns and ets_indexed["bsv_usd"].notna().any():
            st.subheader("BSV/USD rate (per enrichment)")
            st.line_chart(ets_indexed["bsv_usd"])

    if not has_timeseries:
        st.info("Need at least 2 snapshots for the BSV-denominated charts.")
    else:
        metrics = aggregate_metrics(ts)

        if not metrics.empty:
            metrics_indexed = metrics.set_index("date")

            st.subheader("Total BSV in Top 1000")
            st.line_chart(metrics_indexed["total_bsv"])

            st.subheader("Concentration (Herfindahl Index)")
            st.line_chart(metrics_indexed["herfindahl"])
            st.caption("Higher = more concentrated. 0.001 = evenly distributed, >0.01 = concentrated.")

            st.subheader("Address Count by Tier")
            tier_cols = ["count_10k", "count_100k", "count_1m"]
            tier_display = metrics_indexed[tier_cols].rename(columns={
                "count_10k": ">10k BSV",
                "count_100k": ">100k BSV",
                "count_1m": ">1M BSV",
            })
            st.line_chart(tier_display)

            st.subheader("Addresses in Top 1000")
            st.line_chart(metrics_indexed["address_count"])
        else:
            st.warning("Could not compute aggregate metrics.")

# ===== TAB 5: LABELS =====
with tab_labels:
    st.header("Known Address Labels")
    st.caption(
        "Edit labels inline, delete rows with the row menu, or add new rows at the bottom. "
        "Click **Save changes** to persist."
    )

    # Build editable table with current balances
    label_rows = []
    snap_by_addr = {}
    if has_data:
        snap_by_addr = {
            (a.get("address") or a.get("scripthash")): a for a in snap_addresses
        }
    for addr, lbl in labels.items():
        addr_data = snap_by_addr.get(addr)
        bal = sat_to_bsv(addr_data["balance"]) if addr_data else None
        label_rows.append({"Address": addr, "Label": lbl, "Balance (BSV)": bal})

    df_labels = pd.DataFrame(label_rows, columns=["Address", "Label", "Balance (BSV)"])

    edited = st.data_editor(
        df_labels,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "Address": st.column_config.TextColumn(required=True),
            "Label": st.column_config.TextColumn(required=True),
            "Balance (BSV)": st.column_config.NumberColumn(format="%.2f", disabled=True),
        },
        key="labels_editor",
    )

    if st.button("Save changes", type="primary"):
        new_labels: dict[str, str] = {}
        for _, row in edited.iterrows():
            addr = str(row["Address"]).strip() if pd.notna(row["Address"]) else ""
            lbl = str(row["Label"]).strip() if pd.notna(row["Label"]) else ""
            if addr and lbl:
                new_labels[addr] = lbl
        save_labels(new_labels)
        st.cache_data.clear()
        st.success(f"Saved {len(new_labels)} label(s).")
        st.rerun()

    if labels:
        st.subheader("Total BSV per Entity")
        grouped = (
            df_labels.groupby("Label")["Balance (BSV)"].sum().sort_values(ascending=False).dropna()
        )
        if not grouped.empty:
            st.bar_chart(grouped)

        csv = df_labels.to_csv(index=False)
        st.download_button("Download Labels CSV", csv, "labels.csv", "text/csv")
    else:
        st.info("No labels yet. Add rows in the table above.")

    st.caption(
        "ℹ️ On Streamlit Cloud, label edits are lost on every redeploy. "
        "For persistent labels, edit `data/labels.json` locally and commit to the repo."
    )

# ===== TAB 6: RAW DATA =====
with tab_raw:
    st.header("Raw Snapshot Data")

    if not dates:
        st.warning("No snapshots available.")
    else:
        selected_date = st.selectbox("Select snapshot date", dates, index=len(dates) - 1)
        snap_data = load_snapshot(selected_date)

        if snap_data:
            st.metric("Addresses in snapshot", snap_data["count"])
            st.metric("Fetched at", snap_data["fetched_at"])

            st.json(snap_data, expanded=False)

            # Downloads
            json_str = json.dumps(snap_data, indent=2)
            st.download_button(
                "Download Snapshot JSON",
                json_str,
                f"bsv_top1000_{selected_date}.json",
                "application/json",
            )

        if has_timeseries:
            st.subheader("Export Timeseries")
            csv_data = ts.reset_index().to_csv(index=False)
            st.download_button(
                "Download Timeseries CSV",
                csv_data,
                "bsv_top1000_timeseries.csv",
                "text/csv",
            )
