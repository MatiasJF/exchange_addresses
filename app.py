#!/usr/bin/env python3
"""Streamlit dashboard for BSV top 1000 address monitoring."""

import streamlit as st
import pandas as pd
import numpy as np
import json

from analysis import (
    load_timeseries, load_labels, save_labels, latest_snapshot,
    snapshot_dates, load_snapshot, compute_changes, detect_big_movers,
    aggregate_metrics, address_history, sat_to_bsv,
)

st.set_page_config(page_title="BSV Top 1000 Monitor", layout="wide")

# --- Auth gate ---
APP_PASSWORD = "bsv2026"

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("BSV Top 1000 Address Monitor")
    pwd = st.text_input("Password", type="password")
    if pwd == APP_PASSWORD:
        st.session_state.authenticated = True
        st.rerun()
    elif pwd:
        st.error("Incorrect password")
    st.stop()

# --- Data loading ---
@st.cache_data
def load_data():
    ts = load_timeseries()
    labels = load_labels()
    snap = latest_snapshot()
    dates = snapshot_dates()
    return ts, labels, snap, dates

ts, labels, snap, dates = load_data()

has_data = snap is not None
has_timeseries = not ts.empty and len(ts) >= 2

# --- Helper ---
def label_for(addr: str) -> str:
    return labels.get(addr, "")

def addr_display(addr: str, max_len: int = 16) -> str:
    lbl = label_for(addr)
    short = addr[:max_len] + "..." if len(addr) > max_len else addr
    return f"{short} ({lbl})" if lbl else short

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

# --- Tabs ---
tab_overview, tab_movers, tab_address, tab_trends, tab_labels, tab_raw = st.tabs(
    ["Overview", "Big Movers", "Address Detail", "Aggregate Trends", "Labels", "Raw Data"]
)

# ===== TAB 1: OVERVIEW =====
with tab_overview:
    if not has_data:
        st.warning("No snapshots yet. Run `python collector.py` to fetch your first snapshot.")
        st.stop()

    st.header(f"Top 1000 BSV Addresses — {snap_date}")

    # KPIs
    total_bsv = sum(a["balance"] for a in snap_addresses) / 1e8
    largest = max(snap_addresses, key=lambda a: a["balance"])
    largest_bsv = largest["balance"] / 1e8
    median_bsv = sorted(a["balance"] for a in snap_addresses)[500] / 1e8

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total BSV (Top 1000)", f"{total_bsv:,.0f}")
    col2.metric("Largest Balance", f"{largest_bsv:,.0f} BSV")
    col3.metric("Median Balance", f"{median_bsv:,.0f} BSV")
    col4.metric("Addresses", len(snap_addresses))

    # Table
    table_data = []
    for i, a in enumerate(sorted(snap_addresses, key=lambda x: x["balance"], reverse=True)):
        addr = a.get("address") or a.get("scripthash", "?")
        table_data.append({
            "Rank": i + 1,
            "Address": addr,
            "Label": label_for(addr),
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
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Current Balance", f"{bal_bsv:,.2f} BSV")
                    c2.metric("Current Rank", f"#{rank}")
                    c3.metric("Type", addr_data.get("type", "unknown"))

                    lbl = label_for(selected_addr)
                    if lbl:
                        st.info(f"Label: **{lbl}**")

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

# ===== TAB 4: AGGREGATE TRENDS =====
with tab_trends:
    st.header("Aggregate Trends")

    if not has_timeseries:
        st.info("Need at least 2 snapshots to show trends.")
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

    if labels:
        # Build label table with current balances
        label_rows = []
        for addr, lbl in labels.items():
            addr_data = next(
                (a for a in (snap_addresses if has_data else [])
                 if (a.get("address") or a.get("scripthash")) == addr),
                None,
            )
            bal = sat_to_bsv(addr_data["balance"]) if addr_data else None
            label_rows.append({"Address": addr, "Label": lbl, "Balance (BSV)": bal})

        df_labels = pd.DataFrame(label_rows)
        st.dataframe(df_labels, use_container_width=True)

        # Grouped by entity
        st.subheader("Total BSV per Entity")
        grouped = df_labels.groupby("Label")["Balance (BSV)"].sum().sort_values(ascending=False)
        grouped = grouped.dropna()
        if not grouped.empty:
            st.bar_chart(grouped)

        # Export
        csv = df_labels.to_csv(index=False)
        st.download_button("Download Labels CSV", csv, "labels.csv", "text/csv")
    else:
        st.info("No labels yet. Add labels from the Address Detail tab.")

    # Add new label
    st.subheader("Add Label")
    with st.form("add_label"):
        new_addr = st.text_input("Address")
        new_lbl = st.text_input("Entity Name")
        submitted = st.form_submit_button("Add")
        if submitted and new_addr and new_lbl:
            labels[new_addr] = new_lbl
            save_labels(labels)
            st.success(f"Added: {new_addr[:20]}... = {new_lbl}")
            st.cache_data.clear()

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
