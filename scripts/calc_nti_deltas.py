# scripts/calc_nti_deltas.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd
from utils_io import ensure_dirs, latest_two_snapshots, read_csv_safe

DATA_DIR = Path("data")
SNAP_DIR = DATA_DIR / "snapshots"
DELTAS_DIR = DATA_DIR / "deltas"
NTI_LOG = DELTAS_DIR / "nti_deltas.csv"

PLAYERS_STEM = "players_raw"

def _read_with_ts(path: Path) -> tuple[pd.DataFrame, datetime]:
    stamp = path.stem.split("_")[-1]
    ts = datetime.strptime(stamp, "%Y-%m-%dT%H-%M")
    df = pd.read_csv(path)
    return df, ts

def main():
    ensure_dirs(DELTAS_DIR)
    latest, prev = latest_two_snapshots(SNAP_DIR, PLAYERS_STEM)
    if latest is None:
        raise SystemExit("[WARN] No snapshot found yet.")
    if prev is None:
        df_latest, ts_latest = _read_with_ts(latest)
        for col in ["transfers_in_event", "transfers_out_event"]:
            if col not in df_latest.columns:
                df_latest[col] = 0
        df_latest["NTI"] = df_latest["transfers_in_event"] - df_latest["transfers_out_event"]
        out = pd.DataFrame({
            "timestamp": ts_latest,
            "id": df_latest["id"],
            "web_name": df_latest.get("web_name", ""),
            "NTI": df_latest["NTI"],
            "NTI_1h": 0,
            "NTI_24h": df_latest["NTI"],
        })
        out.to_csv(NTI_LOG, index=False)
        print("[PASS] Initialized NTI log.")
        return

    df_L, ts_L = _read_with_ts(latest)
    df_P, ts_P = _read_with_ts(prev)

    for col in ["transfers_in_event", "transfers_out_event"]:
        if col not in df_L.columns: df_L[col] = 0
        if col not in df_P.columns: df_P[col] = 0

    df_L["NTI"] = df_L["transfers_in_event"] - df_L["transfers_out_event"]
    df_P["NTI"] = df_P["transfers_in_event"] - df_P["transfers_out_event"]

    merged = pd.merge(
        df_L[["id", "web_name", "NTI"]],
        df_P[["id", "NTI"]].rename(columns={"NTI": "NTI_prev"}),
        on="id",
        how="left",
    )
    merged["NTI_prev"] = merged["NTI_prev"].fillna(0)
    merged["NTI_1h"] = merged["NTI"] - merged["NTI_prev"]

    log = read_csv_safe(NTI_LOG)
    cur = pd.DataFrame({
        "timestamp": ts_L,
        "id": merged["id"],
        "web_name": merged["web_name"],
        "NTI": merged["NTI"],
        "NTI_1h": merged["NTI_1h"],
        "NTI_24h": merged["NTI"],
    })
    log = pd.concat([log, cur], ignore_index=True)

    log["timestamp"] = pd.to_datetime(log["timestamp"])
    log.sort_values(["id", "timestamp"], inplace=True)

    nti24_values = []
    for pid, grp in log.groupby("id", sort=False):
        vals = []
        for _, row in grp.iterrows():
            window_start = row["timestamp"] - pd.Timedelta(hours=24)
            mask = (grp["timestamp"] > window_start) & (grp["timestamp"] <= row["timestamp"])
            nti24 = grp.loc[mask, "NTI_1h"].sum(skipna=True)
            vals.append(nti24)
        nti24_values.extend(vals)

    log["NTI_24h"] = nti24_values
    log.to_csv(NTI_LOG, index=False)
    print("[PASS] NTI deltas updated.")

if __name__ == "__main__":
    main()
