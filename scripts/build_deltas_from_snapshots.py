# scripts/build_deltas_from_snapshots.py
import os, glob
from pathlib import Path
import pandas as pd

SNAP_DIR = "../data/snapshots"
OUT_DIR  = "../data/deltas"

DELTA_COLS = [
    "price_m",
    "transfers_in_between_gws_current",
    "transfers_out_between_gws_current",
    "selected_by_percent",
]

ID_COLS = [
    "id","web_name","first_name","second_name","team_name","team_short","position",
    "current_gw","current_gw_deadline_utc","last_updated_utc"
]

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def main():
    base = Path(__file__).resolve().parent
    snap_dir = (base / SNAP_DIR).resolve()
    out_dir  = (base / OUT_DIR).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(glob.glob(os.path.join(snap_dir, "players_raw_*.csv")))
    if len(files) < 2:
        print("WARN - need at least 2 snapshots. Run snapshot_players_raw.py again later.")
        return

    f_prev, f_curr = files[-2], files[-1]
    df_prev = pd.read_csv(f_prev)
    df_curr = pd.read_csv(f_curr)

    keep_prev = [c for c in ID_COLS + DELTA_COLS if c in df_prev.columns]
    keep_curr = [c for c in ID_COLS + DELTA_COLS if c in df_curr.columns]

    df_prev = df_prev[keep_prev].copy()
    df_curr = df_curr[keep_curr].copy()

    for c in DELTA_COLS:
        if c in df_prev.columns: df_prev[c] = to_num(df_prev[c])
        if c in df_curr.columns: df_curr[c] = to_num(df_curr[c])

    df_prev = df_prev.rename(columns={"id":"player_id"})
    df_curr = df_curr.rename(columns={"id":"player_id"})

    merged = df_curr.merge(
        df_prev[["player_id"] + [c for c in DELTA_COLS if c in df_prev.columns]],
        on="player_id", how="left", suffixes=("", "_prev")
    )

    for c in DELTA_COLS:
        if c in merged.columns and f"{c}_prev" in merged.columns:
            merged[f"delta_{c}"] = merged[c] - merged[f"{c}_prev"]

    sort_col = "delta_transfers_in_between_gws_current" if "delta_transfers_in_between_gws_current" in merged.columns else None
    if sort_col:
        merged = merged.sort_values(sort_col, ascending=False)

    ts_curr = os.path.basename(f_curr).replace("players_raw_","").replace(".csv","")
    ts_prev = os.path.basename(f_prev).replace("players_raw_","").replace(".csv","")
    out_file = out_dir / f"deltas_{ts_prev}_to_{ts_curr}.csv"
    merged.to_csv(out_file, index=False, encoding="utf-8")

    print("OK - deltas written: {}".format(out_file))
    print("Compared snapshots: {} -> {}".format(os.path.basename(f_prev), os.path.basename(f_curr)))
    if sort_col:
        cols = [c for c in ["player_id","web_name","team_name","position", sort_col] if c in merged.columns]
        print("Top 10 by {}:".format(sort_col))
        print(merged[cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
