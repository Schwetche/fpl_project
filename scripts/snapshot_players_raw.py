# scripts/snapshot_players_raw.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
from utils_io import ensure_dirs, always_write_csv, now_local

DATA_DIR = Path("data")
SNAP_DIR = DATA_DIR / "snapshots"

PLAYERS_RAW = DATA_DIR / "players_raw.csv"
PLAYERS_STEM = "players_raw"

REQUIRED_COLS = [
    "element", "id", "web_name", "team", "element_type", "now_cost",
    "cost_change_event", "cost_change_start", "transfers_in_event",
    "transfers_out_event", "selected_by_percent", "status",
    "chance_of_playing_this_round", "news", "minutes", "total_points",
    "expected_goals", "expected_assists", "expected_goal_involvements",
]

def main():
    ensure_dirs(DATA_DIR, SNAP_DIR)
    if not PLAYERS_RAW.exists():
        raise SystemExit(f"[ERROR] {PLAYERS_RAW} not found. Run your extraction first.")

    df = pd.read_csv(PLAYERS_RAW)

    # Ensure id column exists (duplicate element)
    if "id" not in df.columns and "element" in df.columns:
        df["id"] = df["element"].astype("Int64")

    # Keep superset: if some columns missing live, keep what exists
    keep = [c for c in REQUIRED_COLS if c in df.columns]
    df = df[keep].copy()

    # Always-Write snapshot + petit "courant" pour consultation rapide
    ts = now_local()
    always_write_csv(
        df=df,
        current_path=DATA_DIR / "players_raw_snapshot_current.csv",
        snapshots_dir=SNAP_DIR,
        base_filename=PLAYERS_STEM,
        ts=ts,
    )
    print("[PASS] Snapshot saved.")

if __name__ == "__main__":
    main()
