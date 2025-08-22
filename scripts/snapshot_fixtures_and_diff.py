# scripts/snapshot_fixtures_and_diff.py
# 1) télécharge les fixtures actuels
# 2) écrit un snapshot CSV data/fixtures_snapshots/fixtures_<ts>.csv
# 3) compare au snapshot précédent et produit:
#    - un CSV des différences data/fixtures_diffs/diff_<prev>_to_<curr>.csv
#    - un ALERT texte data/ALERT_fixtures_diff_<ts>.txt si changements

import os, glob
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

UA = {"User-Agent": "Mozilla/5.0 (FPL-FixturesDiff/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
FIXTURES  = "https://fantasy.premierleague.com/api/fixtures/"

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "data"
SNAP_DIR = DATA / "fixtures_snapshots"
DIFF_DIR = DATA / "fixtures_diffs"

def fetch_json(url):
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.json()

def get_team_map():
    try:
        boot = fetch_json(BOOTSTRAP)
        teams = boot.get("teams", [])
        # id -> short_name
        return {t.get("id"): t.get("short_name") or t.get("name") for t in teams if t.get("id") is not None}
    except Exception:
        return {}

def load_fixtures_df():
    fx = fetch_json(FIXTURES)
    df = pd.DataFrame(fx)
    # garder colonnes utiles
    keep = ["id","event","kickoff_time","team_h","team_a","team_h_score","team_a_score","finished","minutes","provisional_start_time"]
    for c in keep:
        if c not in df.columns:
            df[c] = None
    df = df[keep].copy()
    # map teams
    id2short = get_team_map()
    df["team_h_name"] = df["team_h"].map(id2short)
    df["team_a_name"] = df["team_a"].map(id2short)
    return df

def write_snapshot(df):
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    fp = SNAP_DIR / f"fixtures_{ts}.csv"
    df.to_csv(fp, index=False, encoding="utf-8")
    return fp

def find_last_two_snapshots():
    files = sorted(glob.glob(str(SNAP_DIR / "fixtures_*.csv")))
    if len(files) < 2:
        return None, None
    return Path(files[-2]), Path(files[-1])

def compare_snapshots(prev_fp, curr_fp):
    prev = pd.read_csv(prev_fp)
    curr = pd.read_csv(curr_fp)

    key = "id"
    prev_ids = set(prev[key].dropna().astype(int))
    curr_ids = set(curr[key].dropna().astype(int))

    added_ids   = sorted(curr_ids - prev_ids)
    removed_ids = sorted(prev_ids - curr_ids)

    # align on common ids
    common_ids = sorted(prev_ids & curr_ids)
    p = prev.set_index(key).loc[common_ids]
    c = curr.set_index(key).loc[common_ids]

    # compare selected columns
    cols = ["event","kickoff_time","team_h","team_a","team_h_name","team_a_name","provisional_start_time"]
    diffs = []
    for fid in common_ids:
        changed = []
        row = {"id": fid}
        for col in cols:
            pv = p.at[fid, col] if col in p.columns else None
            cv = c.at[fid, col] if col in c.columns else None
            if str(pv) != str(cv):
                changed.append(col)
                row[col+"_prev"] = pv
                row[col+"_curr"] = cv
        if changed:
            row["changed_fields"] = ",".join(changed)
            diffs.append(row)

    df_diff = pd.DataFrame(diffs, columns=["id"] + [f"{col}_prev" for col in cols] + [f"{col}_curr" for col in cols] + ["changed_fields"])
    return added_ids, removed_ids, df_diff

def write_diff_outputs(prev_fp, curr_fp, added, removed, df_diff):
    DIFF_DIR.mkdir(parents=True, exist_ok=True)
    ts_prev = prev_fp.stem.replace("fixtures_", "")
    ts_curr = curr_fp.stem.replace("fixtures_", "")
    out_csv = DIFF_DIR / f"diff_{ts_prev}_to_{ts_curr}.csv"

    # écrire CSV des changements (si vide, écrire un CSV vide avec en-tête)
    df_diff.to_csv(out_csv, index=False, encoding="utf-8")

    # écrire ALERT si quelque chose a changé
    if added or removed or not df_diff.empty:
        alert = DATA / f"ALERT_fixtures_diff_{ts_curr}.txt"
        lines = []
        lines.append("Fixtures diff summary")
        lines.append(f"Prev snapshot: {prev_fp.name}")
        lines.append(f"Curr snapshot: {curr_fp.name}")
        lines.append(f"Added fixtures: {len(added)}")
        lines.append(f"Removed fixtures: {len(removed)}")
        lines.append(f"Changed fixtures: {len(df_diff)}")
        if added:
            lines.append("First 10 added ids: " + ", ".join(map(str, added[:10])))
        if removed:
            lines.append("First 10 removed ids: " + ", ".join(map(str, removed[:10])))
        if not df_diff.empty:
            lines.append("First 5 changed rows:")
            sample_cols = ["id","changed_fields","event_prev","event_curr","kickoff_time_prev","kickoff_time_curr","team_h_name_prev","team_h_name_curr","team_a_name_prev","team_a_name_curr"]
            sample_cols = [c for c in sample_cols if c in df_diff.columns]
            lines.append(df_diff[sample_cols].head(5).to_string(index=False))
        alert.write_text("\n".join(lines), encoding="utf-8")
        print(f"ALERT written: {alert}")
    else:
        print("No fixture changes vs previous snapshot.")

    print(f"Diff CSV: {out_csv}")

def main():
    df = load_fixtures_df()
    snap_fp = write_snapshot(df)
    print(f"Snapshot written: {snap_fp}")

    prev_fp, curr_fp = find_last_two_snapshots()
    if not prev_fp or not curr_fp:
        print("Need at least 2 snapshots to compute diff. Run again later.")
        return

    added, removed, df_diff = compare_snapshots(prev_fp, curr_fp)
    write_diff_outputs(prev_fp, curr_fp, added, removed, df_diff)

if __name__ == "__main__":
    main()
