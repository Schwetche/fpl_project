# scripts/build_one_gw_csv.py
# Construit data/season/gw{GW}.csv (par joueur) depuis l'API /event/{gw}/live
# Always Write + snapshot

import argparse, requests, pandas as pd
from pathlib import Path
import sys

# === Import robuste du package 'scripts' même si on lance depuis .\scripts ===
ROOT = Path(__file__).resolve().parent.parent  # dossier projet (fpl_project)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ============================================================================

from scripts.utils_io import ensure_dirs, write_gw_and_snapshot

UA = {"User-Agent": "Mozilla/5.0 (FPL-GWBuild/1.1)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
EVENT_LIVE = "https://fantasy.premierleague.com/api/event/{gw}/live/"

def fetch_bootstrap():
    r = requests.get(BOOTSTRAP, headers=UA, timeout=30); r.raise_for_status()
    return r.json()

def fetch_event_live(gw: int):
    r = requests.get(EVENT_LIVE.format(gw=gw), headers=UA, timeout=30); r.raise_for_status()
    return r.json()

def build_gw_per_player(gw: int) -> pd.DataFrame:
    boot = fetch_bootstrap()
    live = fetch_event_live(gw)

    elems_boot = pd.DataFrame(boot.get("elements", []))  # mapping id -> noms, team, element_type
    teams = pd.DataFrame(boot.get("teams", []))
    id2name = teams.set_index("id")["name"].to_dict() if not teams.empty else {}

    el = pd.DataFrame(live.get("elements", []))  # [{"id": <player_id>, "stats": {...}}]
    if el.empty:
        raise RuntimeError(f"Aucune donnée live pour GW{gw}")

    # IMPORTANT: normaliser la Series de dicts via .tolist()
    stats = pd.json_normalize(el["stats"].tolist())
    stats.columns = [c.replace(".", "_") for c in stats.columns]
    stats.insert(0, "element", el["id"].values)

    df = stats
    df["gw"] = gw

    if not elems_boot.empty:
        base = elems_boot[["id","web_name","team","element_type"]].rename(columns={"id":"element"})
        df = df.merge(base, on="element", how="left")
        df["team_name"] = df["team"].map(id2name)
        pos_map = {1:"GK",2:"DEF",3:"MID",4:"FWD"}
        df["position"] = df["element_type"].map(pos_map)

    order = ["element","web_name","team_name","position","gw","minutes","total_points"]
    cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
    return df[cols]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gw", type=int, required=True)
    args = p.parse_args()

    base = ROOT
    ensure_dirs(base)
    df = build_gw_per_player(args.gw)
    write_gw_and_snapshot(base, df, args.gw, stem="gw")

if __name__ == "__main__":
    main()
