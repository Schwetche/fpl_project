# scripts/build_all_gw_csvs.py
import os
import time
import requests
import pandas as pd
from pathlib import Path

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-Totals/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
EVENT_LIVE = "https://fantasy.premierleague.com/api/event/{gw}/live/"

def build_gw_total(gw: int, players: pd.DataFrame, teams: pd.DataFrame, out_dir: Path) -> bool:
    """Construit gw{gw}.csv (totaux par joueur pour la GW).
    Retourne True si un fichier a ?t? ?crit, False sinon (GW future / vide)."""
    # 1) stats live pour la GW
    live = requests.get(EVENT_LIVE.format(gw=gw), headers=UA, timeout=30).json()
    rows = []
    for el in live.get("elements", []):
        pid = el.get("id")
        stats = el.get("stats", {}) or {}
        if pid is None:
            continue
        rows.append({"player_id": pid, "gw": gw, **stats})
    df = pd.DataFrame(rows)
    if df.empty:
        print(f"~ GW{gw}: pas de lignes (probablement future/non jou?e) - on saute.")
        return False

    # 2) enrichissement : joueurs + ?quipes + position
    pos_map = {1:"GK", 2:"DEF", 3:"MID", 4:"FWD"}
    players = players.copy()
    players["position"] = players["element_type"].map(pos_map)
    df = df.merge(players.rename(columns={"id":"player_id"}),
                  on="player_id", how="left")
    df = df.merge(teams, on="team", how="left")

    # 3) colonnes principales (garde celles pr?sentes)
    wanted = [
        "gw","player_id","web_name","first_name","second_name","team_name","position",
        "minutes","goals_scored","assists","clean_sheets","goals_conceded",
        "own_goals","saves","penalties_saved","penalties_missed",
        "yellow_cards","red_cards","bonus","bps",
        "influence","creativity","threat","ict_index",
        "expected_goals","expected_assists","expected_goal_involvements","expected_goals_conceded",
        "total_points"
    ]
    df = df[[c for c in wanted if c in df.columns]]

    # 4) ?criture
    out_file = out_dir / f"gw{gw}.csv"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False, encoding="utf-8")
    print(f"OK gw{gw}.csv -> {out_file} ({len(df)} lignes)")
    return True

def main(out_dir="../data/season"):
    # Pr?pare sorties
    base = Path(__file__).resolve().parent.joinpath(out_dir).resolve()
    base.mkdir(parents=True, exist_ok=True)

    # R?cup?re dictionnaires depuis bootstrap
    boot = requests.get(BOOTSTRAP, headers=UA, timeout=30).json()
    events  = boot.get("events", [])
    players = pd.DataFrame(boot["elements"])[["id","web_name","first_name","second_name","team","element_type"]]
    teams   = pd.DataFrame(boot["teams"])
