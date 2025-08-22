# scripts/fetch_bootstrap_and_update_players.py
# Rôle:
# - Récupère bootstrap-static
# - Écrit data/players_raw.csv avec:
#     • TOUTES les colonnes de elements[] (full dump)
#     • alias d'identifiants: element, player_id (= id)
#     • team_name, team_short (join teams)
#     • position (join element_types.singular_name_short -> GKP/DEF/MID/FWD)
#     • garanties: chance_of_*, news_added (créées à NaN si manquantes)
#     • price_m (now_cost / 10.0) — pratique pour d’autres checks
# - Met à jour data/teams.csv et data/player_idlist.csv
# - Toujours snapshot (data/snapshots)

from pathlib import Path
import requests, pandas as pd
from utils_io import ensure_dirs, write_current_and_snapshot

BASE_URL = "https://fantasy.premierleague.com/api/"

# Casts utiles (le reste reste tel quel)
INT_COLS = [
    "id","code","team","team_code","element_type",
    "now_cost",
    "cost_change_event","cost_change_event_fall",
    "cost_change_start","cost_change_start_fall",
    "total_points","event_points","minutes",
    "goals_scored","assists","clean_sheets","goals_conceded",
    "own_goals","penalties_saved","penalties_missed",
    "yellow_cards","red_cards","saves","bonus","bps",
    "transfers_in","transfers_out","transfers_in_event","transfers_out_event",
]
FLOAT_COLS = [
    "selected_by_percent",
    "influence","creativity","threat","ict_index",
    "form","value_form","value_season","points_per_game",
    "ep_next","ep_this",
    "chance_of_playing_next_round","chance_of_playing_this_round",
    "price_m",
]

# Champs à garantir (créés à NaN si absents)
GUARANTEE_COLS = [
    "first_name","second_name","web_name",
    "status","news","news_added",
    "chance_of_playing_next_round","chance_of_playing_this_round",
    "now_cost","cost_change_start","cost_change_event",
    "transfers_in_event","transfers_out_event","selected_by_percent",
    "team","element_type",
]

def fetch_json(path: str):
    headers = {"User-Agent": "FPL-ETL/1.5 (players_raw add position)"}
    r = requests.get(BASE_URL + path, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)

    boot = fetch_json("bootstrap-static/")
    elements = pd.DataFrame(boot["elements"])
    teams = pd.DataFrame(boot["teams"])
    etypes = pd.DataFrame(boot["element_types"])  # pour 'position'

    # === PLAYERS_RAW: full dump ===
    players_raw = elements.copy()

    # Garantir quelques champs sensibles (si l'API change)
    for c in GUARANTEE_COLS:
        if c not in players_raw.columns:
            players_raw[c] = pd.NA

    # Aliases d'identifiants
    players_raw["element"] = players_raw.get("id")
    players_raw["player_id"] = players_raw.get("id")

    # team_name / team_short
    team_map = teams[["id","name","short_name"]].rename(
        columns={"id":"team","name":"team_name","short_name":"team_short"}
    )
    players_raw = players_raw.merge(team_map, on="team", how="left")

    # position (GKP/DEF/MID/FWD) via element_types.singular_name_short
    # etypes: id (1..4), singular_name_short
    if not etypes.empty and {"id","singular_name_short"}.issubset(etypes.columns):
        pos_map = etypes[["id","singular_name_short"]].rename(
            columns={"id":"element_type","singular_name_short":"position"}
        )
        players_raw = players_raw.merge(pos_map, on="element_type", how="left")
    else:
        # au pire, crée la colonne vide (le validateur ne plantera pas)
        if "position" not in players_raw.columns:
            players_raw["position"] = pd.NA

    # price_m (lecture humaine / check annexe)
    if "now_cost" in players_raw.columns:
        players_raw["price_m"] = pd.to_numeric(players_raw["now_cost"], errors="coerce") / 10.0

    # Casts utiles
    for c in INT_COLS:
        if c in players_raw.columns:
            players_raw[c] = pd.to_numeric(players_raw[c], errors="coerce").astype("Int64")
    for c in FLOAT_COLS:
        if c in players_raw.columns:
            players_raw[c] = pd.to_numeric(players_raw[c], errors="coerce")

    # Ordonner lisiblement : identifiants / noms / position d’abord
    head = [col for col in [
        "id","element","player_id","code","team","team_code","team_name","team_short",
        "element_type","position","first_name","second_name","web_name",
        "status","news","news_added",
        "now_cost","price_m","cost_change_event","cost_change_event_fall",
        "cost_change_start","cost_change_start_fall",
        "selected_by_percent",
        "chance_of_playing_next_round","chance_of_playing_this_round",
        "transfers_in","transfers_out","transfers_in_event","transfers_out_event",
        "total_points","event_points","minutes",
    ] if col in players_raw.columns]
    tail = [c for c in players_raw.columns if c not in head]
    players_raw = players_raw[head + tail]

    write_current_and_snapshot(
        players_raw,
        current_path=(root / "data" / "players_raw.csv"),
        name_for_snapshot="players_raw"
    )
    print("[OK] data/players_raw.csv (full dump + aliases + team_name/short + position)")

    # === TEAMS ===
    team_keep = ["id","name","short_name","strength",
                 "strength_overall_home","strength_overall_away",
                 "strength_attack_home","strength_attack_away",
                 "strength_defence_home","strength_defence_away"]
    team_keep = [c for c in team_keep if c in teams.columns]
    teams_out = teams[team_keep].copy()
    write_current_and_snapshot(
        teams_out,
        current_path=(root / "data" / "teams.csv"),
        name_for_snapshot="teams"
    )
    print("[OK] data/teams.csv")

    # === PLAYER_IDLIST ===
    pid_cols = [c for c in ["id","web_name","first_name","second_name","team","element_type"] if c in elements.columns]
    pid = elements[pid_cols].copy()
    if "name" in teams.columns:
        pid = pid.merge(
            teams[["id","name"]].rename(columns={"id":"team_id","name":"team_name"}),
            left_on="team", right_on="team_id", how="left"
        ).drop(columns=["team_id"])
    write_current_and_snapshot(
        pid,
        current_path=(root / "data" / "player_idlist.csv"),
        name_for_snapshot="player_idlist"
    )
    print("[OK] data/player_idlist.csv")

if __name__ == "__main__":
    main()
