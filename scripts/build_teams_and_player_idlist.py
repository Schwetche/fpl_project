# scripts/build_teams_and_player_idlist.py
import os
import requests
import pandas as pd

BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"

def main():
    r = requests.get(BOOTSTRAP, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    data = r.json()

    # ----- TEAMS -----
    teams = pd.DataFrame(data["teams"])
    # On ?crit TOUTES les colonnes disponibles pour maximiser l'info
    out_teams = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/teams.csv"))
    os.makedirs(os.path.dirname(out_teams), exist_ok=True)
    teams.to_csv(out_teams, index=False, encoding="utf-8")

    # ----- PLAYER_IDLIST -----
    elements = pd.DataFrame(data["elements"])

    # Ajoute position lisible + team_name
    pos_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    elements["position"] = elements["element_type"].map(pos_map)
    id_to_team = teams.set_index("id")["name"].to_dict()
    elements["team_name"] = elements["team"].map(id_to_team)

    # Conserve les colonnes d'identit? principales
    pid = elements[[
        "id", "web_name", "first_name", "second_name",
        "team", "team_name", "element_type", "position"
    ]].copy()

    out_pid = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/player_idlist.csv"))
    pid.to_csv(out_pid, index=False, encoding="utf-8")

    print(f"OK teams.csv -> {out_teams}  ({len(teams)} ?quipes)")
    print(f"OK player_idlist.csv -> {out_pid}  ({len(pid)} joueurs)")

if __name__ == "__main__":
    main()
