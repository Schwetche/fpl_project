# scripts/build_fixtures_csv.py
import os
import requests
import pandas as pd

BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
FIXTURES  = "https://fantasy.premierleague.com/api/fixtures/"

def main(out_path="../data/fixtures.csv"):
    # On prend les noms d'?quipes via bootstrap
    boot = requests.get(BOOTSTRAP, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).json()
    teams = pd.DataFrame(boot["teams"])[["id","name","short_name"]].rename(
        columns={"id":"team_id","name":"team_name","short_name":"team_short"}
    )

    # Tous les fixtures (y compris pass?s, blanks/doubles, etc.)
    fx = requests.get(FIXTURES, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).json()
    df = pd.json_normalize(fx)

    # Map noms ?quipes
    id2name  = teams.set_index("team_id")["team_name"].to_dict()
    id2short = teams.set_index("team_id")["team_short"].to_dict()
    df["team_h_name"] = df["team_h"].map(id2name)
    df["team_a_name"] = df["team_a"].map(id2name)
    df["team_h_short"] = df["team_h"].map(id2short)
    df["team_a_short"] = df["team_a"].map(id2short)

    # Colonnes principales (tu pourras en ajouter ensuite)
    keep = [
        "id","event","kickoff_time","finished","finished_provisional",
        "team_h","team_h_name","team_h_short","team_h_score","team_h_difficulty",
        "team_a","team_a_name","team_a_short","team_a_score","team_a_difficulty",
        "minutes","pulse_id"
    ]
    # garde celles qui existent
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    out_full = os.path.abspath(os.path.join(os.path.dirname(__file__), out_path))
    os.makedirs(os.path.dirname(out_full), exist_ok=True)
    df.to_csv(out_full, index=False, encoding="utf-8")
    print(f"OK fixtures.csv ?crit -> {out_full}  ({len(df)} lignes)")

if __name__ == "__main__":
    main()
