# scripts/build_players_raw_min.py
import os
import requests
import pandas as pd

BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"

def main():
    r = requests.get(BOOTSTRAP, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    data = r.json()

    elements = pd.DataFrame(data["elements"])
    teams = (
        pd.DataFrame(data["teams"])[["id","name","short_name"]]
        .rename(columns={"id":"team","name":"team_name","short_name":"team_short"})
    )

    # Colonnes minimales pour valider le pipeline
    keep = ["id","web_name","team","element_type","now_cost","transfers_in_event","transfers_out_event"]
    df = elements[keep].copy()

    # Join ?quipes + prix lisible
    df = df.merge(teams, on="team", how="left")
    df["price_m"] = df["now_cost"] / 10.0

    # ?criture du CSV
    out_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/players_raw.csv"))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print("OK ->", out_path, f"({len(df)} lignes)")

if __name__ == "__main__":
    main()
