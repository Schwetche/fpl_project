# scripts/build_players_raw.py
import os
from datetime import datetime, timezone
import requests
import pandas as pd

BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"

def build_players_raw(out_path="../data/players_raw.csv"):
    # 1) Appel API (ajout d'un User-Agent pour ?viter tout blocage occasionnel)
    r = requests.get(BOOTSTRAP, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    data = r.json()

    # 2) Tables principales
    elements = pd.DataFrame(data["elements"])  # joueurs
    teams = (
        pd.DataFrame(data["teams"])[["id","name","short_name"]]
        .rename(columns={"id":"team","name":"team_name","short_name":"team_short"})
    )
    events = pd.DataFrame(data["events"])

    # 3) Position lisible
    pos_map = {1:"GK", 2:"DEF", 3:"MID", 4:"FWD"}
    elements["position"] = elements["element_type"].map(pos_map)

    # 4) Colonnes utiles (tu peux en ajouter/enlever si besoin)
    keep = [
        "id","web_name","first_name","second_name",
        "team","element_type","position",
        "now_cost","cost_change_event","cost_change_start",
        "transfers_in_event","transfers_out_event",   # ? transferts "entre-deux" (depuis derni?re deadline)
        "transfers_in","transfers_out",               # cumul saison
        "selected_by_percent","status",
        "chance_of_playing_next_round","chance_of_playing_this_round"
    ]
    df = elements[keep].copy()

    # 5) Joins/d?riv?es
    df = df.merge(teams, on="team", how="left")
    df["price_m"] = df["now_cost"] / 10.0
    df["delta_price_this_gw"] = df["cost_change_event"] / 10.0
    df["delta_price_since_start"] = df["cost_change_start"] / 10.0

    # Renommer explicitement les transferts "entre-deux"
    df = df.rename(columns={
        "transfers_in_event":  "transfers_in_between_gws_current",
        "transfers_out_event": "transfers_out_between_gws_current"
    })

    # 6) GW courante et deadline (si hors-saison, bascule sur la prochaine)
    current = events[events["is_current"] == True]
    if not current.empty:
        df["current_gw"] = int(current.iloc[0]["id"])
        df["current_gw_deadline_utc"] = current.iloc[0]["deadline_time"]
    else:
        nxt = events[events["is_next"] == True]
        if not nxt.empty:
            df["current_gw"] = int(nxt.iloc[0]["id"])
            df["current_gw_deadline_utc"] = nxt.iloc[0]["deadline_time"]

    # 7) Timestamp de g?n?ration
    df["last_updated_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 8) Ordre des colonnes (lisibles d'abord)
    first = [
        "id","web_name","first_name","second_name","position","team_name","team_short",
        "price_m","delta_price_this_gw","delta_price_since_start",
        "transfers_in_between_gws_current","transfers_out_between_gws_current",
        "transfers_in","transfers_out","selected_by_percent","status",
        "chance_of_playing_next_round","chance_of_playing_this_round",
        "current_gw","current_gw_deadline_utc","last_updated_utc"
    ]
    ordered = [c for c in first if c in df.columns] + [c for c in df.columns if c not in first]
    df = df[ordered]

    # 9) ?criture
    out_full = os.path.abspath(os.path.join(os.path.dirname(__file__), out_path))
    os.makedirs(os.path.dirname(out_full), exist_ok=True)
    df.to_csv(out_full, index=False, encoding="utf-8")
    print(f"OK players_raw.csv g?n?r? : {out_full}  ({len(df)} joueurs)")

if __name__ == "__main__":
    build_players_raw()
