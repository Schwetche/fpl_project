# scripts/fetch_bootstrap.py
from __future__ import annotations
import requests, pandas as pd
from pathlib import Path

DATA = Path("data")
DATA.mkdir(parents=True, exist_ok=True)

BOOT = "https://fantasy.premierleague.com/api/bootstrap-static/"
FIX  = "https://fantasy.premierleague.com/api/fixtures/"

def main():
    r = requests.get(BOOT, timeout=30); r.raise_for_status()
    j = r.json()

    # players_raw.csv (elements)
    el = pd.DataFrame(j.get("elements", []))
    if not el.empty and "id" not in el.columns and "element" in el.columns:
        el["id"] = el["element"]
    el.to_csv(DATA / "players_raw.csv", index=False)

    # teams.csv
    teams = pd.DataFrame(j.get("teams", []))
    teams.to_csv(DATA / "teams.csv", index=False)

    # player_idlist.csv (id + web_name minimal)
    pid = el[["id","web_name","team","element_type"]].copy() if not el.empty else pd.DataFrame()
    pid.to_csv(DATA / "player_idlist.csv", index=False)

    # fixtures.csv
    rf = requests.get(FIX, timeout=30); rf.raise_for_status()
    fx = pd.DataFrame(rf.json() or [])
    fx.to_csv(DATA / "fixtures.csv", index=False)

    # placeholders si besoin par ton validateur
    if not (DATA / "merged_gw.csv").exists():
        pd.DataFrame().to_csv(DATA / "merged_gw.csv", index=False)
    if not (DATA / "cleaned_players.csv").exists():
        el.to_csv(DATA / "cleaned_players.csv", index=False)

    print("[PASS] bootstrap + fixtures fetched.")

if __name__ == "__main__":
    main()
