# scripts/refresh_fixtures.py
# Rôle:
# - Récupère fixtures/
# - Ajoute 'started' (depuis l'API, pas déduit)
# - Écrit TOUJOURS data/fixtures.csv + snapshot

from pathlib import Path
import requests, pandas as pd
from utils_io import ensure_dirs, write_current_and_snapshot

BASE_URL = "https://fantasy.premierleague.com/api/"

def fetch_json(path: str):
    headers = {"User-Agent": "FPL-ETL/1.0 (schema-align)"}
    r = requests.get(BASE_URL + path, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)

    fixtures = pd.DataFrame(fetch_json("fixtures/"))
    keep = ["id","event","kickoff_time","team_h","team_a",
            "team_h_difficulty","team_a_difficulty",
            "finished","finished_provisional","started","minutes"]
    keep = [c for c in keep if c in fixtures.columns]
    out = fixtures[keep].copy()

    for c in ["id","team_h","team_a","team_h_difficulty","team_a_difficulty","minutes"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    if "event" in out.columns:
        out["event"] = pd.to_numeric(out["event"], errors="coerce").astype("Int64")
    for c in ["finished","finished_provisional","started"]:
        if c in out.columns:
            out[c] = out[c].astype("boolean")

    write_current_and_snapshot(
        out,
        current_path=(root / "data" / "fixtures.csv"),
        name_for_snapshot="fixtures"
    )
    print("[OK] data/fixtures.csv (inclut 'started')")

if __name__ == "__main__":
    main()
