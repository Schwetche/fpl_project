# scripts/update_deadlines.py
from __future__ import annotations
import requests
import pandas as pd
from pathlib import Path
from utils_io import ensure_dirs

DATA_DIR = Path("data")
DEADLINES_CSV = DATA_DIR / "deadlines.csv"
API = "https://fantasy.premierleague.com/api/bootstrap-static/"

def main():
    ensure_dirs(DATA_DIR)
    r = requests.get(API, timeout=30)
    r.raise_for_status()
    j = r.json()
    events = j.get("events", [])
    out = []
    for e in events:
        out.append({
            "id": e.get("id"),
            "name": e.get("name"),
            "deadline_time": e.get("deadline_time"),
            "is_current": e.get("is_current"),
            "is_next": e.get("is_next"),
            "is_previous": e.get("is_previous"),
        })
    pd.DataFrame(out).to_csv(DEADLINES_CSV, index=False)
    print("[PASS] deadlines.csv updated.")

if __name__ == "__main__":
    main()
