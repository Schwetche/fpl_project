# scripts/fetch_one_element_summary.py
import json, time
from pathlib import Path
from datetime import datetime, timezone
import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-Fetcher/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
ELEM_SUMMARY = "https://fantasy.premierleague.com/api/element-summary/{pid}/"

def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out_dir = (Path(__file__).resolve().parent / f"../data/raw/{ts}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # choisis un joueur depuis bootstrap (le premier de la liste)
    boot = requests.get(BOOTSTRAP, headers=UA, timeout=30).json()
    elements = boot.get("elements", [])
    if not elements:
        raise SystemExit("Aucun joueur trouv? dans bootstrap-static.")
    pid = elements[0]["id"]

    url = ELEM_SUMMARY.format(pid=pid)
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()

    out_file = out_dir / f"element_summary_{pid}.json"
    out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK sauvegard? : {out_file}  (player_id={pid})")

if __name__ == "__main__":
    main()
