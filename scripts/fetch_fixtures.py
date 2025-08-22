# scripts/fetch_fixtures.py
import json
from pathlib import Path
from datetime import datetime, timezone
import requests

URL = "https://fantasy.premierleague.com/api/fixtures/"
UA  = {"User-Agent": "Mozilla/5.0 (compatible; FPL-Fetcher/1.0)"}

def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out_dir = (Path(__file__).resolve().parent / f"../data/raw/{ts}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    r = requests.get(URL, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()

    out_file = out_dir / "fixtures.json"
    out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("OK sauvegard? :", out_file)

if __name__ == "__main__":
    main()
