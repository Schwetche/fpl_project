# scripts/fetch_one_event_live.py
import json, time
from pathlib import Path
from datetime import datetime, timezone
import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-Fetcher/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
EVENT_LIVE = "https://fantasy.premierleague.com/api/event/{gw}/live/"

def pick_one_gw(events):
    # priorit? : derni?re GW pass?e, sinon courante, sinon la premi?re de la liste
    prev = [e["id"] for e in events if e.get("is_previous")]
    if prev:
        return sorted(prev)[-1]
    cur = [e["id"] for e in events if e.get("is_current")]
    if cur:
        return cur[0]
    ids = sorted([e["id"] for e in events if "id" in e])
    return ids[0] if ids else None

def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out_dir = (Path(__file__).resolve().parent / f"../data/raw/{ts}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    boot = requests.get(BOOTSTRAP, headers=UA, timeout=30).json()
    events = boot.get("events", [])
    gw = pick_one_gw(events)
    if gw is None:
        raise SystemExit("Aucune GW trouv?e dans bootstrap-static.")

    url = EVENT_LIVE.format(gw=gw)
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()

    out_file = out_dir / f"event_{gw}_live.json"
    out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK sauvegard? : {out_file}  (gw={gw})")

if __name__ == "__main__":
    main()
