# scripts/fetch_all_events_live.py
import json, time
from pathlib import Path
from datetime import datetime, timezone
import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-Fetcher/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
EVENT_LIVE = "https://fantasy.premierleague.com/api/event/{gw}/live/"

def fetch_json(url, timeout=30):
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return r.json()

def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out_dir = (Path(__file__).resolve().parent / f"../data/raw/{ts}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # liste des GWs depuis bootstrap
    boot = fetch_json(BOOTSTRAP)
    events = boot.get("events", [])
    gw_ids = [e["id"] for e in events if "id" in e]
    if not gw_ids:
        raise SystemExit("Aucune GW trouv?e.")

    ok, err = 0, 0
    for gw in sorted(gw_ids):
        out_file = out_dir / f"event_{gw}_live.json"
        if out_file.exists():
            continue
        try:
            data = fetch_json(EVENT_LIVE.format(gw=gw))
            out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            ok += 1
        except Exception as e:
            print(f"! GW {gw} erreur: {e}")
            err += 1
        time.sleep(0.10)  # petit d?lai poli

    # r?sum? simple
    summary = {"events_total": len(gw_ids), "downloaded_ok": ok, "errors": err}
    (out_dir / "_events_live_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("OK termin? ->", out_dir)
    print(summary)

if __name__ == "__main__":
    main()
