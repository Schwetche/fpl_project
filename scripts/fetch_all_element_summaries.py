# scripts/fetch_all_element_summaries.py
import json, time
from pathlib import Path
from datetime import datetime, timezone
import requests

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-Fetcher/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
ELEM_SUMMARY = "https://fantasy.premierleague.com/api/element-summary/{pid}/"

def fetch_json(url, timeout=30, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(1.0 * (i + 1))

def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out_dir = (Path(__file__).resolve().parent / f"../data/raw/{ts}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    print("? dossier:", out_dir)

    # 1) bootstrap pour r?cup?rer la liste des joueurs
    boot = fetch_json(BOOTSTRAP)
    (out_dir / "bootstrap-static.json").write_text(json.dumps(boot, ensure_ascii=False, indent=2), encoding="utf-8")

    elements = boot.get("elements", [])
    player_ids = [e["id"] for e in elements if "id" in e]
    print("joueurs d?tect?s:", len(player_ids))

    ok = err = 0
    for pid in player_ids:
        out_file = out_dir / f"element_summary_{pid}.json"
        if out_file.exists():
            continue
        try:
            data = fetch_json(ELEM_SUMMARY.format(pid=pid))
            out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            ok += 1
        except Exception as e:
            print(f"! player {pid} erreur: {e}")
            err += 1
        time.sleep(0.25)  # d?lai poli

    summary = {"players_total": len(player_ids), "downloaded_ok": ok, "errors": err}
    (out_dir / "_element_summary_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("OK termin?")
    print(summary)

if __name__ == "__main__":
    main()
