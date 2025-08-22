# scripts/build_all_gw_permatch_csvs.py
import time
import requests
from pathlib import Path

# S?curise l'import si on ex?cute depuis la racine du projet
try:
    from build_one_gw_permatch_csv import build_one_gw_permatch
except ImportError:
    import sys, os
    sys.path.append(os.path.dirname(__file__))
    from build_one_gw_permatch_csv import build_one_gw_permatch

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-PerMatch/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"

def main(out_dir="../data/season"):
    # Liste de toutes les GWs via bootstrap
    boot = requests.get(BOOTSTRAP, headers=UA, timeout=30).json()
    events = boot.get("events", [])
    gw_ids = sorted(e["id"] for e in events if "id" in e)

    base = Path(__file__).resolve().parent.joinpath(out_dir).resolve()
    base.mkdir(parents=True, exist_ok=True)

    ok = err = skipped = 0
    for gw in gw_ids:
        out_file = base / f"gw{gw}_permatch.csv"
        if out_file.exists():
            print(f"- d?j? pr?sent, on saute: {out_file.name}")
            skipped += 1
            continue
        try:
            build_one_gw_permatch(gw=gw, out_dir=out_dir)
            # si le fichier n'a pas ?t? cr?? (GW future), on compte comme 'skipped'
            if out_file.exists():
                ok += 1
            else:
                print(f"~ GW{gw} sans fichier (probablement future/non jou?e) - saut?.")
                skipped += 1
        except BaseException as e:
            print(f"! GW {gw} erreur: {e}")
            err += 1
        time.sleep(0.10)  # petit d?lai poli

    print(f"OK termin? - nouveaux fichiers: {ok}, ignor?s: {skipped}, erreurs: {err}")

if __name__ == "__main__":
    main()
