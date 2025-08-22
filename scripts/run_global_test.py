# scripts/run_global_test.py
"""
Validation globale du projet FPL.

Vérifie :
- Accessibilité des APIs FPL
- Présence des fichiers clés
- Cohérence de price_change_forecast.csv
- Contrôles de normalisation (colonnes clés + snapshots)
- Résumé global PASS/FAIL/WARN
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import requests
import sys

DATA = Path("data")
SNAP = DATA / "snapshots"
REQUIRED_PRICE_COLS = [
    "id","web_name","team","position","now_cost","ownership",
    "NTI_1h","NTI_24h","forecast","risk","eta_window"
]

# Compteurs globaux
COUNTS = {"PASS": 0, "FAIL": 0, "WARN": 0}

def log(status: str, msg: str):
    """Enregistre et affiche un message avec statut normalisé"""
    status = status.upper()
    if status not in COUNTS:
        raise ValueError(f"Statut inconnu: {status}")
    COUNTS[status] += 1
    print(f"[{status}] {msg}")

def check_api() -> bool:
    ok = True
    for url in [
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        "https://fantasy.premierleague.com/api/fixtures/",
    ]:
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            log("PASS", f"API reachable: {url}")
        except Exception as e:
            log("FAIL", f"API unreachable: {url} ({e})")
            ok = False
    return ok

def check_file_exists(path: Path) -> bool:
    if path.exists():
        log("PASS", f"File present: {path}")
        return True
    log("FAIL", f"File missing: {path}")
    return False

def check_price_forecast() -> bool:
    f = DATA / "price_change_forecast.csv"
    if not check_file_exists(f):
        return False
    df = pd.read_csv(f)
    ok = True

    # Vérifie colonnes essentielles
    for c in REQUIRED_PRICE_COLS:
        if c not in df.columns:
            log("FAIL", f"Missing column in price_change_forecast.csv: {c}")
            ok = False

    if not ok:
        return False

    # Vérifie cohérence forecasts
    if "NTI_24h" in df.columns and "forecast" in df.columns:
        big = df[df["NTI_24h"].abs() >= 300000]
        non_stable = df[df["forecast"] != "stable"]
        if len(big) > 0 and len(non_stable) == 0:
            log("WARN", "Detected big NTI_24h but no non-stable forecasts.")
        else:
            log("PASS", "Forecasts presence check OK.")
    return True

# --- Contrôles spécifiques de normalisation ---
def check_file_has_cols(path: Path, required: list[str]) -> bool:
    if not path.exists():
        log("FAIL", f"{path} manquant")
        return False
    try:
        cols = pd.read_csv(path, nrows=0).columns.tolist()
    except Exception as e:
        log("FAIL", f"{path} illisible: {e}")
        return False
    ok = True
    for col in required:
        if col not in cols:
            log("FAIL", f"{path.name} : colonne '{col}' absente")
            ok = False
    if ok:
        log("PASS", f"{path.name} contient {', '.join(required)}")
    return ok

def check_normalization() -> bool:
    ok = True
    ok &= check_file_has_cols(DATA / "players_raw.csv", ["element", "id"])
    ok &= check_file_has_cols(DATA / "fixtures.csv", ["id", "fixture"])
    ok &= check_file_has_cols(DATA / "season" / "gw1.csv", ["gw", "event"])

    if SNAP.exists():
        snaps = sorted(SNAP.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)[:5]
        if snaps:
            log("PASS", "Snapshots récents trouvés :")
            for s in snaps:
                print("   ", s.name, s.stat().st_mtime)
        else:
            log("FAIL", "Aucun snapshot trouvé dans data/snapshots/")
            ok = False
    else:
        log("FAIL", "Dossier snapshots absent")
        ok = False
    return ok

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--with-api-check", action="store_true", default=False)
    ap.add_argument("--with-fixtures-diff", action="store_true", default=False)  # placeholder
    args = ap.parse_args()

    overall_ok = True

    # API check
    if args.with_api_check:
        overall_ok &= check_api()

    # Fichiers de base
    core = [
        DATA / "players_raw.csv",
        DATA / "fixtures.csv",
        DATA / "merged_gw.csv",
        DATA / "cleaned_players.csv",
    ]
    for f in core:
        overall_ok &= check_file_exists(f)

    # Nouveaux modules
    overall_ok &= check_file_exists(DATA / "snapshots")
    overall_ok &= check_file_exists(DATA / "deltas")
    overall_ok &= check_file_exists(DATA / "deadlines.csv")
    overall_ok &= check_price_forecast()

    # Contrôles de normalisation
    overall_ok &= check_normalization()

    # Résumé final
    print("\n=== GLOBAL SUMMARY ===")
    print(f"PASS : {COUNTS['PASS']}")
    print(f"WARN : {COUNTS['WARN']}")
    print(f"FAIL : {COUNTS['FAIL']}")
    if overall_ok and COUNTS["FAIL"] == 0:
        print("RESULT: PASS ✅")
    else:
        print("RESULT: FAIL ❌")
        sys.exit(1)

if __name__ == "__main__":
    main()
