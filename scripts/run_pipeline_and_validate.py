# scripts/run_pipeline_and_validate.py
"""
Pipeline principal du projet FPL.

Fonctionne en 2 modes :
- Sans argument → pipeline complet (normalisation, cleaned, snapshot, deltas, deadlines, forecast, validation).
- Avec --snapshot et/ou --deltas → exécute uniquement snapshot et/ou deltas (utile pour run_snapshot.ps1).
"""

from __future__ import annotations
import subprocess
import sys
import argparse
from pathlib import Path

PY = sys.executable
ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

def run(cmd: list[str], cwd: Path | None = None):
    """Lance un sous-script Python et arrête si échec."""
    print(f"[RUN] {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=cwd or ROOT)
    if res.returncode != 0:
        raise SystemExit(f"[FAIL] {' '.join(cmd)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", action="store_true", help="Exécuter uniquement snapshot")
    ap.add_argument("--deltas", action="store_true", help="Exécuter uniquement NTI deltas")
    args, _ = ap.parse_known_args()

    # === Mode snapshot/deltas (utilisé par run_snapshot.ps1) ===
    if args.snapshot or args.deltas:
        if args.snapshot:
            run([PY, str(SCRIPTS / "snapshot_players_raw.py")])
        if args.deltas:
            run([PY, str(SCRIPTS / "calc_nti_deltas.py")])
        # Forecast toujours après snapshot+deltas
        run([PY, str(SCRIPTS / "price_change_forecast.py")])
        # Pas de validation globale ici → gérée par run_snapshot.ps1
        return

    # === Pipeline complet ===
    # 1) Extraction initiale (si script dédié)
    # run([PY, str(SCRIPTS / "extract_players_and_fixtures.py")])

    # 2) Normalisation
    try:
        run([PY, str(SCRIPTS / "normalize_columns.py")])
    except FileNotFoundError:
        print("[WARN] normalize_columns.py missing — skipping")

    # 3) Cleaned players
    try:
        run([PY, str(SCRIPTS / "build_cleaned_players.py")])
    except FileNotFoundError:
        print("[WARN] build_cleaned_players.py missing — skipping")

    # 4) Snapshot
    run([PY, str(SCRIPTS / "snapshot_players_raw.py")])

    # 5) NTI deltas
    run([PY, str(SCRIPTS / "calc_nti_deltas.py")])

    # 6) Deadlines
    run([PY, str(SCRIPTS / "update_deadlines.py")])

    # 7) Price Change Forecast
    run([PY, str(SCRIPTS / "price_change_forecast.py")])

    # 8) Validation globale
    run([PY, str(SCRIPTS / "run_global_test.py"), "--with-api-check", "--with-fixtures-diff"])

    print("[PASS] Pipeline finished.")

if __name__ == "__main__":
    main()
