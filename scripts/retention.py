# scripts/retention.py
"""
Purge de rétention pour le repo FPL.

- Fonctionne en "dry-run" (par défaut) ou "force" (suppression effective).
- Politiques par défaut sûres et configurables via variables d'environnement
  ou paramètres de workflow:
    • data/snapshots/*.csv : keep_last=200, max_age_days=120
    • data/deltas/*.csv    : keep_last=120, max_age_days=180

- Exclusions de sécurité (jamais supprimés):
  players_raw.csv, players_raw_history.csv, price_change_forecast.csv,
  deadlines.csv, merged_gw.csv, merged_gw_permatch.csv, cleaned_players.csv,
  teams.csv, fixtures.csv, player_idlist.csv
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

CRITICAL_FILES = {
    "data/players_raw.csv",
    "data/players_raw_history.csv",
    "data/price_change_forecast.csv",
    "data/deadlines.csv",
    "data/merged_gw.csv",
    "data/merged_gw_permatch.csv",
    "data/cleaned_players.csv",
    "data/teams.csv",
    "data/fixtures.csv",
    "data/player_idlist.csv",
}

DEFAULT_POLICIES = [
    {
        "glob": "data/snapshots/*.csv",
        "keep_last": int(os.getenv("KEEP_LAST_SNAPSHOTS", "200")),
        "max_age_days": int(os.getenv("MAX_AGE_SNAPSHOTS", "120")),
    },
    {
        "glob": "data/deltas/*.csv",
        "keep_last": int(os.getenv("KEEP_LAST_DELTAS", "120")),
        "max_age_days": int(os.getenv("MAX_AGE_DELTAS", "180")),
    },
]

def list_sorted_by_mtime(paths: list[Path]) -> list[Path]:
    return sorted(paths, key=lambda p: p.stat().st_mtime, reverse=True)

def apply_policy(glob_pattern: str, keep_last: int, max_age_days: int, root: Path) -> tuple[list[Path], list[Path]]:
    now = datetime.now(timezone.utc)
    files = [p for p in root.glob(glob_pattern) if p.is_file()]
    files_sorted = list_sorted_by_mtime(files)

    keep_set = set(files_sorted[: max(keep_last, 0)])

    cutoff = now - timedelta(days=max(0, max_age_days))
    to_delete: list[Path] = []
    for p in files_sorted[max(keep_last, 0):]:
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        except FileNotFoundError:
            continue
        if mtime < cutoff:
            to_delete.append(p)

    to_keep = [p for p in files_sorted if p not in to_delete]
    return to_keep, to_delete

def is_critical(p: Path) -> bool:
    rel = p.as_posix()
    return any(rel.endswith(cf) for cf in CRITICAL_FILES)

def main():
    parser = argparse.ArgumentParser(description="FPL retention (purge) with dry-run/force.")
    parser.add_argument("--root", default=".", help="Racine du repo (par défaut: .)")
    parser.add_argument("--dry-run", action="store_true", help="Ne supprime rien, affiche seulement (défaut).")
    parser.add_argument("--force", action="store_true", help="Supprime réellement les fichiers ciblés.")
    parser.add_argument("--verbose", action="store_true", help="Logs détaillés.")
    args = parser.parse_args()

    dry_run = args.dry_run or not args.force
    root = Path(args.root).resolve()

    print(f"[INFO] Root = {root}")
    print(f"[INFO] Mode = {'DRY-RUN' if dry_run else 'FORCE (DELETE)'}")

    deleted_total: list[Path] = []
    kept_total: list[Path] = []

    for pol in DEFAULT_POLICIES:
        glob_pattern = pol["glob"]
        keep_last = int(pol["keep_last"])
        max_age_days = int(pol["max_age_days"])

        to_keep, to_delete = apply_policy(glob_pattern, keep_last, max_age_days, root)
        to_delete_safe = [p for p in to_delete if not is_critical(p)]

        if args.verbose:
            print(f"\n[POLICY] {glob_pattern} | keep_last={keep_last} | max_age_days={max_age_days}")
            print(f"         found={len(to_keep) + len(to_delete)} keep={len(to_keep)} delete(candidates)={len(to_delete)}")

        for p in to_delete_safe:
            if dry_run:
                print(f"[DRY] would delete: {p.relative_to(root)}")
            else:
                try:
                    p.unlink()
                    print(f"[DEL] {p.relative_to(root)}")
                except Exception as e:
                    print(f"[WARN] failed to delete {p}: {e}")

        kept_total.extend(to_keep)
        deleted_total.extend(to_delete_safe)

    print("\n=== RETENTION SUMMARY ===")
    print(f"Kept   : {len(kept_total)} files (after policy)")
    print(f"Deleted: {len(deleted_total)} files ({'simulé' if dry_run else 'effectué'})")
    print("=========================")

    # ✅ Message repérable en un coup d'œil dans les logs
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n[OK] Retention executed at {ts} | Mode={'DRY-RUN' if dry_run else 'FORCE'} | Deleted={len(deleted_total)}\n")

if __name__ == "__main__":
    main()
