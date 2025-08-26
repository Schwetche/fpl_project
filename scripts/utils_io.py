# scripts/utils_io.py
"""
Utilitaires I/O du projet FPL avec logique Always-Write.
Contient:
- ensure_dirs, timestamp, now_local
- always_write_csv (fichier courant + snapshot horodaté)
- write_current_and_snapshot (compat anciens scripts)
- list_snapshots, latest_two_snapshots
- read_csv_safe, to_float_safe
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime
import re
import pandas as pd

# --- Horodatage / timezone ---
def now_local() -> datetime:
    # Laisse Windows/OS gérer le fuseau local (Zurich) côté planifications;
    # pour les noms de fichiers on n'a pas besoin de tz-aware ici.
    return datetime.now()

def timestamp_iso_for_filename(dt: datetime | None = None) -> str:
    if dt is None:
        dt = now_local()
    # nom de fichier safe Windows: 2025-08-22T11-00
    return dt.strftime("%Y-%m-%dT%H-%M")

# --- FS helpers ---
def ensure_dirs(*dirs: str | Path) -> None:
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)

# --- Always-Write v2 (utilisée par les nouveaux scripts) ---
def always_write_csv(
    df: pd.DataFrame,
    current_path: str | Path,
    snapshots_dir: str | Path,
    base_filename: str,
    ts: datetime | None = None,
) -> Path:
    """Écrit le fichier courant (overwrite) + un snapshot horodaté; retourne le chemin du snapshot."""
    ensure_dirs(Path(current_path).parent, snapshots_dir)
    df.to_csv(current_path, index=False, encoding="utf-8")
    stamp = timestamp_iso_for_filename(ts)
    snap_path = Path(snapshots_dir) / f"{Path(base_filename).stem}_{stamp}.csv"
    df.to_csv(snap_path, index=False, encoding="utf-8")
    return snap_path

# --- Always-Write v1 (compat anciens scripts normaliseur) ---
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SNAPSHOTS = DATA / "snapshots"
ensure_dirs(SNAPSHOTS)

def write_current_and_snapshot(df: pd.DataFrame, current_path: Path, name_for_snapshot: str):
    """Compat: réécrit le fichier courant + snapshot daté (YYYYMMDD_HHMMSS)."""
    df.to_csv(current_path, index=False, encoding="utf-8")
    ts = now_local().strftime("%Y%m%d_%H%M%S")
    snap_name = f"{name_for_snapshot}_{ts}.csv"
    snap_path = SNAPSHOTS / snap_name
    df.to_csv(snap_path, index=False, encoding="utf-8")
    print(f"[WRITE] {current_path.name} réécrit + snapshot {snap_name}")

# --- Parcours des snapshots ---
def list_snapshots(snapshots_dir: str | Path, stem: str) -> list[Path]:
    p = Path(snapshots_dir)
    if not p.exists():
        return []
    patt = re.compile(rf"^{re.escape(stem)}_\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}-\d{{2}}\.csv$")
    files = [f for f in p.glob("*.csv") if patt.match(f.name)]
    files.sort()
    return files

def latest_two_snapshots(snapshots_dir: str | Path, stem: str) -> tuple[Path | None, Path | None]:
    files = list_snapshots(snapshots_dir, stem)
    if len(files) == 0:
        return None, None
    if len(files) == 1:
        return files[-1], None
    return files[-1], files[-2]

# --- Lecture / conversions sûres ---
def read_csv_safe(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)

def to_float_safe(x):
    try:
        return float(str(x).replace("%", "").strip())
    except Exception:
        return None

# --- write_gw_and_snapshot (robuste & compatible) ---
import os
from pathlib import Path
from datetime import datetime, timezone

def write_gw_and_snapshot(*args, **kwargs):
    """
    Compatible avec 2 styles d'appel :

    A) write_gw_and_snapshot(df, gw, out_dir="../data/season", prefix="gw")

    B) write_gw_and_snapshot(base_path, df, gw, stem="gw")
       - base_path peut être la racine du projet OU directement .../data/season
    """
    # Détection de signature
    if len(args) >= 3 and isinstance(args[0], (str, os.PathLike, Path)):
        # Style B
        base = Path(args[0]).resolve()
        df   = args[1]
        gw   = int(args[2])
        prefix = kwargs.get("stem", kwargs.get("prefix", "gw"))

        # Déterminer project_root et season_dir en fonction de "base"
        if base.name == "season" and base.parent.name == "data":
            season_dir   = base
            project_root = season_dir.parent.parent
        elif (base / "scripts").exists() or (base / "data").exists():
            # base = racine projet
            project_root = base
            season_dir   = project_root / "data" / "season"
        else:
            # fallback: considérer base comme season_dir
            season_dir   = base
            # si base = .../data/season -> parent.parent = racine projet
            project_root = season_dir.parent.parent
    else:
        # Style A
        df   = args[0]
        gw   = int(args[1])
        out_dir = kwargs.get("out_dir", "../data/season")
        prefix  = kwargs.get("prefix", "gw")
        project_root = Path(__file__).resolve().parent.parent
        season_dir   = (project_root / out_dir).resolve()

    # Créer dossiers
    season_dir.mkdir(parents=True, exist_ok=True)
    current_path = season_dir / f"{prefix}{gw}.csv"

    snapshots_dir = project_root / "data" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snapshot_path = snapshots_dir / f"{prefix}{gw}_{ts}.csv"

    # Écritures
    df.to_csv(current_path, index=False)
    print(f"[WROTE] current  -> {current_path}")
    df.to_csv(snapshot_path, index=False)
    print(f"[WROTE] snapshot -> {snapshot_path}")
# --- fin ---
