# scripts/enrich_gw.py
# Rôle:
# - Enrichit data/season/gwX.csv avec: fixture, kickoff_time (UTC), opponent_team, name
# - Source principale: gwX_permatch.csv
# - Fallback kickoff_time: fixtures.csv
# - Nettoyage systématique des colonnes suffixées (fixture/opponent_team/kickoff_time)
# - Écrit TOUJOURS un courant + snapshot via write_gw_and_snapshot

import argparse
from pathlib import Path
import pandas as pd
from utils_io import ensure_dirs, write_gw_and_snapshot

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Introuvable: {path}")
    return pd.read_csv(path, encoding="utf-8")

def unify_element(df: pd.DataFrame) -> pd.DataFrame:
    if "element" in df.columns:
        return df
    for cand in ("player_id", "id"):
        if cand in df.columns:
            out = df.copy()
            out["element"] = out[cand]
            return out
    raise KeyError("Aucune colonne d'identifiant joueur (element/player_id/id).")

def prefer_full_name(row) -> str | None:
    fn = str(row.get("first_name") or "").strip()
    sn = str(row.get("second_name") or "").strip()
    if fn and sn:
        return f"{fn} {sn}"
    wn = str(row.get("web_name") or "").strip()
    return wn or None

def coalesce_columns(df: pd.DataFrame, base: str) -> pd.DataFrame:
    """
    Unifie 'base' parmi les variantes: base, base__pm, base_map, base_x, base_y, base_from_fix.
    Garde une seule colonne 'base' au final.
    """
    variants = [base, f"{base}__pm", f"{base}_map", f"{base}_x", f"{base}_y", f"{base}_from_fix"]
    present = [c for c in variants if c in df.columns]
    if not present:
        return df
    if base not in df.columns:
        df[base] = pd.NA
    for c in present:
        if c == base:
            continue
        df[base] = df[base].fillna(df[c])
    # drop toutes les variantes sauf la finale
    for c in present:
        if c != base and c in df.columns:
            df.drop(columns=[c], inplace=True, errors="ignore")
    return df

def main():
    parser = argparse.ArgumentParser(description="Enrichit gwX.csv avec fixture/kickoff_time/opponent_team/name")
    parser.add_argument("--gw", type=int, required=True)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)

    season_dir = root / "data" / "season"
    fixtures_path = root / "data" / "fixtures.csv"
    pid_path = root / "data" / "player_idlist.csv"

    # Localiser gwX.csv & gwX_permatch.csv (priorité data/season, fallback racine)
    gw_path = season_dir / f"gw{args.gw}.csv"
    if not gw_path.exists():
        gw_path = root / f"gw{args.gw}.csv"
    pm_path = season_dir / f"gw{args.gw}_permatch.csv"
    if not pm_path.exists():
        pm_path = root / f"gw{args.gw}_permatch.csv"

    gw = unify_element(load_csv(gw_path))
    pm = unify_element(load_csv(pm_path))

    # Nettoyage préalable dans les 2 DataFrames (si fichiers héritent d'anciennes merges)
    for base in ["fixture", "opponent_team", "kickoff_time"]:
        gw = coalesce_columns(gw, base)
        pm = coalesce_columns(pm, base)

    need_pm = ["element","fixture","opponent_team"]
    missing = [c for c in need_pm if c not in pm.columns]
    if missing:
        raise KeyError(f"Colonnes manquantes dans gw{args.gw}_permatch.csv: {missing}")

    # Jointure: si 'fixture' déjà dans gw, joindre sur (element, fixture), sinon sur element
    join_cols = ["element","fixture"] if "fixture" in gw.columns else ["element"]

    pm_for_merge = pm.copy()
    if join_cols == ["element"]:
        # éviter la duplication (double GW) en gardant la 1re occurrence
        pm_for_merge = pm_for_merge.sort_values(["element","fixture"]).drop_duplicates("element", keep="first")

    cols_inject = ["element","fixture","opponent_team"] + (["kickoff_time"] if "kickoff_time" in pm_for_merge.columns else [])

    # IMPORTANT: utiliser un suffixe sûr pour éviter '..._x' déjà existants dans le CSV
    gw2 = gw.merge(pm_for_merge[cols_inject], on=join_cols, how="left", suffixes=("", "__pm"))

    # Coalesce après merge (supprime *_x, *_y, *_map, *__pm, *_from_fix)
    for base in ["fixture", "opponent_team", "kickoff_time"]:
        gw2 = coalesce_columns(gw2, base)

    # Compléter kickoff_time via fixtures.csv si manquant
    if ("kickoff_time" not in gw2.columns) or (gw2["kickoff_time"].isna().any()):
        if fixtures_path.exists():
            fixtures = load_csv(fixtures_path)
            if {"id","kickoff_time"}.issubset(fixtures.columns):
                fix_mini = fixtures[["id","kickoff_time"]].rename(columns={"id":"fixture"})
                gw2 = gw2.merge(fix_mini, on="fixture", how="left", suffixes=("", "_from_fix"))
                gw2 = coalesce_columns(gw2, "kickoff_time")

    # name : full name si possible, sinon web_name
    if "name" not in gw2.columns or gw2["name"].isna().all():
        if pid_path.exists():
            pid = load_csv(pid_path)[["id","web_name","first_name","second_name"]].rename(columns={"id":"element"})
            gw2 = gw2.merge(pid, on="element", how="left")
            gw2["name"] = gw2.apply(prefer_full_name, axis=1)
            gw2.drop(columns=["first_name","second_name","web_name"], inplace=True, errors="ignore")
        elif "web_name" in gw2.columns and "name" not in gw2.columns:
            gw2["name"] = gw2["web_name"]
        elif "name" not in gw2.columns:
            gw2["name"] = pd.NA

    # Types (nullable autorisé)
    for c in ["element","fixture","opponent_team","minutes"]:
        if c in gw2.columns:
            gw2[c] = pd.to_numeric(gw2[c], errors="coerce").astype("Int64")

    # Écriture (courant: data/season/gwX.csv + snapshot: data/snapshots/gwX_*.csv)
    write_gw_and_snapshot(root, gw2, args.gw, stem="gw")
    print(f"[OK] gw{args.gw}.csv enrichi (fixture, kickoff_time, opponent_team, name)")

if __name__ == "__main__":
    main()
