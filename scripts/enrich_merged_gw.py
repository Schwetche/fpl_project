# scripts/enrich_merged_gw.py
# Rôle:
# - Enrichit data/merged_gw.csv avec: fixture, kickoff_time (UTC), opponent_team, name
# - Parcourt tous les gw*_permatch.csv (data/season puis racine)
# - Jointure sur (element, gw/round/event) si dispo, sinon fallback
# - Unifie toujours les colonnes (coalesce) pour éviter fixture_x/fixture_y/_map
# - Écrit TOUJOURS courant + snapshot via write_current_and_snapshot

from pathlib import Path
import re, glob
import pandas as pd
from utils_io import ensure_dirs, write_current_and_snapshot

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Introuvable: {path}")
    return pd.read_csv(path, encoding="utf-8")

def unify_element(df: pd.DataFrame) -> pd.DataFrame:
    if "element" in df.columns:
        return df
    for cand in ("player_id","id"):
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

def parse_gw_from_filename(fn: str):
    m = re.search(r"gw(\d+)_permatch\.csv$", fn)
    return int(m.group(1)) if m else None

def coalesce_columns(df: pd.DataFrame, base: str) -> pd.DataFrame:
    """Unifie base parmi [base, base_map, base_x, base_y] en une seule colonne 'base'."""
    candidates = [c for c in (base, f"{base}_map", f"{base}_x", f"{base}_y") if c in df.columns]
    if not candidates:
        return df
    if base not in df.columns:
        df[base] = pd.NA
    for cand in candidates:
        if cand == base:
            continue
        df[base] = df[base].fillna(df[cand])
    # supprimer colonnes candidates sauf la finale
    for cand in candidates:
        if cand != base and cand in df.columns:
            df.drop(columns=[cand], inplace=True, errors="ignore")
    return df

def main():
    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)

    merged_path = root / "data" / "merged_gw.csv"
    fixtures_path = root / "data" / "fixtures.csv"
    pid_path = root / "data" / "player_idlist.csv"

    merged = unify_element(load_csv(merged_path))

    # Détecter la colonne GW si présente
    gw_col = next((c for c in ["gw","round","event"] if c in merged.columns), None)

    # Collecter gw*_permatch.csv (data/season d'abord, puis racine)
    pm_files = sorted(glob.glob(str(root / "data" / "season" / "gw*_permatch.csv")))
    if not pm_files:
        pm_files = sorted(glob.glob(str(root / "gw*_permatch.csv")))
    if not pm_files:
        raise FileNotFoundError("Aucun gw*_permatch.csv trouvé (data/season/ ou racine).")

    rows = []
    for fp in pm_files:
        pm = unify_element(load_csv(Path(fp)))
        cols = ["element","fixture","opponent_team"]
        if "kickoff_time" in pm.columns:
            cols.append("kickoff_time")
        pm = pm[cols].copy()
        gw_num = parse_gw_from_filename(Path(fp).name)
        if gw_col is not None and gw_num is not None:
            pm[gw_col] = gw_num
            pm = pm.sort_values(["element","fixture"]).drop_duplicates(["element", gw_col], keep="first")
        else:
            pm = pm.sort_values(["element","fixture"]).drop_duplicates("element", keep="first")
        rows.append(pm)

    mapping = pd.concat(rows, ignore_index=True).drop_duplicates()

    # Jointure principale (toujours avec suffixes pour contrôler et coalescer ensuite)
    if gw_col is not None and gw_col in mapping.columns:
        mg = merged.merge(mapping, on=["element", gw_col], how="left", suffixes=("", "_map"))
    else:
        if "fixture" in merged.columns:
            mg = merged.merge(mapping, on="fixture", how="left", suffixes=("", "_map"))
        else:
            mg = merged.merge(mapping.groupby("element", as_index=False).first(), on="element", how="left", suffixes=("", "_map"))

    # Coalesce systématique des colonnes clés
    for base in ["fixture", "opponent_team", "kickoff_time"]:
        mg = coalesce_columns(mg, base)

    # Compléter kickoff_time via fixtures.csv si encore manquant et si 'fixture' existe
    if "fixture" in mg.columns and (("kickoff_time" not in mg.columns) or (mg["kickoff_time"].isna().any())):
        if fixtures_path.exists():
            fixtures = load_csv(fixtures_path)
            if {"id","kickoff_time"}.issubset(fixtures.columns):
                fix_mini = fixtures[["id","kickoff_time"]].rename(columns={"id":"fixture"})
                mg = mg.merge(fix_mini, on="fixture", how="left", suffixes=("", "_from_fix"))
                # Coalesce de kickoff_time avec la version issue des fixtures
                if "kickoff_time_from_fix" in mg.columns:
                    mg["kickoff_time"] = mg["kickoff_time"].fillna(mg["kickoff_time_from_fix"])
                    mg.drop(columns=["kickoff_time_from_fix"], inplace=True, errors="ignore")
                # Re-coalesce au cas où
                mg = coalesce_columns(mg, "kickoff_time")
        else:
            print("[INFO] fixtures.csv introuvable — 'kickoff_time' non complété via fixtures.")

    # name : full name si possible sinon web_name
    if "name" not in mg.columns or mg["name"].isna().all():
        if pid_path.exists():
            pid = load_csv(pid_path)[["id","web_name","first_name","second_name"]].rename(columns={"id":"element"})
            mg = mg.merge(pid, on="element", how="left")
            mg["name"] = mg.apply(prefer_full_name, axis=1)
            mg.drop(columns=["first_name","second_name","web_name"], inplace=True, errors="ignore")
        elif "web_name" in mg.columns and "name" not in mg.columns:
            mg["name"] = mg["web_name"]
        elif "name" not in mg.columns:
            mg["name"] = pd.NA

    # Types
    for c in ["element","fixture","opponent_team","minutes"]:
        if c in mg.columns:
            mg[c] = pd.to_numeric(mg[c], errors="coerce").astype("Int64")

    write_current_and_snapshot(
        mg,
        current_path=merged_path,
        name_for_snapshot="merged_gw"
    )
    print("[OK] data/merged_gw.csv enrichi (fixture, kickoff_time, opponent_team, name)")

if __name__ == "__main__":
    main()
