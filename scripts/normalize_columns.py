# scripts/normalize_columns.py
# But :
# - Normaliser les colonnes de tous les CSV FPL (players_raw, cleaned_players, gwX, merged_gw, *_permatch, fixtures, teams, player_idlist)
# - Supprimer les doublons (web_name_x/_y, id/player_id vs element, etc.)
# - Appliquer les noms canoniques et un ordre logique
# - Réécrire chaque fichier + snapshot daté dans data/snapshots/
#
# Usage :
#   py -3.13 scripts/normalize_columns.py
#
# Notes :
# - On garde kickoff_time tel que l’API (UTC ISO).
# - Abréviations xg/xa/xgi/xgc :
#     * cleaned_players.csv : on GARDE abréviations + versions longues (on remplit les longues si absentes).
#     * autres fichiers     : on conserve uniquement les versions LONGUES (créées depuis les abréviations si besoin).
# - Compat VALIDATEUR :
#     * On conserve **gw** ET **event** pour les fichiers de matchs.
#     * On conserve **player_id** dans les fichiers *_permatch (et on garde aussi element).
#     * Pour **fixtures.csv**, on **garde `id`** ET on **ajoute `fixture` = `id`** (aucun renommage), afin que les deux existent.
# - Compat outils tiers :
#     * Dans **players_raw.csv**, on conserve/ajoute **id** comme **duplicat de `element`**.

from pathlib import Path
import re, glob
import pandas as pd
from utils_io import write_current_and_snapshot

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# ---- utilitaires généraux ----------------------------------------------------

SUFFIXES = ("_x", "_y", "__pm", "_map", "_from_fix")

def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")

def find_variants(df: pd.DataFrame, base_or_alias: str) -> list[str]:
    cands = [base_or_alias] + [base_or_alias + s for s in SUFFIXES]
    return [c for c in cands if c in df.columns]

def coalesce(df: pd.DataFrame, canonical: str, aliases: list[str], drop_others=True):
    if canonical not in df.columns:
        df[canonical] = pd.NA
    used_cols = []
    for a in [canonical] + list(aliases):
        for v in find_variants(df, a):
            if v == canonical:
                continue
            df[canonical] = df[canonical].fillna(df[v])
            used_cols.append(v)
    if drop_others:
        for c in used_cols:
            if c in df.columns:
                df.drop(columns=[c], inplace=True, errors="ignore")
    return df

def unify_element_key(df: pd.DataFrame, drop_alias=True):
    """element = id = player_id ; garde 'element' et supprime 'id'/'player_id' selon drop_alias."""
    df = coalesce(df, "element", ["id", "player_id"])
    if "element" in df.columns:
        df["element"] = pd.to_numeric(df["element"], errors="coerce").astype("Int64")
    if drop_alias:
        for c in ("id", "player_id"):
            if c in df.columns:
                df.drop(columns=[c], inplace=True, errors="ignore")
    return df

def build_full_name(df: pd.DataFrame):
    fn = df.get("first_name")
    sn = df.get("second_name")
    if fn is not None and sn is not None:
        name = (fn.fillna("").astype(str).str.strip() + " " + sn.fillna("").astype(str).str.strip()).str.strip()
        df["name"] = name.where(name != "", df.get("web_name"))
    else:
        if "name" not in df.columns:
            df["name"] = df.get("web_name")
    return df

ABBR_TO_LONG = {
    "xg":  "expected_goals",
    "xa":  "expected_assists",
    "xgi": "expected_goal_involvements",
    "xgc": "expected_goals_conceded",
}

def harmonize_xg(df: pd.DataFrame, keep_abbrev: bool):
    for abbr, longn in ABBR_TO_LONG.items():
        if (longn not in df.columns) and (abbr in df.columns):
            df[longn] = pd.to_numeric(df[abbr], errors="coerce")
    if not keep_abbrev:
        for abbr in ABBR_TO_LONG.keys():
            if abbr in df.columns:
                df.drop(columns=[abbr], inplace=True, errors="ignore")
    return df

def normalize_match_context(df: pd.DataFrame):
    # ---- GW & EVENT : conserver les deux ----
    gw_series = None
    if "gw" in df.columns:
        gw_series = pd.to_numeric(df["gw"], errors="coerce")
    elif "event" in df.columns:
        gw_series = pd.to_numeric(df["event"], errors="coerce")
    elif "round" in df.columns:
        gw_series = pd.to_numeric(df["round"], errors="coerce")

    if gw_series is not None:
        df["gw"] = gw_series.astype("Int64")
        if "event" not in df.columns:
            df["event"] = df["gw"]
        else:
            df["event"] = pd.to_numeric(df["event"], errors="coerce").astype("Int64")

    # ---- FIXTURE ----
    df = coalesce(df, "fixture", ["fixture_id", "id_fixture"])
    if "fixture" in df.columns:
        df["fixture"] = pd.to_numeric(df["fixture"], errors="coerce").astype("Int64")

    # ---- OPPONENT ----
    df = coalesce(df, "opponent_team", ["opp_team", "opponent_id"])
    if "opponent_team" in df.columns:
        df["opponent_team"] = pd.to_numeric(df["opponent_team"], errors="coerce").astype("Int64")

    # ---- KICKOFF ----
    df = coalesce(df, "kickoff_time", ["date", "kickoff_datetime"])
    return df

def normalize_team(df: pd.DataFrame):
    df = coalesce(df, "team", ["team_id"])
    if "team" in df.columns:
        df["team"] = pd.to_numeric(df["team"], errors="coerce").astype("Int64")
    df = coalesce(df, "team_name", [])
    df = coalesce(df, "team_short", [])
    return df

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    groups = [
        ["element","player_id"],  # 'id' sera ajouté explicitement pour players_raw ci-dessous
        ["web_name","name","first_name","second_name","position"],
        ["team","team_name","team_short"],
        ["gw","event","fixture","kickoff_time","opponent_team"],
        ["minutes","total_points","goals_scored","assists","clean_sheets","goals_conceded",
         "own_goals","penalties_saved","penalties_missed","yellow_cards","red_cards","saves","bonus","bps",
         "influence","creativity","threat","ict_index",
         "expected_goals","expected_assists","expected_goal_involvements","expected_goals_conceded"],
        ["now_cost","price_m","cost_change_event","cost_change_start","transfers_in_event","transfers_out_event","selected_by_percent"],
        ["status","chance_of_playing_next_round","chance_of_playing_this_round"],
    ]
    ordered, seen = [], set()
    for g in groups:
        for c in g:
            if c in df.columns and c not in seen:
                ordered.append(c); seen.add(c)
    rest = [c for c in df.columns if c not in seen and not any(c.endswith(s) for s in SUFFIXES)]
    rest.sort()
    return df[ordered + rest] if ordered or rest else df

# ---- normalisations par type de fichier --------------------------------------

def normalize_players_raw(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver id pour compat outils externes : on NE SUPPRIME PAS id/player_id ici
    df = unify_element_key(df, drop_alias=False)
    # S'assurer que 'id' existe et reflète 'element'
    if "element" in df.columns:
        if "id" not in df.columns:
            df["id"] = df["element"]
        else:
            df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
            df.loc[df["id"].isna(), "id"] = df["element"]
    # Nom joueur
    df = coalesce(df, "web_name", ["web_name_x", "web_name_y"])
    df = build_full_name(df)
    # Équipe
    df = normalize_team(df)
    # Marché : price_m si absent
    if "now_cost" in df.columns and "price_m" not in df.columns:
        df["price_m"] = pd.to_numeric(df["now_cost"], errors="coerce") / 10.0
    # xG (versions longues uniquement dans players_raw)
    df = harmonize_xg(df, keep_abbrev=False)
    # Nettoyage suffixes éventuels
    for c in ("web_name_x","web_name_y"):
        if c in df.columns: df.drop(columns=[c], inplace=True, errors="ignore")
    # Ordre : on place 'id' juste après 'element' si présent
    df = reorder_columns(df)
    cols = df.columns.tolist()
    if "element" in cols and "id" in cols:
        # injecter 'id' derrière 'element'
        cols = [c for c in cols if c != "id"]
        idx = cols.index("element") + 1
        cols = cols[:idx] + ["id"] + cols[idx:]
        df = df[cols]
    return df

def normalize_cleaned_players(df: pd.DataFrame) -> pd.DataFrame:
    df = unify_element_key(df, drop_alias=True)
    df = coalesce(df, "web_name", ["web_name_x","web_name_y"])
    df = build_full_name(df)
    df = normalize_team(df)
    df = harmonize_xg(df, keep_abbrev=True)
    return reorder_columns(df)

def normalize_gw_like(df: pd.DataFrame) -> pd.DataFrame:
    df = unify_element_key(df, drop_alias=True)
    df = coalesce(df, "web_name", ["web_name_x","web_name_y"])
    df = build_full_name(df)
    df = normalize_team(df)
    df = normalize_match_context(df)  # conserve gw ET event
    df = harmonize_xg(df, keep_abbrev=False)
    return reorder_columns(df)

def normalize_permatch(df: pd.DataFrame) -> pd.DataFrame:
    # NE PAS supprimer player_id. On garde aussi element.
    df = unify_element_key(df, drop_alias=False)
    if "player_id" not in df.columns and "element" in df.columns:
        df["player_id"] = df["element"]
    df = normalize_match_context(df)
    return reorder_columns(df)

def normalize_fixtures(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver 'id' et ajouter 'fixture' = 'id' si absent (ne PAS renommer)
    df = coalesce(df, "kickoff_time", ["kickoff_datetime","date"])
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        if "fixture" not in df.columns:
            df["fixture"] = df["id"]
    if "fixture" in df.columns:
        df["fixture"] = pd.to_numeric(df["fixture"], errors="coerce").astype("Int64")
    if "event" in df.columns:
        df["event"] = pd.to_numeric(df["event"], errors="coerce").astype("Int64")
    if "team_h" in df.columns:
        df["team_h"] = pd.to_numeric(df["team_h"], errors="coerce").astype("Int64")
    if "team_a" in df.columns:
        df["team_a"] = pd.to_numeric(df["team_a"], errors="coerce").astype("Int64")

    # Réordonner en mettant id/fixture en tête
    df = reorder_columns(df)
    cols = df.columns.tolist()
    front = []
    if "id" in cols: front.append("id")
    if "fixture" in cols and "fixture" not in front: front.append("fixture")
    if front:
        rest = [c for c in cols if c not in front]
        df = df[front + rest]
    return df

def normalize_teams(df: pd.DataFrame) -> pd.DataFrame:
    df = coalesce(df, "name", [])
    df = coalesce(df, "short_name", [])
    return df

def normalize_player_idlist(df: pd.DataFrame) -> pd.DataFrame:
    if "element" not in df.columns and "id" in df.columns:
        df["element"] = df["id"]
    df = coalesce(df, "web_name", ["web_name_x","web_name_y"])
    df = build_full_name(df)
    df = normalize_team(df)
    return reorder_columns(df)

# ---- boucle principale --------------------------------------------------------

def process_file(path: Path):
    name = path.name.lower()
    try:
        df = load_csv(path)
    except Exception as e:
        print(f"[SKIP] {path} -> lecture impossible : {e}")
        return

    if name == "players_raw.csv":
        kind = "players_raw";     df2 = normalize_players_raw(df)
    elif name == "cleaned_players.csv":
        kind = "cleaned_players"; df2 = normalize_cleaned_players(df)
    elif re.fullmatch(r"gw\d+\.csv", name):
        kind = "gw";              df2 = normalize_gw_like(df)
    elif name == "merged_gw.csv":
        kind = "merged_gw";       df2 = normalize_gw_like(df)
    elif re.fullmatch(r"gw\d+_permatch\.csv", name):
        kind = "permatch";        df2 = normalize_permatch(df)
    elif name == "merged_gw_permatch.csv":
        kind = "merged_permatch"; df2 = normalize_permatch(df)
    elif name == "fixtures.csv":
        kind = "fixtures";        df2 = normalize_fixtures(df)
    elif name == "teams.csv":
        kind = "teams";           df2 = normalize_teams(df)
    elif name == "player_idlist.csv":
        kind = "player_idlist";   df2 = normalize_player_idlist(df)
    else:
        kind = "generic";         df2 = normalize_gw_like(df)

    write_current_and_snapshot(df2, current_path=path, name_for_snapshot=path.stem)
    print(f"[OK] normalized {kind:>16} -> {path}")

def main():
    candidates = [
        DATA / "players_raw.csv",
        DATA / "cleaned_players.csv",
        DATA / "merged_gw.csv",
        DATA / "merged_gw_permatch.csv",
        DATA / "fixtures.csv",
        DATA / "teams.csv",
        DATA / "player_idlist.csv",
    ]
    candidates += [Path(p) for p in glob.glob(str(DATA / "season" / "gw*.csv"))]
    candidates = [p for p in candidates if p.exists()]

    if not candidates:
        print("[INFO] aucun CSV trouvé à normaliser.")
        return

    print(f"[INFO] normalisation de {len(candidates)} fichier(s).")
    for p in sorted(candidates):
        process_file(p)

if __name__ == "__main__":
    main()
