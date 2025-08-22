# scripts/build_cleaned_players.py
import os
from datetime import datetime, timezone
import numpy as np
import pandas as pd

MERGED_GW_PATH = "../data/merged_gw.csv"
PLAYERS_RAW_PATH = "../data/players_raw.csv"
OUT_PATH = "../data/cleaned_players.csv"

def to_numeric_safe(df: pd.DataFrame, cols):
    """Convertit en float toutes les colonnes pr?sentes dans df parmi 'cols' (coerce en NaN)."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def per90(series_num: pd.Series, series_minutes: pd.Series) -> pd.Series:
    """Calcule (num * 90 / minutes) en g?rant 0 et NA proprement."""
    num = pd.to_numeric(series_num, errors="coerce")
    mins = pd.to_numeric(series_minutes, errors="coerce")
    mins = mins.replace(0, np.nan)  # ?viter division par 0
    return (num * 90.0) / mins      # renvoie float avec NaN si mins manquant

def main(merged_path=MERGED_GW_PATH, players_raw_path=PLAYERS_RAW_PATH, out_path=OUT_PATH):
    base_dir = os.path.dirname(__file__)
    merged_file = os.path.abspath(os.path.join(base_dir, merged_path))
    raw_file = os.path.abspath(os.path.join(base_dir, players_raw_path))
    out_file = os.path.abspath(os.path.join(base_dir, out_path))

    if not os.path.exists(merged_file):
        raise SystemExit(f"Fichier introuvable: {merged_file} - lance d'abord build_merged_gw_csv.py")
    if not os.path.exists(raw_file):
        raise SystemExit(f"Fichier introuvable: {raw_file} - lance d'abord build_players_raw.py")

    # --- 1) lire les sources
    mgw = pd.read_csv(merged_file)
    pr = pd.read_csv(raw_file)
    if mgw.empty:
        raise SystemExit("merged_gw.csv est vide.")

    # --- 2) forcer les colonnes stats en num?rique
    stat_cols = [
        "minutes","total_points","goals_scored","assists","clean_sheets","goals_conceded",
        "own_goals","saves","penalties_saved","penalties_missed",
        "yellow_cards","red_cards","bonus","bps",
        "influence","creativity","threat","ict_index",
        "expected_goals","expected_assists","expected_goal_involvements","expected_goals_conceded"
    ]
    mgw = to_numeric_safe(mgw, stat_cols)

    # --- 3) colonnes d'identit?
    id_cols = [c for c in ["web_name","first_name","second_name","team_name","position"] if c in mgw.columns]

    # apparitions (minutes > 0)
    if "minutes" in mgw.columns:
        mgw["apps"] = (mgw["minutes"].fillna(0) > 0).astype(int)

    # colonnes ? sommer
    sum_cols = [c for c in ["apps"] + stat_cols if c in mgw.columns]

    # agr?gation
    agg_dict = {c: "first" for c in id_cols}
    agg_dict.update({c: "sum" for c in sum_cols})
    gp = mgw.groupby("player_id", as_index=False).agg(agg_dict)

    # --- 4) d?riv?es per90 (sans cast agressif)
    if "minutes" in gp.columns and "total_points" in gp.columns:
        gp["points_per90"] = per90(gp["total_points"], gp["minutes"]).round(2)

    for col in ["goals_scored","assists","saves"]:
        if col in gp.columns and "minutes" in gp.columns:
            gp[f"{col}_per90"] = per90(gp[col], gp["minutes"]).round(3)

    # alias x* plus courts si pr?sents
    rename_map = {}
    if "expected_goals" in gp.columns: rename_map["expected_goals"] = "xg"
    if "expected_assists" in gp.columns: rename_map["expected_assists"] = "xa"
    if "expected_goal_involvements" in gp.columns: rename_map["expected_goal_involvements"] = "xgi"
    if "expected_goals_conceded" in gp.columns: rename_map["expected_goals_conceded"] = "xgc"
    gp = gp.rename(columns=rename_map)

    # --- 5) players_raw : conversions utiles
    pr_num_cols = ["price_m","selected_by_percent","chance_of_playing_next_round","chance_of_playing_this_round"]
    pr = to_numeric_safe(pr, pr_num_cols)
    pr_min = pr[[c for c in ["id","price_m","selected_by_percent","status",
                             "chance_of_playing_next_round","chance_of_playing_this_round"]
                 if c in pr.columns]].rename(columns={"id":"player_id"})

    # --- 6) merge
    cleaned = gp.merge(pr_min, on="player_id", how="left")

    # --- 7) ordre de colonnes
    preferred = [
        "player_id","web_name","first_name","second_name","team_name","position",
        "minutes","apps","total_points","points_per90",
        "goals_scored","assists","clean_sheets","goals_conceded","own_goals",
        "saves","penalties_saved","penalties_missed",
        "yellow_cards","red_cards","bonus","bps",
        "influence","creativity","threat","ict_index",
        "xg","xa","xgi","xgc",
        "price_m","selected_by_percent","status",
        "chance_of_playing_next_round","chance_of_playing_this_round"
    ]
    ordered = [c for c in preferred if c in cleaned.columns] + [c for c in cleaned.columns if c not in preferred]
    cleaned = cleaned[ordered]

    # --- 8) timestamp et ?criture
    cleaned["last_updated_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    cleaned.to_csv(out_file, index=False, encoding="utf-8")
    print(f"OK cleaned_players.csv -> {out_file}  ({len(cleaned)} joueurs)")

if __name__ == "__main__":
    main()
