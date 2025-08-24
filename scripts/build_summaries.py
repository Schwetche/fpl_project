# scripts/build_summaries.py
# Génère des fichiers légers dans summaries/ à partir des CSV de data/
# Robuste aux colonnes manquantes, variations de schéma et conflits git non résolus.

from __future__ import annotations
import os, json
from datetime import datetime, timezone
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(ROOT, "data")
OUTDIR = os.path.join(ROOT, "summaries")
os.makedirs(OUTDIR, exist_ok=True)

UTC_NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------- Utilitaires ----------

def has_conflict_markers(path: str) -> bool:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            txt = f.read(200_000)
        return ("<<<<<<<" in txt) or ("=======" in txt) or (">>>>>>>" in txt)
    except Exception:
        return False

def load_csv(path: str, expect: list[str] | None = None) -> tuple[pd.DataFrame, list[str]]:
    anoms = []
    rel = os.path.relpath(path, ROOT)
    if not os.path.exists(path):
        anoms.append(f"missing_file:{rel}")
        return pd.DataFrame(), anoms
    if has_conflict_markers(path):
        anoms.append(f"git_conflict_markers:{rel}")
        return pd.DataFrame(), anoms
    try:
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8")
    except Exception:
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
            except Exception as e3:
                anoms.append(f"read_error:{rel}:{e3}")
                return pd.DataFrame(), anoms
    df.columns = [c.strip().lower() for c in df.columns]
    if df.shape[1] == 1:
        anoms.append(f"single_column_parse:{rel}:{df.columns.tolist()}")
        return pd.DataFrame(), anoms
    if expect:
        miss = [c for c in expect if c.lower() not in df.columns]
        if miss:
            anoms.append(f"missing_cols:{rel}:{miss}")
    return df, anoms

def detect_player_id(df: pd.DataFrame):
    cand = ["player_id", "element", "id", "player", "playerid"]
    found = next((c for c in cand if c in df.columns), None)
    if found and found != "player_id":
        df.rename(columns={found: "player_id"}, inplace=True)
        return "player_id", []
    return found, ([] if found else [f"id_not_found:{cand}"])

def to_num(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def head_dict(df: pd.DataFrame, n=5):
    return df.head(n).to_dict(orient="records")

def file_info(path: str):
    try:
        sz = os.path.getsize(path)
    except OSError:
        sz = None
    return {"path": os.path.relpath(path, ROOT), "size_bytes": sz}

# ---------- Chargements ----------

anomalies = []

players_snap, a = load_csv(os.path.join(DATA, "players_raw_snapshot_current.csv"))
anomalies += a
players_hist, a = load_csv(os.path.join(DATA, "players_raw_history.csv")); anomalies += a
pcf, a = load_csv(os.path.join(DATA, "price_change_forecast.csv")); anomalies += a
pcf_hist, a = load_csv(os.path.join(DATA, "price_change_forecast_history.csv")); anomalies += a
fixtures, a = load_csv(os.path.join(DATA, "fixtures.csv")); anomalies += a
deadlines, a = load_csv(os.path.join(DATA, "deadlines.csv")); anomalies += a
merged_gw, a = load_csv(os.path.join(DATA, "merged_gw.csv")); anomalies += a

# ---------- players_snapshot_summary.json ----------
snap_summary = {"last_updated_utc": UTC_NOW, "by_position": [], "by_team": [], "top_selected": []}
if not players_snap.empty:
    if "price_m" not in players_snap.columns:
        for alias in ["now_cost", "cost"]:
            if alias in players_snap.columns:
                players_snap.rename(columns={alias: "price_m"}, inplace=True)
                break
    tcol = "team_name" if "team_name" in players_snap.columns else ("team" if "team" in players_snap.columns else None)
    to_num(players_snap, ["price_m", "selected_by_percent"])
    if "position" in players_snap.columns and "id" in players_snap.columns:
        agg = {"n": ("id","count")}
        if "price_m" in players_snap.columns: agg["avg_price"] = ("price_m","mean")
        if "selected_by_percent" in players_snap.columns: agg["avg_sel"] = ("selected_by_percent","mean")
        g = players_snap.groupby("position", dropna=False).agg(**agg).reset_index()
        snap_summary["by_position"] = head_dict(g.sort_values("position"))
    if tcol and "id" in players_snap.columns:
        agg = {"n": ("id","count")}
        if "price_m" in players_snap.columns: agg["avg_price"] = ("price_m","mean")
        if "selected_by_percent" in players_snap.columns: agg["avg_sel"] = ("selected_by_percent","mean")
        g = players_snap.groupby(tcol, dropna=False).agg(**agg).reset_index().rename(columns={tcol:"team"})
        try: g_sorted = g.sort_values("team")
        except Exception: g_sorted = g.sort_values("n", ascending=False)
        snap_summary["by_team"] = head_dict(g_sorted)
    if {"id","web_name","selected_by_percent"} <= set(players_snap.columns):
        keep = ["id","web_name","position","price_m","selected_by_percent"]
        if tcol: keep.append(tcol)
        keep = [c for c in keep if c in players_snap.columns]
        top = players_snap[keep].copy()
        if tcol and tcol != "team_name": top.rename(columns={tcol:"team"}, inplace=True)
        if "selected_by_percent" in top.columns:
            top = top.sort_values("selected_by_percent", ascending=False).head(20)
        elif "price_m" in top.columns:
            top = top.sort_values("price_m", ascending=False).head(20)
        snap_summary["top_selected"] = head_dict(top)
write_json(os.path.join(OUTDIR, "players_snapshot_summary.json"), snap_summary)

# ---------- gw_summary.json ----------
gw_sum = {"last_updated_utc": UTC_NOW, "top_players": []}
if not merged_gw.empty:
    id_col, a = detect_player_id(merged_gw); anomalies += a
    if id_col:
        stat_cols = ["minutes","total_points","goals_scored","assists"]
        for c in stat_cols:
            if c not in merged_gw.columns: merged_gw[c] = pd.NA
        pts = pd.to_numeric(merged_gw.get("total_points"), errors="coerce")
        mins = pd.to_numeric(merged_gw.get("minutes"), errors="coerce").replace(0, float("nan"))
        merged_gw["points_per90"] = (pts * 90.0) / mins
        keep = [c for c in ["player_id","web_name","team_name","position","minutes","total_points","points_per90","goals_scored","assists"] if c in merged_gw.columns]
        top = merged_gw[keep].copy().sort_values(["total_points","points_per90"], ascending=[False, False]).head(25)
        gw_sum["top_players"] = head_dict(top)
write_json(os.path.join(OUTDIR, "gw_summary.json"), gw_sum)

# ... (les autres blocs inchangés : ownership_momentum, price_changes_observed, price_change_forecast_summary,
# thresholds_calibration, fixtures_outlook, anomaly_report, manifest)
