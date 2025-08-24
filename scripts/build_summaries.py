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
    """Détecte rapidement des marqueurs de conflit git dans un fichier texte."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            txt = f.read(200_000)  # 200KB suffisent largement
        return ("<<<<<<<" in txt) or ("=======" in txt) or (">>>>>>>" in txt)
    except Exception:
        return False

def load_csv(path: str, expect: list[str] | None = None) -> tuple[pd.DataFrame, list[str]]:
    """Lit un CSV si présent. Normalise les colonnes (minuscule/trim). Retourne (df, anomalies)."""
    anoms = []
    rel = os.path.relpath(path, ROOT)

    if not os.path.exists(path):
        anoms.append(f"missing_file:{rel}")
        return pd.DataFrame(), anoms

    # Conflits git non résolus -> on logue et on n'utilise pas ce fichier
    if has_conflict_markers(path):
        anoms.append(f"git_conflict_markers:{rel}")
        return pd.DataFrame(), anoms

    # Tentative 1 : sniff du séparateur (engine=python), SANS low_memory
    try:
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8")
    except Exception:
        # Tentative 2 : séparateur virgule par défaut
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            # Tentative 3 : BOM éventuel
            try:
                df = pd.read_csv(path, encoding="utf-8-sig")
            except Exception as e3:
                anoms.append(f"read_error:{rel}:{e3}")
                return pd.DataFrame(), anoms

    # Normaliser colonnes
    df.columns = [c.strip().lower() for c in df.columns]

    # Si une seule colonne → probablement CSV mal parsé (entête cassée, etc.)
    if df.shape[1] == 1:
        anoms.append(f"single_column_parse:{rel}:{df.columns.tolist()}")
        return pd.DataFrame(), anoms

    if expect:
        miss = [c for c in expect if c.lower() not in df.columns]
        if miss:
            anoms.append(f"missing_cols:{rel}:{miss}")

    return df, anoms

def detect_player_id(df: pd.DataFrame):
    """Trouve la colonne identifiant joueur et renomme en 'player_id' si nécessaire."""
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

anomalies: list[str] = []

players_snap, a = load_csv(os.path.join(DATA, "players_raw_snapshot_current.csv")); anomalies += a
players_hist, a = load_csv(os.path.join(DATA, "players_raw_history.csv")); anomalies += a
pcf, a = load_csv(os.path.join(DATA, "price_change_forecast.csv")); anomalies += a
pcf_hist, a = load_csv(os.path.join(DATA, "price_change_forecast_history.csv")); anomalies += a
fixtures, a = load_csv(os.path.join(DATA, "fixtures.csv")); anomalies += a
deadlines, a = load_csv(os.path.join(DATA, "deadlines.csv")); anomalies += a
merged_gw, a = load_csv(os.path.join(DATA, "merged_gw.csv")); anomalies += a

# ---------- line_counts.json ----------
line_counts = {}
for rel in [
    "players_raw_snapshot_current.csv",
    "players_raw_history.csv",
    "price_change_forecast.csv",
    "price_change_forecast_history.csv",
    "fixtures.csv",
    "deadlines.csv",
    "merged_gw.csv",
]:
    p = os.path.join(DATA, rel)
    if os.path.exists(p):
        try:
            c = sum(1 for _ in open(p, "rb"))
        except Exception:
            c = None
    else:
        c = None
    line_counts[rel] = c

write_json(os.path.join(OUTDIR, "line_counts.json"), {
    "last_updated_utc": UTC_NOW,
    "counts": line_counts
})

# ---------- players_snapshot_summary.json ----------
snap_summary = {"last_updated_utc": UTC_NOW, "by_position": [], "by_team": [], "top_selected": []}
if not players_snap.empty:
    # Harmoniser le prix: accepter 'now_cost'/'cost' comme alias de 'price_m'
    if "price_m" not in players_snap.columns:
        for alias in ["now_cost", "cost"]:
            if alias in players_snap.columns:
                players_snap.rename(columns={alias: "price_m"}, inplace=True)
                break

    # Harmoniser le nom d'équipe pour l'affichage (team_name prioritaire)
    tcol = "team_name" if "team_name" in players_snap.columns else ("team" if "team" in players_snap.columns else None)

    # Conversions numériques (si présentes)
    to_num(players_snap, ["price_m", "selected_by_percent"])

    # by_position
    if "position" in players_snap.columns and "id" in players_snap.columns:
        agg_pos = {"n": ("id", "count")}
        if "price_m" in players_snap.columns:
            agg_pos["avg_price"] = ("price_m", "mean")
        if "selected_by_percent" in players_snap.columns:
            agg_pos["avg_sel"] = ("selected_by_percent", "mean")

        g = players_snap.groupby("position", dropna=False).agg(**agg_pos).reset_index()
        snap_summary["by_position"] = head_dict(g.sort_values("position"))

    # by_team
    if tcol and "id" in players_snap.columns:
        agg_team = {"n": ("id", "count")}
        if "price_m" in players_snap.columns:
            agg_team["avg_price"] = ("price_m", "mean")
        if "selected_by_percent" in players_snap.columns:
            agg_team["avg_sel"] = ("selected_by_percent", "mean")

        g = players_snap.groupby(tcol, dropna=False).agg(**agg_team).reset_index().rename(columns={tcol: "team"})
        try:
            g_sorted = g.sort_values("team")
        except Exception:
            g_sorted = g.sort_values("n", ascending=False)
        snap_summary["by_team"] = head_dict(g_sorted)

    # top selected
    if {"id", "web_name", "selected_by_percent"} <= set(players_snap.columns):
        keep = ["id", "web_name", "position", "price_m", "selected_by_percent"]
        if tcol:
            keep.append(tcol)
        keep = [c for c in keep if c in players_snap.columns]
        top = players_snap[keep].copy()
        if tcol and tcol != "team_name":
            top.rename(columns={tcol: "team"}, inplace=True)
        if "selected_by_percent" in top.columns:
            top = top.sort_values("selected_by_percent", ascending=False).head(20)
        elif "price_m" in top.columns:
            top = top.sort_values("price_m", ascending=False).head(20)
        snap_summary["top_selected"] = head_dict(top)

write_json(os.path.join(OUTDIR, "players_snapshot_summary.json"), snap_summary)

# ---------- ownership_momentum.json ----------
own_mom = {"last_updated_utc": UTC_NOW, "most_in": [], "most_out": []}
if not players_hist.empty and {"id","date","selected_by_percent","transfers_in","transfers_out"} <= set(players_hist.columns):
    to_num(players_hist, ["selected_by_percent","transfers_in","transfers_out","price_m"])
    players_hist["date"] = pd.to_datetime(players_hist["date"], errors="coerce")
    last_date = players_hist["date"].max()
    prev_date = last_date - pd.Timedelta(days=1) if pd.notna(last_date) else None
    if pd.notna(last_date):
        last = players_hist[players_hist["date"] == last_date]
        if prev_date is not None:
            prev = players_hist[players_hist["date"] == prev_date][["id","selected_by_percent"]].rename(
                columns={"selected_by_percent":"selected_by_percent_prev"}
            )
            last = last.merge(prev, on="id", how="left")
            last["delta_sel"] = last["selected_by_percent"] - last["selected_by_percent_prev"]
        else:
            last["delta_sel"] = pd.NA
        last["net_transfers"] = (last.get("transfers_in", 0) - last.get("transfers_out", 0))
        cols = ["id","web_name","team_name","position","price_m","selected_by_percent","delta_sel","net_transfers"]
        cols = [c for c in cols if c in last.columns]
        last = last[cols].copy()
        own_mom["most_in"]  = head_dict(last.sort_values(["net_transfers","delta_sel"], ascending=[False, False]).head(20))
        own_mom["most_out"] = head_dict(last.sort_values(["net_transfers","delta_sel"], ascending=[True, True]).head(20))

write_json(os.path.join(OUTDIR, "ownership_momentum.json"), own_mom)

# ---------- price_changes_observed.json ----------
pco = {"last_updated_utc": UTC_NOW, "risers": [], "fallers": []}
if not players_hist.empty and {"id","date","price_m"} <= set(players_hist.columns):
    players_hist["date"] = pd.to_datetime(players_hist["date"], errors="coerce")
    to_num(players_hist, ["price_m"])
    last_date = players_hist["date"].max()
    if pd.notna(last_date):
        last = players_hist[players_hist["date"] == last_date][["id","web_name","team_name","position","price_m"]].copy()
        prev = players_hist[players_hist["date"] == (last_date - pd.Timedelta(days=1))][["id","price_m"]].rename(columns={"price_m":"price_prev"})
        last = last.merge(prev, on="id", how="left")
        last["price_delta"] = last["price_m"] - last["price_prev"]
        risers = last.sort_values("price_delta", ascending=False).query("price_delta > 0").head(20)
        fallers = last.sort_values("price_delta", ascending=True).query("price_delta < 0").head(20)
        pco["risers"]  = head_dict(risers)
        pco["fallers"] = head_dict(fallers)

write_json(os.path.join(OUTDIR, "price_changes_observed.json"), pco)

# ---------- price_change_forecast_summary.json ----------
pcf_sum = {"last_updated_utc": UTC_NOW, "top_up": [], "top_down": []}
if not pcf.empty:
    cand_id, a = detect_player_id(pcf); anomalies += a
    if cand_id and cand_id != "player_id":
        pcf.rename(columns={cand_id: "player_id"}, inplace=True)
    if "now_cost" in pcf.columns and "price_m" not in pcf.columns:
        pcf.rename(columns={"now_cost": "price_m"}, inplace=True)
    # 'forecast' peut être textuel (ex: 'stable'); s'il n'y a pas 'forecast_delta', on ne fait que skipper le top
    to_num(pcf, ["price_m","forecast_delta"])
    if "forecast_delta" in pcf.columns:
        keep = [c for c in ["player_id","id","web_name","team_name","price_m","forecast_delta"] if c in pcf.columns]
        pcf_top = pcf[keep].copy()
        pcf_sum["top_up"]   = head_dict(pcf_top.sort_values("forecast_delta", ascending=False).head(20))
        pcf_sum["top_down"] = head_dict(pcf_top.sort_values("forecast_delta", ascending=True ).head(20))

write_json(os.path.join(OUTDIR, "price_change_forecast_summary.json"), pcf_sum)

# ---------- thresholds_calibration.json ----------
thr = {"last_updated_utc": UTC_NOW, "by_ownership_bucket": []}
if not pcf_hist.empty and {"id","date","forecast_delta"} <= set(pcf_hist.columns):
    to_num(pcf_hist, ["forecast_delta","selected_by_percent"])
    own = pcf_hist.get("selected_by_percent")
    if own is not None:
        bins = [-0.01, 5, 10, 20, 40, 60, 100]
        labels = ["0-5","5-10","10-20","20-40","40-60","60-100"]
        pcf_hist["own_bucket"] = pd.cut(pcf_hist["selected_by_percent"].astype(float), bins=bins, labels=labels)
        g = pcf_hist.groupby("own_bucket").agg(
            n=("forecast_delta","count"),
            avg_delta=("forecast_delta","mean"),
            p90=("forecast_delta", lambda s: s.quantile(0.9))
        ).reset_index()
        thr["by_ownership_bucket"] = head_dict(g)

write_json(os.path.join(OUTDIR, "thresholds_calibration.json"), thr)

# ---------- fixtures_outlook.json ----------
fx = {"last_updated_utc": UTC_NOW, "by_team_next3": []}
if not fixtures.empty:
    for c in ["event","team_h","team_a","team_h_score","team_a_score","finished"]:
        if c not in fixtures.columns:
            fixtures[c] = pd.NA
    to_num(fixtures, ["event","team_h","team_a"])
    next_gw = None
    try:
        if "finished" in fixtures.columns and "event" in fixtures.columns:
            unfinished = fixtures[(fixtures["finished"] == False) & pd.notna(fixtures["event"])]
            next_gw = int(unfinished["event"].min()) if not unfinished.empty else int(fixtures["event"].max())
    except Exception:
        pass
    outlook = []
    if next_gw is not None:
        teams = pd.unique(pd.concat([fixtures["team_h"], fixtures["team_a"]], ignore_index=True).dropna()).astype(int)
        for t in teams:
            tfx = fixtures[(fixtures["team_h"]==t) | (fixtures["team_a"]==t)]
            fut = tfx[tfx["event"].astype("Int64") >= next_gw].sort_values("event").head(3)
            outlook.append({
                "team_id": int(t),
                "gw_sequence": fut["event"].tolist(),
                "opponents": [
                    (int(r["team_a"]) if int(r["team_h"])==t else int(r["team_h"])) for _, r in fut.iterrows()
                ]
            })
    fx["by_team_next3"] = outlook

write_json(os.path.join(OUTDIR, "fixtures_outlook.json"), fx)

# ---------- gw_summary.json ----------
gw_sum = {"last_updated_utc": UTC_NOW, "top_players": []}
if not merged_gw.empty:
    id_col, a = detect_player_id(merged_gw); anomalies += a
    if id_col:
        # Colonnes minimales
        for c in ["minutes","total_points","goals_scored","assists"]:
            if c not in merged_gw.columns:
                merged_gw[c] = pd.NA
        # per90 robuste (évite NAType): remplacer 0 par NaN float
        pts = pd.to_numeric(merged_gw.get("total_points"), errors="coerce")
        mins = pd.to_numeric(merged_gw.get("minutes"), errors="coerce").replace(0, float("nan"))
        merged_gw["points_per90"] = (pts * 90.0) / mins
        keep = [c for c in ["player_id","web_name","team_name","position","minutes","total_points","points_per90","goals_scored","assists"] if c in merged_gw.columns]
        top = merged_gw[keep].copy().sort_values(["total_points","points_per90"], ascending=[False, False]).head(25)
        gw_sum["top_players"] = head_dict(top)

write_json(os.path.join(OUTDIR, "gw_summary.json"), gw_sum)

# ---------- anomaly_report.json ----------
write_json(os.path.join(OUTDIR, "anomaly_report.json"), {
    "last_updated_utc": UTC_NOW,
    "notes": anomalies,
})

# ---------- manifest.json ----------
produced = []
for f in [
    "manifest.json",
    "line_counts.json",
    "players_snapshot_summary.json",
    "ownership_momentum.json",
    "price_changes_observed.json",
    "price_change_forecast_summary.json",
    "thresholds_calibration.json",
    "fixtures_outlook.json",
    "gw_summary.json",
    "anomaly_report.json",
]:
    p = os.path.join(OUTDIR, f)
    if os.path.exists(p):
        produced.append(file_info(p))

manifest = {
    "last_updated_utc": UTC_NOW,
    "produced": produced
}
write_json(os.path.join(OUTDIR, "manifest.json"), manifest)

print(f"[summaries] OK - {len(produced)} fichiers écrits · {UTC_NOW}")
