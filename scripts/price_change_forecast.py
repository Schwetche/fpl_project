# scripts/price_change_forecast.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
from utils_io import ensure_dirs, latest_two_snapshots, read_csv_safe, to_float_safe, always_write_csv

DATA_DIR = Path("data")
OUT_FILE = DATA_DIR / "price_change_forecast.csv"

SNAP_DIR = DATA_DIR / "snapshots"
PLAYERS_STEM = "players_raw"

DELTAS_DIR = DATA_DIR / "deltas"
NTI_LOG = DELTAS_DIR / "nti_deltas.csv"

def _thresholds(own: float | None) -> tuple[int,int]:
    if own is None: return (600_000, 350_000)
    if own < 5: return (300_000, 15_000)
    if own < 10: return (325_000, 325_000)
    if own < 20: return (625_000, 375_000)
    if own < 40: return (775_000, 450_000)
    return (900_000, 525_000)

def _eta_window(ratio: float, nti1h: float) -> str:
    # estimation grossière : plus le ratio est élevé et plus NTI_1h est fort, plus la fenêtre est courte
    if ratio >= 1.5 or abs(nti1h) >= 150_000: return "0–6h"
    if ratio >= 1.0 or abs(nti1h) >= 100_000: return "6–12h"
    if ratio >= 0.7 or abs(nti1h) >= 60_000:  return "12–24h"
    if ratio >= 0.4:                           return "24–48h"
    return "48h+"

def main():
    ensure_dirs(DATA_DIR)
    latest, _ = latest_two_snapshots(SNAP_DIR, PLAYERS_STEM)
    if latest is None:
        raise SystemExit("[ERROR] No snapshots found.")
    df = pd.read_csv(latest)

    nti = read_csv_safe(NTI_LOG)
    if nti.empty:
        df["NTI"] = df.get("transfers_in_event", 0) - df.get("transfers_out_event", 0)
        df["NTI_1h"] = 0
        df["NTI_24h"] = df["NTI"]
    else:
        nti["timestamp"] = pd.to_datetime(nti["timestamp"])
        nti.sort_values(["id","timestamp"], inplace=True)
        last = nti.groupby("id").tail(1)[["id","NTI","NTI_1h","NTI_24h"]]
        df = pd.merge(df, last, on="id", how="left")
        df[["NTI","NTI_1h","NTI_24h"]] = df[["NTI","NTI_1h","NTI_24h"]].fillna(0)

    # ownership + status
    df["ownership"] = df.get("selected_by_percent", "").apply(to_float_safe)
    df["status"] = df.get("status", "").fillna("")

    rows = []
    for _, r in df.iterrows():
        own = r["ownership"]
        up_thr, down_thr = _thresholds(own)
        nti24 = float(r.get("NTI_24h", 0))
        nti1h = float(r.get("NTI_1h", 0))

        # momentum (croissance/décroissance du NTI)
        momentum = "up" if nti1h > 0 else ("down" if nti1h < 0 else "flat")

        # forecast + risques séparés
        forecast = "stable"; risk_up = 0.0; risk_down = 0.0; ratio = 0.0
        if nti24 >= up_thr:
            forecast = "+0.1"
            ratio = nti24 / up_thr if up_thr else 0.0
            risk_up = min(1.0, ratio)
        elif nti24 <= -down_thr:
            forecast = "-0.1"
            ratio = abs(nti24) / down_thr if down_thr else 0.0
            risk_down = min(1.0, ratio)

        # price_freeze heuristique basique : joueur flaggé/inj/susp → 1
        st = str(r.get("status","")).lower()
        chance = r.get("chance_of_playing_this_round", None)
        try:
            chance = float(chance) if chance is not None else None
        except Exception:
            chance = None
        price_freeze = 1 if (st in {"d","i","s"} or (chance is not None and chance < 75)) else 0

        rows.append({
            "id": r["id"],
            "web_name": r.get("web_name",""),
            "team": r.get("team",""),
            "position": r.get("element_type",""),
            "now_cost": r.get("now_cost",0),
            "ownership": own,
            "NTI_1h": nti1h,
            "NTI_24h": nti24,
            "momentum": momentum,
            "status": r.get("status",""),
            "price_freeze": price_freeze,
            "forecast": forecast,
            "risk_up": round(risk_up,3),
            "risk_down": round(risk_down,3),
            "eta_window": _eta_window(max(risk_up, risk_down), nti1h),
        })

    out = pd.DataFrame(rows, columns=[
        "id","web_name","team","position","now_cost","ownership",
        "NTI_1h","NTI_24h","momentum","status","price_freeze",
        "forecast","risk_up","risk_down","eta_window"
    ])

    always_write_csv(out, OUT_FILE, DATA_DIR/"snapshots", "price_change_forecast")
    print(f"[PASS] price_change_forecast.csv written ({len(out)} rows)")

if __name__=="__main__":
    main()
