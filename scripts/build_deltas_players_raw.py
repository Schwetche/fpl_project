# scripts/build_deltas_players_raw.py
# Compare les DEUX derniers snapshots de data/snapshots/players_raw_*.csv
# Produit data/deltas/players_raw_delta_YYYYMMDD-HHMMSS.csv
# Affiche un résumé (Top transferts nets, Top variations de prix)

from pathlib import Path
from datetime import datetime
import glob
import pandas as pd

def prefer_full_name(df: pd.DataFrame) -> pd.Series:
    fn = df.get("first_name")
    sn = df.get("second_name")
    wn = df.get("web_name")
    out = None
    if fn is not None and sn is not None:
        out = (fn.fillna("").astype(str).str.strip() + " " + sn.fillna("").astype(str).str.strip()).str.strip()
        out = out.where(out.str.strip() != "", None)
    if out is None and wn is not None:
        out = wn
    if out is None:
        out = pd.Series([None] * len(df))
    return out

def main():
    root = Path(__file__).resolve().parents[1]
    snaps_dir = root / "data" / "snapshots"
    deltas_dir = root / "data" / "deltas"
    deltas_dir.mkdir(parents=True, exist_ok=True)

    # Trouver les deux derniers snapshots players_raw_*.csv
    files = sorted(glob.glob(str(snaps_dir / "players_raw_*.csv")))
    if len(files) < 2:
        print("[INFO] Pas assez de snapshots (>=2 requis). Lance d'abord fetch_bootstrap_and_update_players.py au moins 2 fois.")
        return

    old_path, new_path = files[-2], files[-1]
    print(f"[LOAD] OLD: {old_path}")
    print(f"[LOAD] NEW: {new_path}")

    old = pd.read_csv(old_path, encoding="utf-8")
    new = pd.read_csv(new_path, encoding="utf-8")

    # Colonnes à suivre (on garde flexible si certaines manquent)
    follow_int = [
        "now_cost","cost_change_event","cost_change_start",
        "transfers_in_event","transfers_out_event"
    ]
    follow_float = ["selected_by_percent"]

    for c in follow_int:
        if c in new.columns: new[c] = pd.to_numeric(new[c], errors="coerce")
        if c in old.columns: old[c] = pd.to_numeric(old[c], errors="coerce")
    for c in follow_float:
        if c in new.columns: new[c] = pd.to_numeric(new[c], errors="coerce")
        if c in old.columns: old[c] = pd.to_numeric(old[c], errors="coerce")

    # Base d'identifiants
    if "id" not in new.columns or "id" not in old.columns:
        raise KeyError("Colonne 'id' absente dans players_raw snapshots.")

    # Préparer 'name' (facultatif, pour lecture humaine)
    new = new.copy()
    new["name"] = prefer_full_name(new)
    if "name" not in new.columns or new["name"].isna().all():
        if "web_name" in new.columns:
            new["name"] = new["web_name"]

    keep_for_join = ["id","name","web_name","first_name","second_name"] + follow_int + follow_float
    keep_for_join = [c for c in keep_for_join if c in new.columns]
    new2 = new[keep_for_join].copy()
    old2 = old[[c for c in keep_for_join if c in old.columns]].copy()

    merged = new2.merge(
        old2.add_suffix("_prev"),
        left_on="id", right_on="id_prev",
        how="left"
    )

    # Calcul des deltas
    def make_delta(col):
        col_prev = f"{col}_prev"
        if col in merged.columns and col_prev in merged.columns:
            merged[f"Δ_{col}"] = merged[col] - merged[col_prev]

    for col in follow_int + follow_float:
        make_delta(col)

    # Transferts nets & prix en millions pour lecture
    if "transfers_in_event" in merged.columns and "transfers_out_event" in merged.columns:
        merged["transfers_net_event"] = merged["transfers_in_event"] - merged["transfers_out_event"]
    if "transfers_in_event_prev" in merged.columns and "transfers_out_event_prev" in merged.columns:
        merged["transfers_net_event_prev"] = merged["transfers_in_event_prev"] - merged["transfers_out_event_prev"]
    if "now_cost" in merged.columns:
        merged["price_m"] = merged["now_cost"] / 10.0
    if "now_cost_prev" in merged.columns:
        merged["price_m_prev"] = merged["now_cost_prev"] / 10.0
    if "Δ_now_cost" in merged.columns:
        merged["Δ_price_m"] = merged["Δ_now_cost"] / 10.0

    # Nettoyage colonnes techniques
    if "id_prev" in merged.columns:
        merged = merged.drop(columns=["id_prev"])

    # Ordonner colonnes clés en tête (facultatif)
    head_cols = ["id","name","price_m","price_m_prev","Δ_price_m","transfers_net_event","transfers_net_event_prev"] if "Δ_price_m" in merged.columns else ["id","name"]
    head_cols = [c for c in head_cols if c in merged.columns]
    other_cols = [c for c in merged.columns if c not in head_cols]
    merged = merged[head_cols + other_cols]

    # Écriture
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = deltas_dir / f"players_raw_delta_{ts}.csv"
    merged.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[WROTE] DELTA -> {out_path}")

    # Résumé console (Top transferts nets Δ & Top Δ prix)
    print("\n[TOP 10] Δ transferts_net_event (NEW - OLD)")
    if "transfers_net_event" in merged.columns and "transfers_net_event_prev" in merged.columns:
        tmp = merged.copy()
        tmp["Δ_transfers_net_event"] = tmp["transfers_net_event"] - tmp["transfers_net_event_prev"]
        cols_show = [c for c in ["name","price_m","transfers_net_event","transfers_net_event_prev","Δ_transfers_net_event"] if c in tmp.columns]
        tmp = tmp.sort_values("Δ_transfers_net_event", ascending=False)
        print(tmp[cols_show].head(10).to_string(index=False))
    else:
        print("transfers_net_event non disponible sur les deux snapshots.")

    if "Δ_price_m" in merged.columns:
        print("\n[TOP 10] Δ prix (millions)")
        cols_show = [c for c in ["name","price_m_prev","price_m","Δ_price_m"] if c in merged.columns]
        tmp = merged.sort_values("Δ_price_m", ascending=False)
        print(tmp[cols_show].head(10).to_string(index=False))
    else:
        print("\nΔ_price_m non disponible.")

if __name__ == "__main__":
    main()
