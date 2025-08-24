# scripts/append_price_change_forecast.py
"""
Append du fichier price_change_forecast.csv vers un historique global.

- Lit data/price_change_forecast.csv
- Ajoute à data/price_change_forecast_history.csv
- Ajoute snapshot_time (ISO) et source_file
- Évite les doublons (id + snapshot_time)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
SRC_FILE = DATA_DIR / "price_change_forecast.csv"
HIST_FILE = DATA_DIR / "price_change_forecast_history.csv"

def main():
    if not SRC_FILE.exists():
        print(f"[WARN] Fichier source absent : {SRC_FILE}")
        return

    # Lire source
    df = pd.read_csv(SRC_FILE)
    if df.empty:
        print("[WARN] price_change_forecast.csv vide.")
        return

    # Ajouter colonnes
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df.insert(0, "snapshot_time", ts)
    df.insert(1, "source_file", "price_change_forecast.csv")

    # Charger historique existant
    if HIST_FILE.exists():
        hist = pd.read_csv(HIST_FILE)
        combined = pd.concat([hist, df], ignore_index=True)
        combined.drop_duplicates(subset=["id", "snapshot_time"], inplace=True)
    else:
        combined = df

    # Sauvegarde
    combined.to_csv(HIST_FILE, index=False)
    print(f"[PASS] Historique mis à jour : {len(combined)} lignes.")

if __name__ == "__main__":
    main()
