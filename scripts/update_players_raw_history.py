import sys
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
import pytz

# Dossiers
REPO = Path(__file__).resolve().parents[1]
DATA_DIR = REPO / "data"
SNAP_DIR = DATA_DIR / "snapshots"
HIST_FILE = DATA_DIR / "players_raw_history.csv"

API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

# Labels horaires (adapté à tes crons 10h/15h/23h Zurich)
def get_snapshot_label(hour_local: int) -> str:
    # ≤10h → morning ; 11–16h → noon ; >16h → evening
    if hour_local <= 10:
        return "morning"
    elif hour_local <= 16:
        return "noon"
    else:
        return "evening"

def main():
    tz_ch = pytz.timezone("Europe/Zurich")
    now_local = datetime.now(tz_ch)
    now_utc = now_local.astimezone(pytz.UTC)

    snapshot_time = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO en UTC (Z correct)
    snapshot_label = get_snapshot_label(now_local.hour)

    # 1) Récupérer données API
    r = requests.get(API_URL, timeout=60)
    r.raise_for_status()
    data = r.json()["elements"]
    df = pd.DataFrame(data)

    # 2) Colonnes additionnelles
    df.insert(0, "snapshot_time", snapshot_time)
    df.insert(1, "snapshot_label", snapshot_label)

    # 3) Snapshot brut horodaté (en local time pour le nom de fichier)
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"players_raw_{now_local.strftime('%Y%m%d_%H%M')}.csv"
    snap_path = SNAP_DIR / fname
    df.to_csv(snap_path, index=False)

    # 4) Mettre à jour players_raw_history.csv (append sans doublons id+snapshot_time)
    HIST_FILE.parent.mkdir(parents=True, exist_ok=True)
    if HIST_FILE.exists():
        hist = pd.read_csv(HIST_FILE)
        combined = pd.concat([hist, df], ignore_index=True)
        combined.drop_duplicates(subset=["id", "snapshot_time"], inplace=True)
    else:
        combined = df

    combined.to_csv(HIST_FILE, index=False)
    print(f"[PASS] Snapshot {fname} ajouté. History total = {len(combined):,} lignes.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FAIL] {e}")
        sys.exit(1)
