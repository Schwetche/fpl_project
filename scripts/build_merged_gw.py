# scripts/build_merged_gw.py
# Concatène toutes les data/season/gw{N}.csv (par joueur) en data/merged_gw.csv
# Always Write + snapshot. Ajoute un alias player_id si "element" présent.

import re, pandas as pd
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.utils_io import ensure_dirs, write_current_and_snapshot

def main():
    base = ROOT
    ensure_dirs(base)

    season = base / "data" / "season"
    files = sorted([p for p in season.glob("gw*.csv") if "_permatch" not in p.name])

    frames = []
    for p in files:
        try:
            df = pd.read_csv(p)
            if "gw" not in df.columns:
                m = re.match(r"gw(\d+)\.csv$", p.name, re.IGNORECASE)
                if m:
                    df["gw"] = int(m.group(1))
            # alias player_id si nécessaire
            if "player_id" not in df.columns and "element" in df.columns:
                df["player_id"] = df["element"]
            frames.append(df)
        except Exception as e:
            print(f"[WARN] cannot read {p.name}: {e}")

    merged = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame({"info":["no gw*.csv found"]})
    write_current_and_snapshot(merged, base / "data" / "merged_gw.csv", "merged_gw")

if __name__ == "__main__":
    main()
