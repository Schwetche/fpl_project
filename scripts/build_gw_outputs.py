# scripts/build_gw_outputs.py
from pathlib import Path
import re
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data"
SEASON = DATA / "season"
MERGED_GW = DATA / "merged_gw.csv"
MERGED_GW_PM = DATA / "merged_gw_permatch.csv"

def _concat_glob(pattern: str) -> pd.DataFrame | None:
    rx = re.compile(pattern, re.IGNORECASE)
    files = sorted([p for p in SEASON.glob("*.csv") if rx.match(p.name)])
    if not files:
        return None
    frames = []
    for p in files:
        try:
            df = pd.read_csv(p)
            # Ajouter la colonne gw si absente et devinable depuis le nom
            m = re.match(r"^gw(\d+)(?:_permatch)?\.csv$", p.name, re.IGNORECASE)
            if m and "gw" not in df.columns:
                df.insert(0, "gw", int(m.group(1)))
            frames.append(df)
        except Exception as e:
            print(f"[WARN] lecture impossible {p.name}: {e}")
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)

def main():
    SEASON.mkdir(parents=True, exist_ok=True)

    # merged_gw.csv
    df_gw = _concat_glob(r"^gw\d+\.csv$")
    if df_gw is not None:
        df_gw.to_csv(MERGED_GW, index=False)
        print(f"[PASS] {MERGED_GW.name} écrit ({len(df_gw):,} lignes)")
    else:
        print("[WARN] Aucun gwN.csv trouvé — merged_gw non mis à jour")

    # merged_gw_permatch.csv
    df_gwpm = _concat_glob(r"^gw\d+_permatch\.csv$")
    if df_gwpm is not None:
        df_gwpm.to_csv(MERGED_GW_PM, index=False)
        print(f"[PASS] {MERGED_GW_PM.name} écrit ({len(df_gwpm):,} lignes)")
    else:
        print("[WARN] Aucun gwN_permatch.csv trouvé — merged_gw_permatch non mis à jour")

if __name__ == "__main__":
    main()

