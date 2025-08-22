# scripts/build_merged_gw_permatch.py
import os
import glob
import pandas as pd

def main(season_dir="../data/season", out_file="../data/merged_gw_permatch.csv"):
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), season_dir))
    pattern = os.path.join(base, "gw*_permatch.csv")
    files = sorted(glob.glob(pattern), key=lambda p: (
        int(os.path.basename(p).split("_")[0][2:])  # extrait le num?ro de GW depuis "gw<gw>_permatch.csv"
    ))

    if not files:
        print("WARN Aucun fichier gw*_permatch.csv trouv? dans", base)
        return

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["source_file"] = os.path.basename(f)
            frames.append(df)
        except Exception as e:
            print(f"! Probl?me de lecture {f}: {e}")

    if not frames:
        print("WARN Aucun contenu lisible pour fusion.")
        return

    merged = pd.concat(frames, ignore_index=True, sort=False)

    out_full = os.path.abspath(os.path.join(os.path.dirname(__file__), out_file))
    os.makedirs(os.path.dirname(out_full), exist_ok=True)
    merged.to_csv(out_full, index=False, encoding="utf-8")

    print(f"OK merged_gw_permatch.csv ?crit -> {out_full}  ({len(merged)} lignes, {len(files)} fichiers fusionn?s)")

if __name__ == "__main__":
    main()
