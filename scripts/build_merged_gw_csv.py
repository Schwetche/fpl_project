# scripts/build_merged_gw_csv.py
import os
import glob
import pandas as pd

def main(season_dir="../data/season", out_file="../data/merged_gw.csv"):
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), season_dir))
    # On prend seulement les totaux (exclure *_permatch.csv)
    files = sorted(
        [p for p in glob.glob(os.path.join(base, "gw*.csv")) if "_permatch" not in os.path.basename(p)],
        key=lambda p: int(os.path.basename(p).split(".")[0].replace("gw",""))
    )

    if not files:
        print("WARN Aucun fichier gwX.csv trouv? pour fusion.")
        return

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["source_file"] = os.path.basename(f)
            frames.append(df)
        except Exception as e:
            print(f"! Lecture ?chou?e {f}: {e}")

    if not frames:
        print("WARN Aucun contenu lisible.")
        return

    merged = pd.concat(frames, ignore_index=True, sort=False)

    out_full = os.path.abspath(os.path.join(os.path.dirname(__file__), out_file))
    os.makedirs(os.path.dirname(out_full), exist_ok=True)
    merged.to_csv(out_full, index=False, encoding="utf-8")
    print(f"OK merged_gw.csv -> {out_full}  ({len(merged)} lignes, {len(files)} fichiers fusionn?s)")

if __name__ == "__main__":
    main()
