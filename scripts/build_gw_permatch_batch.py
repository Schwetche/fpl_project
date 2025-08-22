# scripts/build_gw_permatch_batch.py
# Boucle de génération "par match" pour GW start..end en appelant directement la fonction Python.

import argparse, sys
from build_one_gw_permatch_csv import build_one_gw_permatch

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, default=1)
    p.add_argument("--end", type=int, default=38)
    p.add_argument("--out_dir", default="../data/season")
    args = p.parse_args()

    ok = err = 0
    built = []

    for gw in range(args.start, args.end + 1):
        try:
            build_one_gw_permatch(gw=gw, out_dir=args.out_dir)
            ok += 1
            built.append(gw)
        except Exception as e:
            print(f"[WARN] GW{gw} skipped: {e}")
            err += 1

    print(f"[BATCH] done: ok={ok}, err={err}, built={built}")
    return 0 if err == 0 else 0  # on ne plante pas le pipeline si certaines GW manquent (normal avant match)

if __name__ == "__main__":
    sys.exit(main())
