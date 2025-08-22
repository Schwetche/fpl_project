# scripts/validate_fpl_pipeline.py
# Validation robuste de la pipeline FPL.
# - Tolérant aux noms d'ID: 'player_id' > 'element' > 'id'
# - Pour le mapping fixture->event, utilise gw1.csv si possible, sinon bascule sur gw1_permatch.csv

from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

def pick_id_col(df, candidates=("player_id", "element", "id")):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def safe_read_csv(path: Path):
    if not path.exists():
        raise FileNotFoundError(str(path))
    return pd.read_csv(path)

def main():
    passes = warns = fails = 0

    def ok(msg):
        nonlocal passes
        print(f"[PASS] {msg}"); passes += 1

    def warn(msg):
        nonlocal warns
        print(f"[WARN] {msg}"); warns += 1

    def fail(msg):
        nonlocal fails
        print(f"[FAIL] {msg}"); fails += 1

    # 1) Présence des fichiers clés
    pr_path = DATA / "players_raw.csv"
    fx_path = DATA / "fixtures.csv"
    mg_path = DATA / "merged_gw.csv"
    cp_path = DATA / "cleaned_players.csv"

    pr = safe_read_csv(pr_path); ok(f"File present: players_raw.csv ({pr_path})")
    fx = safe_read_csv(fx_path); ok(f"File present: fixtures.csv ({fx_path})")
    mg = safe_read_csv(mg_path); ok(f"File present: merged_gw.csv ({mg_path})")
    cp = safe_read_csv(cp_path); ok(f"File present: cleaned_players.csv ({cp_path})")

    # 2) Sanity basique players_raw
    if len(pr) > 0 and {"web_name","team_name","position"}.issubset(pr.columns):
        ok(f"players_raw key columns OK ({len(pr)} players)")
    else:
        warn("players_raw missing some key columns")

    # 3) Numériques basiques
    for col in ["price_m","selected_by_percent","minutes","total_points"]:
        if col in pr.columns:
            pd.to_numeric(pr[col], errors="coerce")
    ok("players_raw numerics OK")

    # 4) Cross-checks GW1 si présents
    gw1p = DATA / "season" / "gw1.csv"
    gw1m = DATA / "season" / "gw1_permatch.csv"
    g1p = pd.read_csv(gw1p) if gw1p.exists() else None
    g1m = pd.read_csv(gw1m) if gw1m.exists() else None

    if g1p is not None and g1m is not None:
        # 4a) minutes per player == somme per-match
        idp = pick_id_col(g1p)
        idm = pick_id_col(g1m)
        if idp and idm and "minutes" in g1p.columns and "minutes" in g1m.columns:
            left = g1p.groupby(idp, as_index=False)["minutes"].sum().rename(columns={"minutes":"min_players"})
            right = g1m.groupby(idm, as_index=False)["minutes"].sum().rename(columns={idm:idp, "minutes":"min_permatch"})
            merged = left.merge(right, on=idp, how="outer").fillna(0)
            diff = (merged["min_players"] - merged["min_permatch"]).abs().sum()
            if diff == 0:
                ok("GW1 minutes total equals per-match sum for all players.")
            else:
                fail(f"GW1 minutes mismatch total diff={diff}")
        else:
            warn("GW1 cross-check skipped (id/minutes columns missing).")

        # 4b) mapping fixture->event==gw
        # préférer g1p si 'fixture' présent, sinon basculer sur g1m
        use_df = None
        if g1p is not None and {"fixture","gw"}.issubset(g1p.columns):
            use_df = g1p[["fixture","gw"]].dropna().copy()
        elif g1m is not None and {"fixture","gw"}.issubset(g1m.columns):
            use_df = g1m[["fixture","gw"]].dropna().copy()

        if use_df is not None and {"id","event"}.issubset(fx.columns):
            fx1 = fx[["id","event"]].rename(columns={"id":"fixture"})
            test = use_df.drop_duplicates()
            merged2 = test.merge(fx1, on="fixture", how="left")
            bad = merged2[
                merged2["event"].notna()
                & merged2["gw"].notna()
                & (merged2["event"].astype(int) != merged2["gw"].astype(int))
            ]
            if bad.empty:
                ok("GW1 fixture->event mapping is coherent (event==gw).")
            else:
                warn(f"GW1 fixture->event mismatch rows={len(bad)}")
        else:
            warn("GW1 fixture mapping skipped (required columns missing).")
    else:
        warn("GW1 files not found; GW cross-checks skipped.")

    # 5) merged_gw présence minimale
    if len(mg) > 0:
        ok(f"merged_gw.csv non-empty ({len(mg)} rows).")
    else:
        warn("merged_gw.csv empty")

    # 6) cleaned_players unique joueurs
    if "web_name" in cp.columns:
        ok(f"cleaned_players.csv OK ({cp['web_name'].nunique()} unique players).")
    else:
        warn("cleaned_players.csv missing 'web_name'")

    # 7) Snapshots & deltas (best-effort)
    snaps = list((DATA / "snapshots").glob("players_raw_*.csv"))
    if snaps:
        ok(f"Snapshots OK ({len(snaps)} files).")
    else:
        warn("No players_raw snapshots found.")

    deltas = list((DATA / "deltas").glob("deltas_*_to_*.csv"))
    if deltas:
        anyd = pd.read_csv(deltas[-1])
        ok(f"Deltas present ({deltas[-1].name}), columns like {anyd.columns[:4].tolist()} ...")
    else:
        warn("No deltas present.")

    print(f"\nSUMMARY: {passes} PASS, {warns} WARN, {fails} FAIL")
    sys.exit(0 if fails == 0 else 1)

if __name__ == "__main__":
    main()
