#!/usr/bin/env python3
import sys, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

markers = ("<<<<<<<", "=======", ">>>>>>>")
bad = []

for p in DATA.rglob("*.csv"):
    try:
        with p.open("r", encoding="utf-8", errors="ignore") as f:
            txt = f.read()
        if any(m in txt for m in markers):
            bad.append(p.relative_to(ROOT).as_posix())
    except Exception:
        pass

if bad:
    print("❌ CSV avec marqueurs de conflit :", *bad, sep="\n - ")
    sys.exit(1)
else:
    print("✅ Aucun marqueur de conflit trouvé dans data/*.csv")
