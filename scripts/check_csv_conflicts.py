#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

MARKERS = ("<<<<<<<", "=======", ">>>>>>>")
EXTS = {".csv", ".json", ".txt"}

bad = []

for p in DATA.rglob("*"):
    if p.is_file() and p.suffix.lower() in EXTS:
        try:
            txt = p.read_text(encoding="utf-8", errors="ignore")
            if any(m in txt for m in MARKERS):
                bad.append(p.relative_to(ROOT).as_posix())
        except Exception:
            pass

if bad:
    print("❌ Fichiers avec marqueurs de conflit :", *[" - " + b for b in bad], sep="\n")
    sys.exit(1)
else:
    print("✅ Aucun marqueur de conflit détecté dans data/** (csv/json/txt)")
