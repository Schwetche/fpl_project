# scripts/sanitize_ascii.py
import os, sys, re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT
MAP = {
    "\u2714": "OK",    # checkmark
    "\u2705": "OK",    # white heavy check mark
    "\u2716": "X",
    "\u2718": "X",
    "\u25CF": "-",     # black circle
    "\u2022": "-",     # bullet
    "\u2192": "->",    # right arrow
    "\u2014": "-",     # em dash
    "\u2013": "-",     # en dash
    "\u00A0": " ",     # nbsp
    "✅": "OK",
    "✔": "OK",
    "•": "-",
    "→": "->",
    "—": "-",
    "–": "-",
    "⚠": "WARN",
}
# guillemets typographiques -> ASCII
MAP.update({ "“": '"', "”": '"', "‘": "'", "’": "'" })

def sanitize_text(txt: str) -> str:
    for k, v in MAP.items():
        txt = txt.replace(k, v)
    # remplace tout résidu non-ASCII par un point d'interrogation dans les chaînes imprimées
    try:
        txt.encode("ascii")
        return txt
    except UnicodeEncodeError:
        # fallback brutal: supprimer / remplacer non-ascii
        return txt.encode("ascii", "replace").decode("ascii")

def sanitize_file(fp: Path):
    if fp.suffix.lower() != ".py":
        return False, "skip (ext)"
    orig = fp.read_text(encoding="utf-8", errors="ignore")
    clean = sanitize_text(orig)
    if clean != orig:
        fp.write_text(clean, encoding="utf-8")
        return True, "patched"
    return False, "unchanged"

def main():
    patched = 0
    total = 0
    for p in SCRIPTS.glob("*.py"):
        if p.name == Path(__file__).name:
            continue
        total += 1
        ch, status = sanitize_file(p)
        if ch:
            patched += 1
            print(f"[OK] {p.name} -> {status}")
        else:
            print(f"[..] {p.name} -> {status}")
    print(f"\nDone. {patched}/{total} files patched.")

if __name__ == "__main__":
    main()
