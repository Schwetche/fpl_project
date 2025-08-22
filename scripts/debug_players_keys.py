# scripts/debug_players_keys.py
# Objectif : afficher les colonnes "clés" attendues par ton validateur et
# montrer lesquelles manquent dans data/players_raw.csv

from pathlib import Path
import importlib.util, ast, pandas as pd, sys, re

def find_validator_file():
    try:
        spec = importlib.util.find_spec("validate_fpl_pipeline")
        if spec and spec.origin:
            return Path(spec.origin)
    except Exception:
        pass
    # fallback : racine du projet
    here = Path(__file__).resolve().parents[1]
    cand = here / "validate_fpl_pipeline.py"
    return cand if cand.exists() else None

def extract_expected_cols(py_path: Path):
    """Parsing AST : récupère listes de colonnes dont le nom de variable contient 'player' et ('require' ou 'key')."""
    txt = py_path.read_text(encoding="utf-8", errors="ignore")
    tree = ast.parse(txt, filename=str(py_path))
    found = {}
    class V(ast.NodeVisitor):
        def visit_Assign(self, node):
            # variables du style REQUIRED_PLAYERS_COLS, PLAYERS_KEY_COLUMNS, etc.
            targets = []
            for t in node.targets:
                if isinstance(t, ast.Name):
                    targets.append(t.id)
                elif isinstance(t, ast.Attribute):
                    targets.append(t.attr)
            for name in targets:
                lname = name.lower()
                if "player" in lname and ("require" in lname or "key" in lname or "must" in lname):
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        vals = []
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                vals.append(elt.value)
                        if vals:
                            found[name] = vals
            self.generic_visit(node)
    V().visit(tree)

    # bonus : recherche textuelle basique si rien trouvé via AST
    if not found:
        m = re.search(r"players?_.*?(required|key).*?=\s*\[(.*?)\]", txt, re.I | re.S)
        if m:
            raw = m.group(2)
            vals = [s.strip().strip("'\"") for s in re.findall(r"['\"]([^'\"]+)['\"]", raw)]
            if vals:
                found["guessed_required_players_cols"] = vals

    return found

def main():
    root = Path(__file__).resolve().parents[1]
    players_path = root / "data" / "players_raw.csv"
    if not players_path.exists():
        print(f"[ERR] Introuvable : {players_path}")
        sys.exit(1)

    cols = pd.read_csv(players_path, nrows=0, encoding="utf-8").columns.tolist()
    print(f"[INFO] players_raw.csv colonnes ({len(cols)}) : {cols[:12]}{' ...' if len(cols)>12 else ''}\n")

    vf = find_validator_file()
    if not vf:
        print("[ERR] Impossible de localiser validate_fpl_pipeline.py — colle son emplacement si différent.")
        sys.exit(2)
    print(f"[INFO] Validateur détecté : {vf}")

    expected_sets = extract_expected_cols(vf)
    if not expected_sets:
        print("[WARN] Aucune liste 'required/key' détectée automatiquement dans le validateur.")
        print("      -> Poste ici la ligne complète du WARN ou un extrait du validateur.")
        sys.exit(3)

    cols_set = set(cols)
    for name, expected in expected_sets.items():
        missing = [c for c in expected if c not in cols_set]
        print(f"\n[EXPECTED SET] {name} ({len(expected)} colonnes)")
        print(f" - Manquantes ({len(missing)}): {missing}")
        present = [c for c in expected if c in cols_set]
        print(f" - Présentes  ({len(present)}): {present[:12]}{' ...' if len(present)>12 else ''}")

if __name__ == "__main__":
    main()
