# scripts/build_indexes.py
from pathlib import Path
import re
from datetime import datetime

REPO = Path(__file__).resolve().parents[1]
SNAPS_DIR = REPO / "data" / "snapshots"
SEASON_DIR = REPO / "data" / "season"

def human_size(n):
    for unit in ["B","KB","MB","GB"]:
        if n < 1024.0:
            return f"{n:.0f} {unit}"
        n /= 1024.0
    return f"{n:.0f} TB"

def render_page(title, sections):
    # sections: list of tuples (section_title, list_of_dict(name, href, size, mtime))
    css = """
    body{font-family:system-ui,Arial,sans-serif;margin:24px;background:#f8fafc;color:#0f172a}
    h1{color:#0b6bcb;margin-bottom:.25rem}
    h2{color:#1e293b;margin:16px 0 8px}
    table{border-collapse:collapse;width:100%;background:#fff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden}
    th,td{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:left}
    tr:nth-child(even){background:#f9fbff}
    a{color:#0b6bcb;text-decoration:none}
    a:hover{text-decoration:underline}
    footer{margin-top:16px;color:#475569;font-size:.9rem}
    """
    html = [f"<!doctype html><meta charset='utf-8'><title>{title}</title><style>{css}</style>",
            f"<h1>{title}</h1>"]
    for sec_title, rows in sections:
        html.append(f"<h2>{sec_title}</h2>")
        if not rows:
            html.append("<p><em>Aucun fichier pour le moment.</em></p>")
            continue
        html.append("<table><thead><tr><th>Fichier</th><th>Taille</th><th>Modifié</th></tr></thead><tbody>")
        for r in rows:
            html.append(
                f"<tr><td><a href='{r['href']}'>{r['name']}</a></td>"
                f"<td>{r['size']}</td>"
                f"<td>{r['mtime']}</td></tr>"
            )
        html.append("</tbody></table>")
    html.append(f"<footer>Généré le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</footer>")
    return "\n".join(html)

def list_csvs(folder: Path, pattern: str):
    rx = re.compile(pattern, re.IGNORECASE)
    items = []
    if folder.exists():
        for p in folder.iterdir():
            if p.is_file() and rx.match(p.name):
                stat = p.stat()
                items.append({
                    "name": p.name,
                    "href": p.name,  # liens relatifs
                    "size": human_size(stat.st_size),
                    "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                })
    # tri décroissant par nom (snapshots: timestamp dans le nom → récent d'abord)
    items.sort(key=lambda x: x["name"], reverse=True)
    return items

def build_snapshots_index():
    SNAPS_DIR.mkdir(parents=True, exist_ok=True)
    rows = list_csvs(SNAPS_DIR, r"^players_raw_\d{8}_\d{4}\.csv$")
    html = render_page("Snapshots players_raw", [("Fichiers", rows)])
    (SNAPS_DIR / "index.html").write_text(html, encoding="utf-8")

def build_season_index():
    SEASON_DIR.mkdir(parents=True, exist_ok=True)
    gw_rows = list_csvs(SEASON_DIR, r"^gw\d+\.csv$")
    gwp_rows = list_csvs(SEASON_DIR, r"^gw\d+_permatch\.csv$")
    html = render_page("Données de saison (GW)", [
        ("GW par joueur (gwN.csv)", gw_rows),
        ("GW par match (gwN_permatch.csv)", gwp_rows),
    ])
    (SEASON_DIR / "index.html").write_text(html, encoding="utf-8")

def main():
    build_snapshots_index()
    build_season_index()
    print("[PASS] index générés: data/snapshots/index.html et data/season/index.html")

if __name__ == "__main__":
    main()
