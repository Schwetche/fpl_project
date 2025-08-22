# scripts/build_one_gw_permatch_csv.py
import os
import requests
import pandas as pd

UA = {"User-Agent": "Mozilla/5.0 (compatible; FPL-PerMatch/1.0)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
EVENT_LIVE = "https://fantasy.premierleague.com/api/event/{gw}/live/"
FIXTURES = "https://fantasy.premierleague.com/api/fixtures/?event={gw}"

def pick_one_gw(events):
    """Retourne la derni?re GW pass?e si dispo, sinon la courante, sinon la premi?re."""
    prev = [e["id"] for e in events if e.get("is_previous")]
    if prev:
        return sorted(prev)[-1]
    cur = [e["id"] for e in events if e.get("is_current")]
    if cur:
        return cur[0]
    ids = sorted([e["id"] for e in events if "id" in e])
    return ids[0] if ids else None

def normalize_explain(explain):
    """Aplati 'explain' (liste de dicts ou liste de listes) en une liste de dicts {'fixture', 'stats': [...]}."""
    out = []
    if not isinstance(explain, list):
        return out
    for chunk in explain:
        if isinstance(chunk, dict) and "fixture" in chunk:
            out.append(chunk)
        elif isinstance(chunk, list):
            for sub in chunk:
                if isinstance(sub, dict) and "fixture" in sub:
                    out.append(sub)
    return out

def build_one_gw_permatch(gw=None, out_dir="../data/season"):
    # 1) bootstrap : joueurs/?quipes + choix GW
    boot = requests.get(BOOTSTRAP, headers=UA, timeout=30).json()
    events = boot.get("events", [])
    players = pd.DataFrame(boot["elements"])[["id", "web_name", "first_name", "second_name", "team", "element_type"]]
    teams = pd.DataFrame(boot["teams"])[["id", "name", "short_name"]].rename(columns={"id": "team"})
    pos_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    players["position"] = players["element_type"].map(pos_map)

    if gw is None:
        gw = pick_one_gw(events)
        if gw is None:
            print("WARN Aucune GW d?tect?e.")
            return

    # 2) fixtures de la GW (id->fixture, event->gw)
    fx = requests.get(FIXTURES.format(gw=gw), headers=UA, timeout=30).json()
    df_fx = pd.json_normalize(fx)
    keep_fx = ["id", "event", "kickoff_time", "team_h", "team_a", "team_h_score", "team_a_score"]
    df_fx = df_fx[[c for c in keep_fx if c in df_fx.columns]].rename(columns={"id": "fixture", "event": "gw"})

    # 3) event live (stats par match via 'explain')
    live = requests.get(EVENT_LIVE.format(gw=gw), headers=UA, timeout=30).json()

    rows = []
    for el in live.get("elements", []):
        pid = el["id"]
        per_matches = normalize_explain(el.get("explain", []))
        for m in per_matches:
            fixture_id = m.get("fixture")
            stat_list = m.get("stats", []) or []
            stat_map = {s.get("identifier"): s.get("value") for s in stat_list}
            rows.append({
                "gw": gw,
                "player_id": pid,
                "fixture": fixture_id,
                **stat_map
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"WARN GW{gw}: aucune donn?e 'par match' (explain vide). GW future/non jou?e - on saute.")
        return  # pas de fichier vide

    # 4) enrichissement: joueurs + fixtures + home/away/opponent
    df = df.merge(players.rename(columns={"id": "player_id"}), on="player_id", how="left")
    df = df.merge(df_fx, on=["fixture", "gw"], how="left")

    id2name = teams.set_index("team")["name"].to_dict()
    id2short = teams.set_index("team")["short_name"].to_dict()

    df["team_name"] = df["team"].map(id2name)
    df["team_short"] = df["team"].map(id2short)

    df["is_home"] = df.apply(lambda r: True if r.get("team") == r.get("team_h") else False, axis=1)
    df["opponent_team"] = df.apply(lambda r: r.get("team_a") if r.get("is_home") else r.get("team_h"), axis=1)
    df["opponent_name"] = df["opponent_team"].map(id2name)
    df["opponent_short"] = df["opponent_team"].map(id2short)

    # 5) colonnes ordonn?es (garde celles pr?sentes)
    preferred = [
        "gw", "fixture", "kickoff_time",
        "player_id", "web_name", "first_name", "second_name", "position",
        "team", "team_name", "team_short", "is_home", "opponent_team", "opponent_name", "opponent_short",
        "team_h", "team_h_score", "team_a", "team_a_score",
        "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals",
        "saves", "penalties_saved", "penalties_missed", "yellow_cards", "red_cards",
        "bonus", "bps", "influence", "creativity", "threat", "ict_index",
        "expected_goals", "expected_assists", "expected_goal_involvements", "expected_goals_conceded",
        "total_points"
    ]
    df = df[[c for c in preferred if c in df.columns]]

    # 6) ?criture
    out_full = os.path.abspath(os.path.join(os.path.dirname(__file__), f"{out_dir}/gw{gw}_permatch.csv"))
    os.makedirs(os.path.dirname(out_full), exist_ok=True)
    df.to_csv(out_full, index=False, encoding="utf-8")
    print(f"OK gw{gw}_permatch.csv ?crit -> {out_full}  ({len(df)} lignes)")

if __name__ == "__main__":
    build_one_gw_permatch()
