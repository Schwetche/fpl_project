# scripts/test_fpl_api.py
import requests

URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

r = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
print("HTTP:", r.status_code)
r.raise_for_status()

data = r.json()
print("Racines:", list(data.keys())[:6])
print("elements (joueurs):", len(data.get("elements", [])))
print("teams:", len(data.get("teams", [])))
print("events:", len(data.get("events", [])))
