# scripts/schedule_pre_deadline_runs.py
# Planifie des runs à H-48, H-24, H-12, H-6 et H-1 avant la deadline de chaque GW (futures).
# Ajoute --refresh pour supprimer et recréer toutes les tâches "FPL Pre-Deadline".
# Compatible locales FR (jj/mm/aaaa) et EN (mm/jj/aaaa) pour schtasks.

import argparse
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests

UA = {"User-Agent": "Mozilla/5.0 (FPL-PreDeadline/1.3)"}
BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"
OFFSETS_HOURS = [48, 24, 12, 6, 1]
TASK_PREFIX_DEFAULT = "FPL Pre-Deadline"

def get_project_paths():
    project_dir = Path.home() / "Documents" / "fpl_project"
    run_ps1 = project_dir / "scripts" / "run_snapshot.ps1"
    return project_dir, run_ps1

def run_cmd(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")

def list_tasks_with_prefix(prefix: str):
    res = run_cmd(["schtasks", "/Query", "/FO", "LIST", "/V"])
    if res.returncode != 0:
        return []
    names = []
    for line in res.stdout.splitlines():
        if line.strip().startswith("TaskName:"):
            name = line.split(":", 1)[1].strip()
            if prefix in name:
                names.append(name)
    return names

def delete_task(name: str):
    return run_cmd(["schtasks", "/Delete", "/TN", name, "/F"])

def fetch_events():
    r = requests.get(BOOTSTRAP, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("events", []) or []

def parse_deadline_to_local(deadline_str):
    # ex: "2025-08-23T10:30:00Z"
    dt_utc = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
    return dt_utc.astimezone(datetime.now().astimezone().tzinfo)

def fmt_date(dt_local, mode="mdy"):
    return dt_local.strftime("%m/%d/%Y") if mode == "mdy" else dt_local.strftime("%d/%m/%Y")

def fmt_time(dt_local):
    return dt_local.strftime("%H:%M")  # 24h

def create_once_task(task_name, run_dt_local, run_ps1):
    """
    Tente MM/JJ/AAAA puis JJ/MM/AAAA suivant la langue Windows.
    """
    def _build(sd, st):
        return [
            "schtasks","/Create",
            "/TN", task_name,
            "/SC","ONCE",
            "/SD", sd,
            "/ST", st,
            "/RL","LIMITED",
            "/F",
            "/TR", f'powershell -NoProfile -ExecutionPolicy Bypass -File "{run_ps1}"'
        ]
    st = fmt_time(run_dt_local)
    # 1) MM/JJ/AAAA
    sd = fmt_date(run_dt_local, "mdy")
    r1 = run_cmd(_build(sd, st))
    if r1.returncode == 0:
        return 0, r1.stdout
    # 2) JJ/MM/AAAA si erreur de date
    msg = (r1.stdout + r1.stderr).lower()
    if any(m in msg for m in ["date de d", "incorrecte", "invalid start date", "start date is invalid"]):
        sd2 = fmt_date(run_dt_local, "dmy")
        r2 = run_cmd(_build(sd2, st))
        return r2.returncode, (r2.stdout + r2.stderr)
    return r1.returncode, (r1.stdout + r1.stderr)

def main():
    ap = argparse.ArgumentParser(description="Schedule snapshots before GW deadlines.")
    ap.add_argument("--refresh", action="store_true", help="Delete and recreate all 'FPL Pre-Deadline' tasks.")
    ap.add_argument("--prefix",  default=TASK_PREFIX_DEFAULT, help="Task name prefix (default: %(default)s)")
    args = ap.parse_args()

    project_dir, run_ps1 = get_project_paths()
    if not run_ps1.exists():
        print("ERROR: Script not found:", run_ps1)
        print('Run S2.D1 to create "run_snapshot.ps1" first.')
        sys.exit(2)

    # Option: purge tasks first
    deleted = 0
    if args.refresh:
        existing = list_tasks_with_prefix(args.prefix)
        for name in existing:
            d = delete_task(name)
            if d.returncode == 0:
                deleted += 1
                print(f"[DEL] {name}")
            else:
                print(f"[DEL-ERR] {name} -> {(d.stdout + d.stderr).strip()}")

    try:
        events = fetch_events()
    except Exception as e:
        print("ERROR: cannot fetch bootstrap-static:", e)
        sys.exit(2)

    now_local = datetime.now().astimezone()
    planned, errors = [], []

    for ev in events:
        gw = ev.get("id"); dl = ev.get("deadline_time")
        if not gw or not dl: 
            continue
        dl_local = parse_deadline_to_local(dl)
        if dl_local <= now_local:
            continue  # ignore deadlines déjà passées
        for h in OFFSETS_HOURS:
            run_dt_local = dl_local - timedelta(hours=h)
            if run_dt_local <= now_local:
                continue
            task_name = f"{args.prefix} GW{gw} - {h}h"
            rc, out = create_once_task(task_name, run_dt_local, run_ps1)
            if rc == 0:
                planned.append((task_name, gw, h, run_dt_local.strftime("%Y-%m-%d %H:%M")))
                print(f"[OK] {task_name} -> {run_dt_local.strftime('%Y-%m-%d %H:%M')}")
            else:
                errors.append((task_name, out.strip()))
                print(f"[ERR] {task_name} -> {out.strip()}")

    # Récap CSV
    schedules_dir = project_dir / "data" / "schedules"
    schedules_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    csv_path = schedules_dir / f"scheduled_tasks_{ts}.csv"
    try:
        with csv_path.open("w", encoding="utf-8") as f:
            f.write("task_name,gw,offset_hours,run_time_local\n")
            for name, gw_id, h, t in planned:
                f.write(f"{name},{gw_id},{h},{t}\n")
        print("Summary CSV:", csv_path)
    except Exception as e:
        print("WARN: could not write summary CSV:", e)

    print(f"\nSummary: {len(planned)} tasks created, {len(errors)} errors. Purged: {deleted}")
    if errors:
        print("First error example:")
        print(errors[0][0], "->", errors[0][1])

if __name__ == "__main__":
    main()
