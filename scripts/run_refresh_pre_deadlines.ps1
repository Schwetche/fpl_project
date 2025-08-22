# scripts/run_refresh_pre_deadlines.ps1
# (Ré)crée les tâches pré-deadline GW :
# - H-48 -> lance run_predeadline48h.ps1 (gwX + merged)
# - H-24/H-12/H-6/H-1 -> lance run_snapshot.ps1 (players_raw/fixtures/cleaned + deltas)
# Écrit un résumé CSV.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$VbsDefault = Join-Path $ProjectDir 'scripts\launch_snapshot_hidden.vbs'
$VbsGW      = Join-Path $ProjectDir 'scripts\launch_predeadline48h_hidden.vbs'
$OutDir     = Join-Path $ProjectDir 'data\schedules'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Charges les events FPL
$UA = @{ 'User-Agent' = 'Mozilla/5.0 (FPL-PreDeadline/1.2)' }
$boot = Invoke-RestMethod -Uri 'https://fantasy.premierleague.com/api/bootstrap-static/' -Headers $UA -TimeoutSec 30
$events = @($boot.events)

# Offsets souhaités (heures) — H-48 utilise VbsGW, le reste VbsDefault
$offsets = 48,24,12,6,1
$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
$csv = Join-Path $OutDir "scheduled_tasks_$ts.csv"
"gw,offset_hours,task_name,start_local,status" | Out-File -FilePath $csv -Encoding UTF8

foreach ($e in $events) {
  if (-not $e.deadline_time) { continue }
  # Convertit la deadline UTC -> locale
  $dl = [datetime]::Parse($e.deadline_time).ToLocalTime()
  $gw = $e.id
  foreach ($h in $offsets) {
    $runAt = $dl.AddHours(-$h)
    if ($runAt -lt (Get-Date)) { 
      # On ne crée pas les exécutions déjà passées
      "$gw,$h,FPL Pre-Deadline GW$gw - ${h}h,SKIPPED(past)" | Out-File -FilePath $csv -Append -Encoding UTF8
      continue
    }
    $task = "FPL Pre-Deadline GW$gw - ${h}h"
    try {
      schtasks /Delete /TN "$task" /F *> $null
    } catch {}

    $vbs = $(if ($h -eq 48) { $VbsGW } else { $VbsDefault })
    $hhmm = $runAt.ToString("HH:mm")
    $date = $runAt.ToString("dd/MM/yyyy")
    # /SC ONCE avec Date+Heure locales
    schtasks /Create /TN "$task" /SC ONCE /ST $hhmm /SD $date /RL LIMITED /F /TR "wscript.exe `"$vbs`""
    if ($LASTEXITCODE -eq 0) {
      "[OK] FPL Pre-Deadline GW$gw - ${h}h -> $($runAt.ToString('yyyy-MM-dd HH:mm'))" | Write-Host
      "$gw,$h,$task,$($runAt.ToString('yyyy-MM-dd HH:mm')),OK" | Out-File -FilePath $csv -Append -Encoding UTF8
    } else {
      "[ERR] FPL Pre-Deadline GW$gw - ${h}h -> scheduling error" | Write-Host
      "$gw,$h,$task,$($runAt.ToString('yyyy-MM-dd HH:mm')),ERROR" | Out-File -FilePath $csv -Append -Encoding UTF8
    }
  }
}

"Summary CSV: $csv" | Write-Host
