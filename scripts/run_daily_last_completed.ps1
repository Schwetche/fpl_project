# scripts/run_daily_last_completed.ps1
# Tous les jours (10:00 Zurich) : met à jour la dernière GW terminée (par joueur + par match) + merged + validation.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Py        = Join-Path $ProjectDir '.venv\Scripts\python.exe'
$LogsDir   = Join-Path $ProjectDir 'data\logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

$ts  = Get-Date -Format 'yyyyMMdd-HHmmss'
$log = Join-Path $LogsDir "daily_last_completed_$ts.log"

function Append-Log([string]$text) { $text | Out-File -FilePath $log -Append -Encoding UTF8 }

function RunPyNoArgs([string]$relScript) {
  $script = Join-Path $ProjectDir $relScript
  Append-Log ("[STEP] python {0}" -f $relScript)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName               = $Py
  $psi.Arguments              = ("`"{0}`"" -f $script)
  $psi.WorkingDirectory       = $ProjectDir
  $psi.UseShellExecute        = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  Append-Log $out
  if ($err) { Append-Log $err }
  if ($p.ExitCode -ne 0) { throw "Python exit $($p.ExitCode) for $relScript" }
}

function RunPyGw([string]$relScript, [int]$gw) {
  $script = Join-Path $ProjectDir $relScript
  Append-Log ("[STEP] python {0} --gw {1}" -f $relScript, $gw)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName               = $Py
  # >>> On câble explicitement --gw {gw} dans la ligne d'arguments <<<
  $psi.Arguments              = ("`"{0}`" --gw {1}" -f $script, $gw)
  $psi.WorkingDirectory       = $ProjectDir
  $psi.UseShellExecute        = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  Append-Log $out
  if ($err) { Append-Log $err }
  if ($p.ExitCode -ne 0) { throw "Python exit $($p.ExitCode) for $relScript" }
}

"=== DAILY LAST COMPLETED $ts ===" | Out-File -FilePath $log -Encoding UTF8
Append-Log ("[INFO] Python: {0}" -f $Py)
Append-Log ("[INFO] CWD: {0}" -f $ProjectDir)

# Détecte la dernière GW terminée depuis bootstrap
$UA = @{ 'User-Agent' = 'Mozilla/5.0 (FPL-LastGW/1.2)' }
$boot = Invoke-RestMethod -Uri 'https://fantasy.premierleague.com/api/bootstrap-static/' -Headers $UA -TimeoutSec 30
$events = @($boot.events)

# priorité: finished==true, sinon data_checked==true, sinon is_current
$done = $events | Where-Object { $_.finished -eq $true -or $_.data_checked -eq $true } | Sort-Object id | Select-Object -Last 1
if (-not $done) { $done = $events | Where-Object { $_.is_current -eq $true } | Select-Object -First 1 }
if (-not $done) {
  Append-Log "[WARN] No completed/current GW found. Exiting."
  "EXIT CODE: 0" | Out-File -FilePath $log -Append -Encoding UTF8
  exit 0
}
[int]$gw = $done.id
Append-Log ("[INFO] last completed/current GW = {0}" -f $gw)

# 1) données courantes (toujours)
RunPyNoArgs "scripts\snapshot_players_raw.py"
RunPyNoArgs "scripts\snapshot_fixtures_and_diff.py"
RunPyNoArgs "scripts\build_cleaned_players.py"

# 2) GW N par joueur + par match (ARG EXPLICITE)
RunPyGw "scripts\build_one_gw_csv.py" $gw
RunPyGw "scripts\build_one_gw_permatch_csv.py" $gw

# 3) merged_gw.csv
RunPyNoArgs "scripts\build_merged_gw.py"

# 4) validation
RunPyNoArgs "scripts\validate_fpl_pipeline.py"

"EXIT CODE: 0" | Out-File -FilePath $log -Append -Encoding UTF8
