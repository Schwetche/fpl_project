# scripts/run_snapshot.ps1
# Orchestration snapshot + deltas + validation + global test
# Ajout résumé final clair et ALERT si besoin

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ----- chemins -----
$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Py        = Join-Path $ProjectDir '.venv\Scripts\python.exe'
$Runner    = Join-Path $ProjectDir 'scripts\run_pipeline_and_validate.py'
$GlobalTest = Join-Path $ProjectDir 'scripts\run_global_test.py'
$DataDir   = Join-Path $ProjectDir 'data'
$LogsDir   = Join-Path $DataDir 'logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

# ----- horodatage & log -----
$ts       = Get-Date -Format 'yyyyMMdd-HHmmss'
$logFile  = Join-Path $LogsDir "snapshot_$ts.log"
$startUtc = (Get-Date).ToUniversalTime()
"=== RUN $ts ===" | Out-File -FilePath $logFile -Encoding UTF8

# ----- exécution orchestrateur -----
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $Py
$psi.Arguments = "`"$Runner`" --snapshot --deltas"
$psi.WorkingDirectory = $ProjectDir
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true

$proc = [System.Diagnostics.Process]::Start($psi)
$stdout = $proc.StandardOutput.ReadToEnd()
$stderr = $proc.StandardError.ReadToEnd()
$proc.WaitForExit()
$stdout | Out-File -FilePath $logFile -Append -Encoding UTF8
$stderr | Out-File -FilePath $logFile -Append -Encoding UTF8
("EXIT CODE: {0}" -f $proc.ExitCode) | Out-File -FilePath $logFile -Append -Encoding UTF8

# ----- exécution global test -----
$gt = New-Object System.Diagnostics.ProcessStartInfo
$gt.FileName = $Py
$gt.Arguments = "`"$GlobalTest`" --with-api-check --with-fixtures-diff"
$gt.WorkingDirectory = $ProjectDir
$gt.UseShellExecute = $false
$gt.RedirectStandardOutput = $true
$gt.RedirectStandardError  = $true

$gproc = [System.Diagnostics.Process]::Start($gt)
$gout = $gproc.StandardOutput.ReadToEnd()
$gerr = $gproc.StandardError.ReadToEnd()
$gproc.WaitForExit()
$gout | Out-File -FilePath $logFile -Append -Encoding UTF8
$gerr | Out-File -FilePath $logFile -Append -Encoding UTF8
("GLOBALTEST EXIT CODE: {0}" -f $gproc.ExitCode) | Out-File -FilePath $logFile -Append -Encoding UTF8

# ----- Résumé final -----
$summary = ($gout | Select-String "^SUMMARY:.*" | Select-Object -Last 1).ToString()
if (-not $summary) { $summary = "SUMMARY: (not found)" }
$summary | Out-File -FilePath $logFile -Append -Encoding UTF8

if ($summary -like "*PASS*") {
  Write-Host "✅ OK - $summary"
  exit 0
}
elseif ($summary -like "*WARN*") {
  $alert = Join-Path $DataDir ("ALERT_validation_report_{0}.txt" -f $ts)
  "WARN detected in global test. See log $logFile" | Out-File $alert -Encoding UTF8
  Write-Host "⚠️ WARN - résumé: $summary (ALERT écrit: $alert)"
  exit 2
}
else {
  $alert = Join-Path $DataDir ("ALERT_validation_report_{0}.txt" -f $ts)
  "FAIL detected in global test. See log $logFile" | Out-File $alert -Encoding UTF8
  Write-Host "❌ FAIL - résumé: $summary (ALERT écrit: $alert)"
  exit 1
}
