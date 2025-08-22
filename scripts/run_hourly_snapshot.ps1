# scripts/run_hourly_snapshot.ps1
# Exécute snapshot + deltas + validation, capture stdout/stderr, écrit un log, et signale les ALERTs.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ----- chemins absolus -----
$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Py        = Join-Path $ProjectDir '.venv\Scripts\python.exe'
$Runner    = Join-Path $ProjectDir 'scripts\run_pipeline_and_validate.py'
$DataDir   = Join-Path $ProjectDir 'data'
$LogsDir   = Join-Path $DataDir 'logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

# ----- horodatage & log -----
$ts       = Get-Date -Format 'yyyyMMdd-HHmmss'
$logFile  = Join-Path $LogsDir "hourly_$ts.log"
$startUtc = (Get-Date).ToUniversalTime()

"=== RUN $ts ===" | Out-File -FilePath $logFile -Encoding UTF8

# ----- lancer Python via .NET pour capturer stdout/stderr -----
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName               = $Py
$psi.Arguments              = "`"$Runner`" --snapshot --deltas"
$psi.WorkingDirectory       = $ProjectDir
$psi.UseShellExecute        = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true

$proc = [System.Diagnostics.Process]::Start($psi)
$stdout = $proc.StandardOutput.ReadToEnd()
$stderr = $proc.StandardError.ReadToEnd()
$proc.WaitForExit()

# écrire les flux dans le log
$stdout | Out-File -FilePath $logFile -Append -Encoding UTF8
$stderr | Out-File -FilePath $logFile -Append -Encoding UTF8
("EXIT CODE: {0}" -f $proc.ExitCode) | Out-File -FilePath $logFile -Append -Encoding UTF8

# ----- lire le SUMMARY de la validation -----
$reportPath = Join-Path $DataDir 'validation_report.txt'
$summary = $null
if (Test-Path $reportPath) {
  $summary = (Get-Content $reportPath | Where-Object { $_ -match '^SUMMARY:' } | Select-Object -Last 1)
  if ($summary) { $summary | Out-File -FilePath $logFile -Append -Encoding UTF8 }
} else {
  "No validation_report.txt found." | Out-File -FilePath $logFile -Append -Encoding UTF8
}

# ----- détecter un ALERT écrit pendant ce run -----
$alertFiles = @()
if (Test-Path $DataDir) {
  $alertFiles = @( Get-ChildItem -Path $DataDir -Filter 'ALERT_validation_report_*.txt' -ErrorAction SilentlyContinue |
                   Where-Object { $_.LastWriteTimeUtc -ge $startUtc } )
}

if ($alertFiles.Length -gt 0) {
  "ALERT files created:" | Out-File -FilePath $logFile -Append -Encoding UTF8
  $alertFiles | ForEach-Object { $_.FullName } | Out-File -FilePath $logFile -Append -Encoding UTF8
  Write-Host ("ALERT - see {0}" -f $alertFiles[-1].FullName)
  exit 1
} else {
  if (-not $summary) { $summary = "SUMMARY: (not found)" }
  Write-Host ("OK - {0}" -f $summary)
  exit ($proc.ExitCode)
}
