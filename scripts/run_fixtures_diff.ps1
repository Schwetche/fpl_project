# scripts/run_fixtures_diff.ps1
# Snapshot des fixtures + diff + ALERT si changement, avec log.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Py        = Join-Path $ProjectDir '.venv\Scripts\python.exe'
$ScriptPy  = Join-Path $ProjectDir 'scripts\snapshot_fixtures_and_diff.py'
$LogsDir   = Join-Path $ProjectDir 'data\logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
$log = Join-Path $LogsDir "fixtures_diff_$ts.log"

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName               = $Py
$psi.Arguments              = "`"$ScriptPy`""
$psi.WorkingDirectory       = $ProjectDir
$psi.UseShellExecute        = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true

$proc = [System.Diagnostics.Process]::Start($psi)
$stdout = $proc.StandardOutput.ReadToEnd()
$stderr = $proc.StandardError.ReadToEnd()
$proc.WaitForExit()

"=== FIXTURES $ts ===" | Out-File -FilePath $log -Encoding UTF8
$stdout | Out-File -FilePath $log -Append -Encoding UTF8
$stderr | Out-File -FilePath $log -Append -Encoding UTF8
("EXIT CODE: {0}" -f $proc.ExitCode) | Out-File -FilePath $log -Append -Encoding UTF8

if ($proc.ExitCode -eq 0) {
  Write-Host "OK - fixtures diff done"
  exit 0
} else {
  Write-Host "ERROR - fixtures diff failed, see log: $log"
  exit $proc.ExitCode
}
