# scripts/run_global_test.ps1
# Lance la QA "full-only" et logge dans data\logs\global_test_*.log

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Py        = Join-Path $ProjectDir '.venv\Scripts\python.exe'
$ScriptPy  = Join-Path $ProjectDir 'scripts\run_global_test.py'
$LogsDir   = Join-Path $ProjectDir 'data\logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

# paramètres facultatifs : --with-api-check, --strict-api, --with-fixtures-diff
$ArgsLine = $args -join ' '

$ts  = Get-Date -Format 'yyyyMMdd-HHmmss'
$log = Join-Path $LogsDir "global_test_$ts.log"

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName               = $Py
$psi.Arguments              = "`"$ScriptPy`" $ArgsLine"
$psi.WorkingDirectory       = $ProjectDir
$psi.UseShellExecute        = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true

$proc   = [System.Diagnostics.Process]::Start($psi)
$stdout = $proc.StandardOutput.ReadToEnd()
$stderr = $proc.StandardError.ReadToEnd()
$proc.WaitForExit()

"=== GLOBAL TEST $ts ===" | Out-File -FilePath $log -Encoding UTF8
$stdout | Out-File -FilePath $log -Append -Encoding UTF8
$stderr | Out-File -FilePath $log -Append -Encoding UTF8
("EXIT CODE: {0}" -f $proc.ExitCode) | Out-File -FilePath $log -Append -Encoding UTF8

if ($proc.ExitCode -eq 0) {
  Write-Host "OK - global test passed"
  exit 0
} else {
  Write-Host "ERROR - global test failed, see log:" $log
  exit $proc.ExitCode
}
