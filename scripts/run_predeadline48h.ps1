# scripts/run_predeadline48h.ps1
# H-48 : uniquement players_raw + fixtures + cleaned + validation (plus de rebuild GW ici)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Py        = Join-Path $ProjectDir '.venv\Scripts\python.exe'
$LogsDir   = Join-Path $ProjectDir 'data\logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

$ts  = Get-Date -Format 'yyyyMMdd-HHmmss'
$log = Join-Path $LogsDir "predeadline48h_$ts.log"

function Append-Log([string]$text) { $text | Out-File -FilePath $log -Append -Encoding UTF8 }
function RunPyArgs([string]$relScript, [string]$args = "") {
  $script = Join-Path $ProjectDir $relScript
  Append-Log ("[STEP] python {0} {1}" -f $relScript, $args)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName               = $Py
  $psi.Arguments              = ("`"{0}`" {1}" -f $script, $args)
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

"=== PREDEADLINE48H $ts ===" | Out-File -FilePath $log -Encoding UTF8
RunPyArgs "scripts\snapshot_players_raw.py"
RunPyArgs "scripts\snapshot_fixtures_and_diff.py"
RunPyArgs "scripts\build_cleaned_players.py"
RunPyArgs "scripts\validate_fpl_pipeline.py"
"EXIT CODE: 0" | Out-File -FilePath $log -Append -Encoding UTF8
