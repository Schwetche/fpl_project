# setup_tasks.ps1
# Configure tâches planifiées Windows pour snapshots FPL (auto GW)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectDir = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'fpl_project'
$Script     = Join-Path $ProjectDir 'scripts\run_snapshot.ps1'
$Deadlines  = Join-Path $ProjectDir 'data\deadlines.csv'

if (-not (Test-Path $Deadlines)) {
    Write-Host "[FAIL] deadlines.csv manquant dans data/"
    exit 1
}

# ----- lire la prochaine deadline -----
$csv = Import-Csv $Deadlines
$now = Get-Date
$next = $csv | Where-Object { [datetime]$_.deadline_time -gt $now } | Sort-Object {[datetime]$_.deadline_time} | Select-Object -First 1

if (-not $next) {
    Write-Host "[FAIL] Impossible de trouver une prochaine deadline dans deadlines.csv"
    exit 1
}

$deadline = [datetime]$next.deadline_time
Write-Host "[INFO] Prochaine deadline détectée: $deadline (GW$($next.event))"

# ----- helper pour créer une tâche planifiée -----
function Register-FPLTask($name, $triggerTime, $comment) {
    Write-Host "[TASK] $name -> $comment ($triggerTime)"
    schtasks /Delete /TN $name /F 2>$null | Out-Null
    schtasks /Create /TN $name /TR "powershell -ExecutionPolicy Bypass -File `"$Script`"" `
        /SC ONCE /ST $triggerTime /SD $triggerTime.ToString("dd/MM/yyyy") /RL HIGHEST /F | Out-Null
}

# ----- 1. Snapshot quotidien 11:00 -----
schtasks /Delete /TN "FPL_Daily_11" /F 2>$null | Out-Null
schtasks /Create /TN "FPL_Daily_11" /TR "powershell -ExecutionPolicy Bypass -File `"$Script`"" `
    /SC DAILY /ST 11:00 /RL HIGHEST /F | Out-Null
Write-Host "[TASK] FPL_Daily_11 -> Daily snapshot 11:00"

# ----- 2. Pré-deadline H-48, H-24, H-12, H-6, H-1 -----
$offsets = @{
    "FPL_PreDeadline_H48" = New-TimeSpan -Hours 48
    "FPL_PreDeadline_H24" = New-TimeSpan -Hours 24
    "FPL_PreDeadline_H12" = New-TimeSpan -Hours 12
    "FPL_PreDeadline_H6"  = New-TimeSpan -Hours 6
    "FPL_PreDeadline_H1"  = New-TimeSpan -Hours 1
}

foreach ($name in $offsets.Keys) {
    $dt = $deadline - $offsets[$name]
    if ($dt -gt $now) {
        Register-FPLTask $name $dt "$name run"
    } else {
        Write-Host "[SKIP] $name déjà passé ($dt)"
    }
}

Write-Host "[OK] All tasks scheduled automatically until next deadline."
