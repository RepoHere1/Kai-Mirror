# WATCHDOG KEEPALIVE (2026-11-05) - the supervisor of the watchdog.
# PROBLEM: three scheduled-task generations of watchdog.ps1 started, acquired
# the mutex, and then wedged BEFORE their first log line - alive, silent,
# holding the Global\KaiWatchdog mutex, so no other instance could start.
# Root cause unknown (task-context curse on this box); this script makes it
# irrelevant. It is deliberately TOO STUPID TO WEDGE: no network, no jobs, no
# HTTP - just file timestamps and process kills.
#   1. Heartbeat age > 180s => any powershell running watchdog.ps1 is wedged:
#      kill it (the mutex dies with it).
#   2. No watchdog process alive => launch one detached (Start-Process, no
#      -Wait). It logs 'watchdog v3 started' and loops with breadcrumbs.
# Scheduled every 2 minutes. Extra watchdog instances are impossible: the
# mutex inside watchdog.ps1 makes launches single-instance.
$ErrorActionPreference = 'SilentlyContinue'
$WdScript = Join-Path $PSScriptRoot 'watchdog.ps1'
$WdLog    = Join-Path $PSScriptRoot 'watchdog.log'

# --- 1. heartbeat staleness -> kill wedged holders -------------------------
$lastBeat = [datetime]::MinValue
if (Test-Path $WdLog) {
  $lines = Get-Content $WdLog -Tail 30
  foreach ($ln in $lines) {
    if ($ln -match '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (heartbeat|loop#|watchdog v3 started|LOG-WRITE)') {
      $t = [datetime]::ParseExact($Matches[1], 'yyyy-MM-dd HH:mm:ss', $null)
      if ($t -gt $lastBeat) { $lastBeat = $t }
    }
  }
}
$age = ((Get-Date) - $lastBeat).TotalSeconds
$alive = @(Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" | Where-Object { $_.CommandLine -match 'watchdog\.ps1' })
if ($lastBeat -gt [datetime]::MinValue -and $age -gt 180 -and $alive.Count -gt 0) {
  Add-Content $WdLog ("{0} KEEPALIVE: heartbeat stale {1:n0}s - killing {2} wedged watchdog(s)" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $age, $alive.Count)
  foreach ($p in $alive) { try { Stop-Process -Id $p.ProcessId -Force } catch { } }
  Start-Sleep -Seconds 1
  $alive = @()
}
# --- 2. no live watchdog -> launch one -------------------------------------
# HEARTBEAT VETO (2026-11-11): the CIM process probe above went intermittently
# blind on this box - 543 false 'no watchdog alive - launching' fires between
# 2026-11-09 17:21 and 2026-11-11 09:05 while the real watchdog (looping every
# 60s, strikes 0/0) was alive the whole time; every false fire spawned a hidden
# powershell that the mutex turned away. The heartbeat parsed in step 1 is the
# PRIMARY liveness proof: a FRESH (<180s) heartbeat vetoes a launch no matter
# what the process probe claims. Process-match only breaks ties when the log is
# stale or empty (true death, or first boot before any log line exists).
$hbFresh = ($lastBeat -gt [datetime]::MinValue -and $age -le 180)
if (-not $hbFresh -and $alive.Count -eq 0) {
  Add-Content $WdLog ("{0} KEEPALIVE: no watchdog alive - launching" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
  Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden `
    -ArgumentList '-NoProfile', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', $WdScript
}
exit 0
