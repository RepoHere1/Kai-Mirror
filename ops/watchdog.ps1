# Kai autonomous watchdog v2 - 2026-11-03 (crash-cause fix)
#
# WHY v2 EXISTS (root cause of the "SERVER DOWN / boots 23 / lanes 0 on" tape):
#   v1 probed /api/health with a 3 SECOND timeout. When the server was merely
#   SLOW (SQLite write, busy event loop), v1 concluded DOWN and ran
#   "kctl restart", which did taskkill /F = SIGKILL. That produced:
#     - no crash.log entry (SIGKILL runs no handler),
#     - a possible TORN state.json (non-atomic save),
#     - the next boot falling back to defaultState(): 9 lanes -> 0, and every
#       risk gate (price floor, fair floor, clip, exposure cap) silently reset.
#   The watchdog was not saving the machine, it was disarming it.
#
# v2 RULES:
#   1. TCP connect REFUSED = genuinely down  -> eligible for restart.
#   2. Port LISTENING      = alive, however slow health is -> NEVER kill it.
#   3. Two consecutive confirmed-down checks before acting (no flapping kills).
#   4. Minimum 5 minutes between restarts of the same server (no restart loop).
#   5. Stop is GRACEFUL: POST /api/shutdown flushes state atomically first;
#      force kill only if the server refuses to exit.
#   6. It NEVER touches lane flags. The operator's button push is law.
$mtx = New-Object System.Threading.Mutex($false, 'Global\KaiWatchdog')
$acquired = $false
try { $acquired = $mtx.WaitOne(0) }
catch [System.Threading.AbandonedMutexException] { $acquired = $true }   # previous owner was killed: we own it now
if (-not $acquired) { exit 1 }   # another watchdog already owns the job
# STABLE PATHS (v3 - 2026-11-03). The watchdog used to live in
# %TEMP%\opencode, which Windows may clean at any time and which no version
# control covers. A watchdog that vanishes on a temp cleanup stops healing the
# trading server. Both scripts now live in the REPOSITORY.
$Kctl = Join-Path $PSScriptRoot 'kctl.ps1'

# BOUNDED kctl runner (2026-11-04 incident): kctl wedged for 40+ minutes
# inside an unbounded netstat call, and because the watchdog ran it with
# Start-Process -Wait, the WATCHDOG wedged with it and both books stayed
# down. kctl is now started hidden WITHOUT -Wait, then Wait-Process caps it
# at 3 minutes. On timeout the kctl process TREE is killed (its node child
# survives — kctl only kills what it owns when asked to stop; a start that
# already launched keeps its server). The watchdog always keeps looping.
function Invoke-KctlRestart([string]$srv) {
  $p = Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -PassThru `
    -ArgumentList '-NoProfile', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', $Kctl, $srv, 'restart'
  $done = $true
  try { Wait-Process -Id $p.Id -Timeout 180 -ErrorAction Stop } catch { $done = $false }
  if ($done) {
    WLog "$srv kctl restart finished"
  } else {
    WLog "$srv kctl restart exceeded 180s - killing kctl tree, keeping node children"
    try { & taskkill.exe /PID $p.Id /T /F 2>&1 | Out-Null } catch { }
  }
}
$Log  = Join-Path $PSScriptRoot 'watchdog.log'
function WLog([string]$m) { Add-Content -Path $Log -Value ("{0} {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m) }
WLog 'watchdog v3 started (repo-stable scripts, ownership-safe pids, graceful recovery)'

function Test-PortListening([int]$Port) {
  # HARDENED 2026-11-04: the old synchronous Connect() has NO timeout and was
  # observed freezing the whole watchdog (stuck 20:43 2026-11-03 -> next
  # morning, no heartbeats, mutex held). Async connect with a 3s cap.
  # Semantics unchanged (v2 rule 2): ConnectionRefused = down; anything else,
  # including a connect timeout, = alive so we never kill a merely-slow server.
  $c = New-Object System.Net.Sockets.TcpClient
  try {
    $iar = $c.BeginConnect('127.0.0.1', $Port, $null, $null)
    if ($iar.AsyncWaitHandle.WaitOne(3000)) {
      try {
        $c.EndConnect($iar)
        return $true
      } catch {
        $ex = $_.Exception
        if ($ex.InnerException) { $ex = $ex.InnerException }
        if ($ex -is [System.Net.Sockets.SocketException]) { return ($ex.SocketErrorCode -ne 'ConnectionRefused') }
        return $true
      }
    }
    return $true   # timed out mid-connect: port is not refusing - treat as alive
  } finally {
    try { $c.Close() } catch { }
  }
}

$strikes = @{ 'mirror' = 0; 'live' = 0 }
$up = @{ 'mirror' = $false; 'live' = $false }
$RECYCLE_MIN = 180

# RECYCLE (2026-11-04): long-lived node processes on THIS machine lose the
# ability to open NEW outbound connections while existing sockets keep working
# (both books: 'network: fetch failed' on every Kalshi read while a fresh
# process fetched the same URL in 1.9s; Coinbase WS + local HTTP stayed up).
# Until the root cause is found upstream, bound the damage: gracefully recycle
# any server whose OWN /api/health reports more than RECYCLE_MIN minutes of
# uptime. A slow or failed health probe reads as uptime 0 - a merely-slow
# server is NEVER recycled (v2 rule 2). Graceful restart flushes state
# atomically; boots cost ~15s.
function Get-UpSec([int]$Port) {
  try {
    $h = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/api/health" -TimeoutSec 8 -UseBasicParsing
    $j = $h.Content | ConvertFrom-Json
    return [int]$j.uptime_sec
  } catch { return 0 }
}
$tick = 0
$lastRestart = @{ 'mirror' = (Get-Date).AddMinutes(-10); 'live' = (Get-Date).AddMinutes(-10) }

while ($true) {
  foreach ($srv in 'mirror','live') {
    $port = if ($srv -eq 'mirror') { 8787 } else { 8788 }
    if (Test-PortListening $port) {
      if ($strikes[$srv] -gt 0) { WLog "$srv port listening again - strikes cleared" }
      $strikes[$srv] = 0
      $up[$srv] = $true
      # AGE RECYCLE: probe the server's own uptime; recycle past RECYCLE_MIN.
      $upSec = Get-UpSec $port
      if ($upSec -gt ($RECYCLE_MIN * 60)) {
        $cd = ((Get-Date) - $lastRestart[$srv]).TotalMinutes
        if ($cd -ge 5) {
          WLog ("{0} uptime {1}m > {2}m - graceful RECYCLE (fetch-layer rot bound)" -f $srv, [int]($upSec/60), $RECYCLE_MIN)
        try { Invoke-KctlRestart $srv } catch { WLog "$srv recycle launch failed: $($_.Exception.Message)" }
        $lastRestart[$srv] = Get-Date
        }
      }
      continue
    }
    $up[$srv] = $false
    $strikes[$srv] = $strikes[$srv] + 1
    WLog "$srv port REFUSED (strike $($strikes[$srv])/2)"
    if ($strikes[$srv] -lt 2) { continue }
    $mins = ((Get-Date) - $lastRestart[$srv]).TotalMinutes
    if ($mins -lt 5) { WLog "$srv cooldown $([int]$mins)m - restart skipped"; continue }
    WLog "$srv confirmed down - issuing GRACEFUL restart"
    # BOUNDED (2026-11-04 incident): never Start-Process -Wait on kctl again.
    try { Invoke-KctlRestart $srv } catch { WLog "$srv restart launch failed: $($_.Exception.Message)" }
    $lastRestart[$srv] = Get-Date
    $strikes[$srv] = 0
    WLog "$srv restart issued"
  }
  # HEARTBEAT: a watchdog that cannot prove it is alive is a watchdog nobody
  # can trust. Every pass states what it saw, so "it did nothing" and "it never
  # ran" can never be confused again.
  $tick += 1
  WLog ("heartbeat #{0} mirror={1} live={2} strikes=({3}/{4})" -f $tick, $(if ($up['mirror']) {'up'} else {'DOWN'}), $(if ($up['live']) {'up'} else {'DOWN'}), $strikes['mirror'], $strikes['live'])
  Start-Sleep -Seconds 60
}
