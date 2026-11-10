# Kai-Mirror DRY Server - Persistent Background Launcher (portable: repo = script dir)
$repo = $PSScriptRoot
$serverPath = Join-Path $repo 'server.js'
$outLog = Join-Path $repo 'server-out.log'
$errLog = Join-Path $repo 'server-err.log'
$port = 8787

# Kill any existing node on this port
$existing = netstat -ano | Select-String ":$port "
if ($existing) {
    $foundPids = $existing | ForEach-Object { ($_ -split '\s+')[-1] } | Sort-Object -Unique
    foreach ($foundPid in $foundPids) {
        if ($foundPid -ne '0') { taskkill /F /PID $foundPid 2>$null }
    }
    Start-Sleep 1
}

# Start server with redirected I/O (use separate files for stdout/stderr)
# 2026-11-07: --max-old-space-size=4096 after LIVE OOMed at the 2GB default cap
# 2026-11-10: 4096 -> 1536 (mirror). The 7.9GB box hit commit famine at 14:36
# (explorer.exe 4.2GB + chrome 2.5GB + cline 2.3GB) and BOTH books stalled
# (mirror loop_lag 74.7s, LIVE 6.9s). A 4096 ceiling per server is an 8GB
# promise on an 8GB box; measured mirror peak is 93MB heap after the 5555-row
# calibration backfill. 1536 = 16x headroom, bounded promise. LIVE: 1024.
# (heap-out-of-memory native crash ~51min after boot during the Kalshi REST outage).
$process = Start-Process node -ArgumentList '--max-old-space-size=1536',"`"$serverPath`"" -WorkingDirectory $repo -RedirectStandardOutput $outLog -RedirectStandardError $errLog -PassThru -WindowStyle Hidden

Write-Host "Kai-Mirror DRY started (PID: $($process.Id)) on port $port"
Write-Host "Stdout: $outLog"
Write-Host "Stderr: $errLog"
$process.Id | Out-File (Join-Path $repo 'server.pid') -Encoding ascii

# Verify it's up
Start-Sleep 3
for ($i=0; $i -lt 10; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$port/api/health" -TimeoutSec 2 -UseBasicParsing
        if ($resp.StatusCode -eq 200) {
            Write-Host "[OK] Server healthy on port $port"
            exit 0
        }
    } catch { }
    Start-Sleep 1
}
Write-Warning "Server may not be fully ready yet"