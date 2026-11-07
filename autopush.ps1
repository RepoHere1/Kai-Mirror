# autopush.ps1 - commit and push tracked changes in BOTH repos (DRY + LIVE).
# Run by Windows Task Scheduler every 15 minutes (task name: KaiAutoPush).
# Safety: stages only tracked files (git add -u) plus the backtester's own files.
# Never force-pushes, never adds untracked probe junk, never touches state.json /
# trading.db / .env (all gitignored). Log: autopush.log (gitignored).
$ErrorActionPreference = 'Continue'
# 2026-11-07: portable sibling-repo discovery (old box hardcoded C:\Users\darri\...).
$KaiRoot = Split-Path $PSScriptRoot -Parent
$repos = @(
  @{ Dir = Join-Path $KaiRoot 'Kai-Mirror'; Extra = @('backtest.js', 'strategies', 'backtest_results.json') },
  @{ Dir = Join-Path $KaiRoot 'Kai-LIVE';   Extra = @('backtest.js', 'strategies') }
)
$log = Join-Path (Join-Path $KaiRoot 'Kai-Mirror') 'autopush.log'
function Write-Log($m) { Add-Content -LiteralPath $log -Value ((Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m) }
foreach ($r in $repos) {
  $d = $r.Dir
  if (-not (Test-Path -LiteralPath (Join-Path $d '.git'))) { Write-Log "SKIP $d (no .git)"; continue }
  git -C $d add -u 2>$null
  foreach ($x in $r.Extra) { if (Test-Path -LiteralPath (Join-Path $d $x)) { git -C $d add -- $x 2>$null } }
  git -C $d diff --cached --quiet 2>$null
  if ($LASTEXITCODE -eq 0) { Write-Log "$d no changes"; continue }
  $msg = 'autopush: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm')
  git -C $d commit -q -m $msg 2>$null
  if ($LASTEXITCODE -ne 0) { Write-Log "$d COMMIT FAILED"; continue }
  $out = git -C $d push origin main 2>&1 | Out-String
  if ($LASTEXITCODE -eq 0) { Write-Log "$d committed + pushed: $(git -C $d rev-parse --short HEAD)" }
  else { Write-Log "$d PUSH FAILED: $($out.Trim())" }
}
