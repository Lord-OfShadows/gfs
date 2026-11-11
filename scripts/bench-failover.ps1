# Repeated failover measurement: stop a chunkserver and time how long the
# coordinator takes to mark it unavailable and to finish re-replicating.
# Run from the repo root:  .\scripts\bench-failover.ps1 [-Runs 20] [-FileMB 20] [-Node cs2]
#
# All times come from the Docker VM's clock: T_stop from `docker inspect`
# (when the container's process exited), the rest from the coordinator's
# JSON log "time" fields. Results: results\failover-<timestamp>.csv

param(
    [int]$Runs = 20,
    [int]$FileMB = 20,
    [ValidateSet('cs1', 'cs2', 'cs3', 'cs4')][string]$Node = 'cs2'
)

. "$PSScriptRoot\lib.ps1"
$root = Split-Path -Parent $PSScriptRoot # repo root: scripts\ is one level down
Set-Location $root
Assert-RepoRoot
Assert-Docker

$RunTimeoutSec = 60
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$resultsDir = Join-Path $root 'results'
New-Item -ItemType Directory -Force $resultsDir | Out-Null
$csv = Join-Path $resultsDir "failover-$stamp.csv"
$file = Join-Path $root 'transfer\bench.bin'
$rows = New-Object System.Collections.Generic.List[object]

function Get-Stats([double[]]$Values) {
    $s = $Values | Sort-Object
    $n = $s.Count
    if ($n % 2 -eq 1) { $median = $s[($n - 1) / 2] } else { $median = ($s[$n / 2 - 1] + $s[$n / 2]) / 2 }
    $p95 = $s[[math]::Ceiling(0.95 * $n) - 1] # nearest-rank percentile
    return [pscustomobject]@{ Min = $s[0]; Median = $median; P95 = $p95; Max = $s[$n - 1] }
}

try {
    Write-Host "Building the image once..." -ForegroundColor Cyan
    Invoke-Docker -Quiet -DockerArgs @('compose', 'build', '--quiet')
    New-RandomFile -Path $file -SizeMB $FileMB

    for ($run = 1; $run -le $Runs; $run++) {
        $row = [ordered]@{ run = $run; detection_ms = ''; repair_ms = ''; total_ms = ''; repaired = ''; status = 'failed' }
        try {
            Invoke-Docker -Quiet -DockerArgs @('compose', 'down', '-v')
            Invoke-Docker -Quiet -DockerArgs @('compose', 'up', '-d')
            Wait-ClusterReady
            Invoke-Docker -Quiet -DockerArgs @('compose', 'run', '--rm', 'gfs', 'upload', '/transfer/bench.bin')

            Invoke-Docker -Quiet -DockerArgs @('compose', 'stop', $Node)
            $stop = Get-StopTime -Node $Node
            $r = Wait-Repair -Node $Node -Since $stop.Raw -TimeoutSec $RunTimeoutSec
            if ($r) {
                $row.detection_ms = [math]::Round(($r.Unavailable.Time - $stop.Time).TotalMilliseconds)
                $row.repair_ms = [math]::Round(($r.Finished.Time - $r.Unavailable.Time).TotalMilliseconds)
                $row.total_ms = [math]::Round(($r.Finished.Time - $stop.Time).TotalMilliseconds)
                $row.repaired = $r.Finished.Data.repaired
                $row.status = 'ok'
            } else {
                $row.status = 'timeout'
            }
        } catch {
            $row.status = 'error'
            Write-Host "  run $run error: $($_.Exception.Message)" -ForegroundColor Red
        }
        $rows.Add([pscustomobject]$row)
        $rows | Export-Csv -NoTypeInformation -Path $csv # rewrite each run, so partial results survive Ctrl+C
        Write-Host ("run {0,3}/{1}: {2,-7} detection {3,6} ms  repair {4,6} ms  total {5,6} ms  repaired {6}" -f `
                $run, $Runs, $row.status, $row.detection_ms, $row.repair_ms, $row.total_ms, $row.repaired)
    }
} finally {
    Write-Host 'Cleaning up (docker compose down -v)...'
    try { Invoke-Docker -Quiet -DockerArgs @('compose', 'down', '-v') } catch { Write-Host $_.Exception.Message -ForegroundColor Red }
    Remove-Item $file -ErrorAction SilentlyContinue
}

$ok = @($rows | Where-Object { $_.status -eq 'ok' })
Write-Host ''
Write-Host "Results: $csv" -ForegroundColor Cyan
Write-Host ("{0} of {1} runs succeeded, {2} failed. Stopped node: {3}, file: {4} MB" -f $ok.Count, $rows.Count, ($rows.Count - $ok.Count), $Node, $FileMB)
if ($ok.Count -gt 0) {
    Write-Host ''
    Write-Host ("{0,-14} {1,8} {2,8} {3,8} {4,8}" -f 'metric (ms)', 'min', 'median', 'p95', 'max')
    foreach ($m in 'detection_ms', 'repair_ms', 'total_ms') {
        $st = Get-Stats ([double[]]($ok | ForEach-Object { $_.$m }))
        Write-Host ("{0,-14} {1,8} {2,8} {3,8} {4,8}" -f $m, $st.Min, $st.Median, $st.P95, $st.Max)
    }
}

$cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name.Trim()
$os = Get-CimInstance Win32_OperatingSystem
Write-Host ''
Write-Host 'Environment:'
Write-Host "  Docker:      $(& docker version --format '{{.Server.Version}}') ($(& docker info --format '{{.OperatingSystem}}'), $(& docker info --format '{{.NCPU}}') CPUs, $([math]::Round([double](& docker info --format '{{.MemTotal}}') / 1GB, 1)) GiB for the Docker VM)"
Write-Host "  Host CPU:    $cpu, $([Environment]::ProcessorCount) logical processors"
Write-Host "  Host OS:     $($os.Caption) $($os.Version)"
