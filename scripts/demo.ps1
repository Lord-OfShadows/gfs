# Step-by-step demo of failure detection and self-healing, for screen recording.
# Run from the repo root:  .\scripts\demo.ps1
# Leaves the cluster running at the end; stop it with:  docker compose down -v

. "$PSScriptRoot\lib.ps1"
$root = Split-Path -Parent $PSScriptRoot # repo root: scripts\ is one level down
Set-Location $root
Assert-RepoRoot
Assert-Docker

$Interesting = @('node state changed', 'chunk re-replicated', 'repair round finished', 'repair failed')
$step = 0
function Step([string]$Title) {
    $script:step++
    Start-Sleep -Milliseconds 1500
    Write-Host ''
    Write-Host "== Step $($script:step): $Title ==" -ForegroundColor Cyan
}

$in = Join-Path $root 'transfer\demo.bin'
$out = Join-Path $root 'transfer\demo-out.bin'

Step 'starting a clean cluster (1 coordinator + 4 chunkservers)'
Invoke-Docker -Quiet -DockerArgs @('compose', 'down', '-v')
Write-Host 'building the image...'
Invoke-Docker -Quiet -DockerArgs @('compose', 'build', '--quiet')
Invoke-Docker -DockerArgs @('compose', 'up', '-d')
Write-Host 'waiting for all 4 chunkservers to heartbeat...'
Wait-ClusterReady
Write-Host 'cluster ready.' -ForegroundColor Green

Step 'uploading a 20 MB random file (5 chunks x 3 replicas)'
New-RandomFile -Path $in -SizeMB 20
Invoke-Docker -DockerArgs @('compose', 'run', '--rm', 'gfs', 'upload', '/transfer/demo.bin')

Step 'killing cs2 and watching the coordinator repair it'
Invoke-Docker -Quiet -DockerArgs @('compose', 'stop', 'cs2')
$stop = Get-StopTime -Node 'cs2'
Write-Host "$($stop.Time.ToString('HH:mm:ss.fff'))  cs2 stopped"
$repair = Wait-Repair -Node 'cs2' -Since $stop.Raw -TimeoutSec 30
if (-not $repair) {
    Write-Host 'No finished repair round within 30 s. Check: docker compose logs coordinator' -ForegroundColor Red
    exit 1
}
$repair.Events | Where-Object { $Interesting -contains $_.Msg } | ForEach-Object { Write-Host (Format-Event $_) }

Step 'killing cs3 too, then downloading'
Invoke-Docker -Quiet -DockerArgs @('compose', 'stop', 'cs3')
$stop3 = Get-StopTime -Node 'cs3'
Write-Host "$($stop3.Time.ToString('HH:mm:ss.fff'))  cs3 stopped; waiting for the coordinator to notice..."
# Wait until cs3 is marked unavailable, so the coordinator lists its replicas last
# and the download doesn't spend time trying it first.
$deadline = (Get-Date).AddSeconds(15)
do {
    Start-Sleep -Milliseconds 250
    $seen = @(Get-CoordinatorEvents -Since $stop3.Raw | Where-Object {
            $_.Msg -eq 'node state changed' -and $_.Data.node_id -eq 'cs3' -and $_.Data.state -eq 'unavailable' })
} while ($seen.Count -eq 0 -and (Get-Date) -lt $deadline)
$seen | ForEach-Object { Write-Host (Format-Event $_) }
Write-Host 'cs2 and cs3 are down; only cs1 and cs4 are left.'
Invoke-Docker -DockerArgs @('compose', 'run', '--rm', 'gfs', 'download', 'demo.bin', '/transfer/demo-out.bin')
$want = (Get-FileHash $in -Algorithm SHA256).Hash
$got = (Get-FileHash $out -Algorithm SHA256).Hash
Write-Host "uploaded:   $want"
Write-Host "downloaded: $got"
$pass = $want -eq $got
if ($pass) {
    Write-Host 'PASS: the downloaded file is byte-for-byte identical.' -ForegroundColor Green
} else {
    Write-Host 'FAIL: the downloaded file is different.' -ForegroundColor Red
}

Step 'bringing cs2 and cs3 back'
Invoke-Docker -Quiet -DockerArgs @('compose', 'start', 'cs2', 'cs3')
$deadline = (Get-Date).AddSeconds(15)
do {
    Start-Sleep -Milliseconds 500
    $recovered = @(Get-CoordinatorEvents -Since $stop.Raw | Where-Object { $_.Msg -eq 'node recovered' })
} while ($recovered.Count -lt 2 -and (Get-Date) -lt $deadline)
$recovered | ForEach-Object { Write-Host (Format-Event $_) }
if ($recovered.Count -lt 2) { Write-Host 'not every node reported back within 15 s' -ForegroundColor Yellow }

Step 'cleaning up'
Remove-Item $in, $out -ErrorAction SilentlyContinue
Write-Host 'demo files deleted. The cluster is still running:'
Write-Host '  docker compose logs -f coordinator   # watch it'
Write-Host '  docker compose down -v               # stop it and delete all data'

if (-not $pass) { exit 1 }
