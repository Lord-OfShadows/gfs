# Client-side latency: replication cost, and reads/writes while a node is
# failing or being repaired.
# Run from anywhere:  .\scripts\bench-latency.ps1 [-Runs 10] [-FileMB 20] [-BigFileMB 400]
#
# Latency is the duration the CLI itself prints ("downloaded ... in 769ms"),
# not a stopwatch around `docker compose run`, which would include container
# startup. Results: results\latency-<timestamp>.csv
#
# Scenarios:
#   A  replication cost: upload + download -Runs files, with replication 1 and 3
#   B  reads during the detection window: download right after stopping cs2,
#      then again once the coordinator has marked it unavailable
#   C  writes during the detection window: same, but uploads
#   D  reads during repair: download a small file while a -BigFileMB file's
#      lost replicas are being re-copied

param(
    [int]$Runs = 10,
    [int]$FileMB = 20,
    [int]$BigFileMB = 400
)

. "$PSScriptRoot\lib.ps1"
$root = Split-Path -Parent $PSScriptRoot # repo root: scripts\ is one level down
Set-Location $root
Assert-RepoRoot
Assert-Docker

$transfer = Join-Path $root 'transfer'
$resultsDir = Join-Path $root 'results'
New-Item -ItemType Directory -Force $resultsDir | Out-Null
$csv = Join-Path $resultsDir "latency-$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
$rows = New-Object System.Collections.Generic.List[object]
$Node = 'cs2'

function Add-Row([string]$Scenario, [string]$Variant, [int]$Run, $Result, [bool]$Ok, [string]$Note) {
    $ms = ''
    if ($null -ne $Result -and $null -ne $Result.Ms) { $ms = $Result.Ms }
    $rows.Add([pscustomobject][ordered]@{ scenario = $Scenario; variant = $Variant; run = $Run; ms = $ms; ok = $Ok; note = $Note })
    $rows | Export-Csv -NoTypeInformation -Path $csv # rewrite each time, so partial results survive Ctrl+C
    $mark = 'ok  '
    if (-not $Ok) { $mark = 'FAIL' }
    Write-Host ("  {0} {1,-20} run {2,2}  {3,7} ms  {4}" -f $mark, "$Scenario/$Variant", $Run, $ms, $Note)
}

# Start-Cluster starts a fresh cluster with the given replication factor.
function Start-Cluster([int]$Replication) {
    $env:GFS_REPLICATION = "$Replication"
    Invoke-Docker -Quiet -DockerArgs @('compose', 'down', '-v')
    Invoke-Docker -Quiet -DockerArgs @('compose', 'up', '-d')
    Wait-ClusterReady
}

# New-TestFile writes a random file into transfer\ and returns its name.
function New-TestFile([string]$Name, [int]$SizeMB) {
    New-RandomFile -Path (Join-Path $transfer $Name) -SizeMB $SizeMB
    return $Name
}

function Upload([string]$Name) { return Invoke-Gfs -GfsArgs @('upload', "/transfer/$Name") }

# Download fetches Name into transfer\<Out> and checks it against the local
# original. Returns the Invoke-Gfs result plus HashOk.
function Download([string]$Name, [string]$Out) {
    $r = Invoke-Gfs -GfsArgs @('download', $Name, "/transfer/$Out")
    $hashOk = $false
    $outPath = Join-Path $transfer $Out
    if ($r.Ok -and (Test-Path $outPath)) {
        $hashOk = (Get-FileHash (Join-Path $transfer $Name)).Hash -eq (Get-FileHash $outPath).Hash
    }
    Remove-Item $outPath -ErrorAction SilentlyContinue
    $r | Add-Member -NotePropertyName HashOk -NotePropertyValue $hashOk
    return $r
}

function Short([string]$Text) {
    $t = ($Text -replace '\s+', ' ').Trim()
    if ($t.Length -gt 160) { $t = $t.Substring(0, 160) + '...' }
    return $t
}

# MsAfter returns how many ms Later is after Earlier, or '?' if either is missing.
function MsAfter($Later, $Earlier) {
    if ($null -eq $Later -or $null -eq $Earlier) { return '?' }
    return [math]::Round(($Later - $Earlier).TotalMilliseconds)
}

function Stop-Node {
    Invoke-Docker -Quiet -DockerArgs @('compose', 'stop', $Node)
    return Get-StopTime -Node $Node
}

function Wait-Unavailable($Stop) {
    return Wait-CoordinatorEvent -Since $Stop.Raw -TimeoutSec 30 -Match {
        $_.Msg -eq 'node state changed' -and $_.Data.node_id -eq $Node -and $_.Data.state -eq 'unavailable'
    }
}

# Restore-Node starts the node again and waits until the coordinator has
# heard from it (it is Healthy again).
function Restore-Node($Stop) {
    Invoke-Docker -Quiet -DockerArgs @('compose', 'start', $Node)
    $back = Wait-CoordinatorEvent -Since $Stop.Raw -TimeoutSec 30 -Match {
        $_.Msg -eq 'node recovered' -and $_.Data.node_id -eq $Node
    }
    if (-not $back) { throw "$Node did not report back within 30 s" }
}

try {
    Write-Host 'Building the image once...' -ForegroundColor Cyan
    Invoke-Docker -Quiet -DockerArgs @('compose', 'build', '--quiet')

    # --- A: replication cost -------------------------------------------------
    foreach ($repl in 1, 3) {
        Write-Host "== A: replication $repl ($Runs uploads + downloads of $FileMB MB) ==" -ForegroundColor Cyan
        Start-Cluster $repl
        for ($i = 1; $i -le $Runs; $i++) {
            $name = New-TestFile "lat-a$repl-$i.bin" $FileMB
            $u = Upload $name
            Add-Row 'A' "upload-r$repl" $i $u $u.Ok (Short $u.Error)
            if ($u.Ok) {
                $d = Download $name "lat-a$repl-$i-out.bin"
                $note = ''
                if ($d.Ok -and -not $d.HashOk) { $note = 'hash mismatch' } else { $note = Short $d.Error }
                Add-Row 'A' "download-r$repl" $i $d ($d.Ok -and $d.HashOk) $note
            }
            Remove-Item (Join-Path $transfer $name) -ErrorAction SilentlyContinue
        }
    }

    # --- B: reads during the detection window ----------------------------------
    Write-Host "== B: downloads while $Node is failing (replication 3) ==" -ForegroundColor Cyan
    Start-Cluster 3
    for ($i = 1; $i -le $Runs; $i++) {
        $name = New-TestFile "lat-b-$i.bin" $FileMB
        $u = Upload $name
        if (-not $u.Ok) { Add-Row 'B' 'setup-upload' $i $u $false (Short $u.Error); continue }

        $stop = Stop-Node
        $d1 = Download $name "lat-b-$i-out1.bin"
        $unav = Wait-Unavailable $stop
        $before = 'unknown'
        if ($unav -and $d1.Started) { $before = ($d1.Started -lt $unav.Time) }
        Add-Row 'B' 'during-detection' $i $d1 ($d1.Ok -and $d1.HashOk) `
            "started $(MsAfter $d1.Started $stop.Time) ms after stop; before unavailable: $before $(Short $d1.Error)"

        if (-not $unav) {
            Add-Row 'B' 'after-unavailable' $i $null $false "$Node never marked unavailable within 30 s"
        } else {
            $d2 = Download $name "lat-b-$i-out2.bin"
            Add-Row 'B' 'after-unavailable' $i $d2 ($d2.Ok -and $d2.HashOk) `
                "started $(MsAfter $d2.Started $unav.Time) ms after unavailable $(Short $d2.Error)"
        }
        Restore-Node $stop
        Remove-Item (Join-Path $transfer $name) -ErrorAction SilentlyContinue
    }

    # --- C: writes during the detection window ---------------------------------
    Write-Host "== C: uploads while $Node is failing (replication 3) ==" -ForegroundColor Cyan
    Start-Cluster 3
    for ($i = 1; $i -le $Runs; $i++) {
        $setup = New-TestFile "lat-c-$i-setup.bin" $FileMB
        $u0 = Upload $setup
        if (-not $u0.Ok) { Add-Row 'C' 'setup-upload' $i $u0 $false (Short $u0.Error); continue }
        $early = New-TestFile "lat-c-$i-early.bin" $FileMB
        $late = New-TestFile "lat-c-$i-late.bin" $FileMB

        $stop = Stop-Node
        $u1 = Upload $early
        $unav = Wait-Unavailable $stop
        $before = 'unknown'
        if ($unav -and $u1.Started) { $before = ($u1.Started -lt $unav.Time) }
        Add-Row 'C' 'during-detection' $i $u1 $u1.Ok `
            "started $(MsAfter $u1.Started $stop.Time) ms after stop; before unavailable: $before $(Short $u1.Error)"

        if (-not $unav) {
            Add-Row 'C' 'after-unavailable' $i $null $false "$Node never marked unavailable within 30 s"
        } else {
            $u2 = Upload $late
            Add-Row 'C' 'after-unavailable' $i $u2 $u2.Ok "started $(MsAfter $u2.Started $unav.Time) ms after unavailable $(Short $u2.Error)"
        }
        Restore-Node $stop
        foreach ($f in $setup, $early, $late) { Remove-Item (Join-Path $transfer $f) -ErrorAction SilentlyContinue }
    }

    # --- D: reads during repair -----------------------------------------------
    Write-Host "== D: small downloads while a $BigFileMB MB file is being repaired (replication 3) ==" -ForegroundColor Cyan
    $big = New-TestFile 'lat-d-big.bin' $BigFileMB
    $small = New-TestFile 'lat-d-small.bin' $FileMB
    for ($i = 1; $i -le $Runs; $i++) {
        Start-Cluster 3
        $ub = Upload $big
        $us = Upload $small
        if (-not ($ub.Ok -and $us.Ok)) {
            Add-Row 'D' 'setup-upload' $i $null $false (Short "$($ub.Error) $($us.Error)")
            continue
        }
        $base = Download $small 'lat-d-out.bin'
        Add-Row 'D' 'baseline' $i $base ($base.Ok -and $base.HashOk) (Short $base.Error)

        $stop = Stop-Node
        $unav = Wait-Unavailable $stop
        if (-not $unav) { Add-Row 'D' 'during-repair' $i $null $false "$Node never marked unavailable within 30 s"; continue }
        $d = Download $small 'lat-d-out.bin'
        $fin = Wait-CoordinatorEvent -Since $stop.Raw -TimeoutSec 60 -Match {
            $_.Msg -eq 'repair round finished' -and $_.Time -ge $unav.Time
        }
        $overlap = 'unknown'
        $note = "started $(MsAfter $d.Started $unav.Time) ms after unavailable"
        if ($fin) {
            if ($d.Started) { $overlap = ($d.Started -lt $fin.Time) }
            $note += "; repair finished $(MsAfter $fin.Time $d.Started) ms after download start ($($fin.Data.repaired) chunks)"
        } else {
            $note += '; no repair round finished within 60 s'
        }
        Add-Row 'D' 'during-repair' $i $d ($d.Ok -and $d.HashOk) "overlap: $overlap; $note $(Short $d.Error)"
    }
} finally {
    Write-Host 'Cleaning up (docker compose down -v, generated files)...'
    try { Invoke-Docker -Quiet -DockerArgs @('compose', 'down', '-v') } catch { Write-Host $_.Exception.Message -ForegroundColor Red }
    Remove-Item (Join-Path $transfer 'lat-*') -ErrorAction SilentlyContinue
    Remove-Item Env:\GFS_REPLICATION -ErrorAction SilentlyContinue
}

Write-Host ''
Write-Host "Results: $csv" -ForegroundColor Cyan
Write-Host ("{0,-22} {1,5} {2,5} {3,8} {4,8} {5,8} {6,8}" -f 'scenario/variant', 'count', 'fail', 'min', 'median', 'p95', 'max')
foreach ($g in ($rows | Group-Object scenario, variant)) {
    $okRows = @($g.Group | Where-Object { $_.ok -and "$($_.ms)" -ne '' })
    $fails = $g.Count - $okRows.Count
    $label = "$($g.Group[0].scenario)/$($g.Group[0].variant)"
    if ($okRows.Count -gt 0) {
        $st = Get-Stats ([double[]]($okRows | ForEach-Object { $_.ms }))
        Write-Host ("{0,-22} {1,5} {2,5} {3,8} {4,8} {5,8} {6,8}" -f $label, $g.Count, $fails, $st.Min, $st.Median, $st.P95, $st.Max)
    } else {
        Write-Host ("{0,-22} {1,5} {2,5} {3,8} {4,8} {5,8} {6,8}" -f $label, $g.Count, $fails, '-', '-', '-', '-')
    }
}
Write-Host ''
Write-Host "Files: $FileMB MB (big file in D: $BigFileMB MB), stopped node: $Node, runs per scenario: $Runs"
Write-Host ''
Write-Environment
