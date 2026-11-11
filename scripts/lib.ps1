# Shared helpers for demo.ps1 and bench-failover.ps1. Dot-source it:  . "$PSScriptRoot\lib.ps1"
# Windows PowerShell 5.1 compatible: no ternary, no ??, no && / ||.

$ErrorActionPreference = 'Stop'

$Chunkservers = @('cs1', 'cs2', 'cs3', 'cs4')

function Assert-RepoRoot {
    if (-not (Test-Path 'docker-compose.yml') -or -not (Test-Path 'go.mod')) {
        Write-Host 'Run this script from the repo root (the folder with docker-compose.yml), e.g.  .\scripts\demo.ps1' -ForegroundColor Red
        exit 1
    }
}

function Assert-Docker {
    # Native stderr plus $ErrorActionPreference = 'Stop' throws in PS 5.1; relax it here only.
    $ErrorActionPreference = 'Continue'
    $null = & docker version --format '{{.Server.Version}}' 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'Docker is not running (or not installed). Start Docker Desktop and try again.' -ForegroundColor Red
        exit 1
    }
    $null = & docker compose version 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "'docker compose' (Compose v2) is not available. Update Docker Desktop and try again." -ForegroundColor Red
        exit 1
    }
}

# Invoke-Docker runs docker with the given arguments, shows its output, and
# throws if it fails. Use -Quiet to hide the output, -Capture to return it.
function Invoke-Docker {
    param([Parameter(Mandatory)][string[]]$DockerArgs, [switch]$Quiet, [switch]$Capture)
    if ($Capture) {
        $out = & docker @DockerArgs
    } elseif ($Quiet) {
        # Capture stdout and stderr (compose writes progress to stderr); show it only on failure.
        $ErrorActionPreference = 'Continue'
        $all = & docker @DockerArgs 2>&1
        if ($LASTEXITCODE -ne 0) { $all | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray } }
    } else {
        & docker @DockerArgs | Out-Host
    }
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($DockerArgs -join ' ') failed (exit code $LASTEXITCODE)"
    }
    if ($Capture) { return $out }
}

# ConvertTo-UtcTime parses a Docker/slog RFC 3339 time. Both use nanoseconds,
# but .NET only parses up to 7 fractional digits, so the rest is dropped.
function ConvertTo-UtcTime {
    param([Parameter(Mandatory)][string]$Text)
    $t = $Text.Trim() -replace '^(.*\.\d{1,7})\d*(Z|[+-]\d\d:\d\d)$', '$1$2'
    return [DateTimeOffset]::Parse($t, [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AssumeUniversal).ToUniversalTime()
}

# Wait-ClusterReady waits until the coordinator and every chunkserver have
# started and each chunkserver's heartbeats are succeeding. Successful
# heartbeats are logged at debug level (invisible), but every failed one is
# logged, so "started, and no 'heartbeat failed' in the last 2 s" means its
# heartbeats are getting through (one is sent per second).
function Wait-ClusterReady {
    param([int]$TimeoutSec = 60)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $started = @(& docker compose logs --no-log-prefix coordinator) -match 'coordinator started'
        foreach ($cs in $Chunkservers) {
            if (-not (@(& docker compose logs --no-log-prefix $cs) -match 'chunkserver started')) { $started = $false }
        }
        if ($started) {
            Start-Sleep -Milliseconds 2500 # make sure the 2 s window below starts after startup
            $failing = @()
            foreach ($cs in $Chunkservers) {
                if (@(& docker compose logs --no-log-prefix --since 2s $cs) -match 'heartbeat failed') { $failing += $cs }
            }
            if ($failing.Count -eq 0) { return }
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Cluster not ready after $TimeoutSec s (check: docker compose logs)"
}

function New-RandomFile {
    param([Parameter(Mandatory)][string]$Path, [int]$SizeMB = 20)
    $bytes = New-Object byte[] ($SizeMB * 1MB)
    (New-Object Random).NextBytes($bytes)
    [IO.File]::WriteAllBytes($Path, $bytes)
}

# Get-StopTime returns when the node's container process exited, by the
# Docker VM's clock (the same clock the coordinator's log times use).
function Get-StopTime {
    param([Parameter(Mandatory)][string]$Node)
    $id = Invoke-Docker -Capture -DockerArgs @('compose', 'ps', '-a', '-q', $Node)
    $raw = Invoke-Docker -Capture -DockerArgs @('inspect', '-f', '{{.State.FinishedAt}}', "$id".Trim())
    return [pscustomobject]@{ Raw = "$raw".Trim(); Time = (ConvertTo-UtcTime "$raw") }
}

# Get-CoordinatorEvents returns the coordinator's JSON log lines since Since
# (an RFC 3339 time) as objects: Time (UTC), Msg, Data (all fields).
function Get-CoordinatorEvents {
    param([Parameter(Mandatory)][string]$Since)
    foreach ($line in @(& docker compose logs --no-log-prefix --since $Since coordinator)) {
        if (-not $line.StartsWith('{')) { continue }
        try { $o = $line | ConvertFrom-Json } catch { continue }
        [pscustomobject]@{ Time = (ConvertTo-UtcTime $o.time); Msg = $o.msg; Data = $o }
    }
}

# Wait-Repair polls the coordinator log after Node was stopped at Since until
# it shows Node going unavailable and then a finished repair round. Returns
# { Unavailable, Finished, Events } or $null on timeout. The times come from
# the log lines themselves, so how often we poll doesn't affect them.
function Wait-Repair {
    param([Parameter(Mandatory)][string]$Node, [Parameter(Mandatory)][string]$Since, [int]$TimeoutSec = 60)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $events = @(Get-CoordinatorEvents -Since $Since)
        $unavailable = $events | Where-Object {
            $_.Msg -eq 'node state changed' -and $_.Data.node_id -eq $Node -and $_.Data.state -eq 'unavailable'
        } | Select-Object -First 1
        if ($unavailable) {
            $finished = $events | Where-Object {
                $_.Msg -eq 'repair round finished' -and $_.Time -ge $unavailable.Time
            } | Select-Object -First 1
            if ($finished) {
                return [pscustomobject]@{ Unavailable = $unavailable; Finished = $finished; Events = $events }
            }
        }
        Start-Sleep -Milliseconds 250
    }
    return $null
}

# Format-Event turns one coordinator event into a short readable line.
function Format-Event {
    param([Parameter(Mandatory)]$Entry)
    $d = $Entry.Data
    $when = $Entry.Time.ToString('HH:mm:ss.fff')
    switch ($Entry.Msg) {
        'node state changed' { return "$when  $($d.node_id) is now $($d.state)" }
        'node recovered' { return "$when  $($d.node_id) recovered (was $($d.was))" }
        'chunk re-replicated' {
            $id = "$($d.chunk_id)"
            if ($id.Length -gt 8) { $id = $id.Substring(0, 8) + '...' }
            return "$when  chunk $id copied  $($d.src) -> $($d.dst)"
        }
        'repair round finished' { return "$when  repair round finished: $($d.repaired) chunk(s) repaired" }
        'repair failed' { return "$when  repair FAILED for $($d.chunk_id): $($d.err)" }
        default { return "$when  $($Entry.Msg)" }
    }
}

# ConvertFrom-GoDuration turns a Go time.Duration string as the CLI prints it
# ("769ms", "1.2s", "1m2.5s") into milliseconds. Returns $null if it can't.
function ConvertFrom-GoDuration {
    param([Parameter(Mandatory)][string]$Text)
    $parts = [regex]::Matches($Text, '(\d+(?:\.\d+)?)(ms|h|m|s)')
    if ($parts.Count -eq 0) { return $null }
    $ms = 0.0
    foreach ($p in $parts) {
        $v = [double]::Parse($p.Groups[1].Value, [Globalization.CultureInfo]::InvariantCulture)
        switch ($p.Groups[2].Value) {
            'h' { $ms += $v * 3600000 }
            'm' { $ms += $v * 60000 }
            's' { $ms += $v * 1000 }
            'ms' { $ms += $v }
        }
    }
    return [math]::Round($ms)
}

# Invoke-Gfs runs one CLI command in a named one-off container and returns
# { Ok, Ms, Started, Output, Error }. Ms is the duration the CLI itself prints
# ("... in 769ms"), which excludes container startup. Started is when the
# container started, by the Docker VM's clock (comparable with Get-StopTime
# and coordinator log times).
function Invoke-Gfs {
    param([Parameter(Mandatory)][string[]]$GfsArgs)
    $ErrorActionPreference = 'Continue' # compose writes progress to stderr
    $name = 'gfs-bench-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
    $out = @(& docker compose run --name $name gfs @GfsArgs 2>&1 | ForEach-Object { "$_" })
    $code = $LASTEXITCODE
    $started = $null
    $raw = & docker inspect -f '{{.State.StartedAt}}' $name 2>$null
    if ($LASTEXITCODE -eq 0 -and "$raw".Trim()) { $started = ConvertTo-UtcTime "$raw" }
    & docker rm -f $name 2>&1 | Out-Null

    $ms = $null
    foreach ($line in $out) {
        if ($line -match '^(uploaded|downloaded) .* in (\S+)$') { $ms = ConvertFrom-GoDuration $Matches[2] }
    }
    $err = $out | Where-Object { $_ -match 'error|failed|Error' } | Select-Object -Last 1
    return [pscustomobject]@{
        Ok      = ($code -eq 0 -and $null -ne $ms)
        Ms      = $ms
        Started = $started
        Output  = $out
        Error   = "$err"
    }
}

# Wait-CoordinatorEvent polls the coordinator log since Since (RFC 3339) and
# returns the first event matching Match (a script block over $_), or $null
# after TimeoutSec.
function Wait-CoordinatorEvent {
    param([Parameter(Mandatory)][string]$Since, [Parameter(Mandatory)][scriptblock]$Match, [int]$TimeoutSec = 60)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $e = @(Get-CoordinatorEvents -Since $Since) | Where-Object -FilterScript $Match | Select-Object -First 1
        if ($e) { return $e }
        Start-Sleep -Milliseconds 250
    }
    return $null
}

# Get-Stats returns min / median / p95 (nearest rank) / max of Values.
function Get-Stats([double[]]$Values) {
    $s = $Values | Sort-Object
    $n = $s.Count
    if ($n % 2 -eq 1) { $median = $s[($n - 1) / 2] } else { $median = ($s[$n / 2 - 1] + $s[$n / 2]) / 2 }
    $p95 = $s[[math]::Ceiling(0.95 * $n) - 1]
    return [pscustomobject]@{ Min = $s[0]; Median = $median; P95 = $p95; Max = $s[$n - 1] }
}

# Write-Environment prints the Docker and host details a result depends on.
function Write-Environment {
    $cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name.Trim()
    $os = Get-CimInstance Win32_OperatingSystem
    $mem = [math]::Round([double](& docker info --format '{{.MemTotal}}') / 1GB, 1)
    Write-Host 'Environment:'
    Write-Host "  Docker:      $(& docker version --format '{{.Server.Version}}') ($(& docker info --format '{{.OperatingSystem}}'), $(& docker info --format '{{.NCPU}}') CPUs, $mem GiB for the Docker VM)"
    Write-Host "  Host CPU:    $cpu, $([Environment]::ProcessorCount) logical processors"
    Write-Host "  Host OS:     $($os.Caption) $($os.Version)"
}
