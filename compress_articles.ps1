#requires -Version 7.0
<#
.SYNOPSIS
    Compress the article corpus's PDFs IN PLACE with Ghostscript. Resumable,
    parallel, never loses data.

.DESCRIPTION
    Each PDF in the corpus (../articles/, the same folder src/config.py calls
    ARTICLES_DIR) is replaced by its compressed version only when that version is
    verified AND strictly smaller; otherwise the original is left exactly as it
    was and the log says why.

    What compression does to the database: file_info.content_sha256 is a hash of
    the PDF's bytes (src/modules/extract_text.py), so every article this script
    compresses is seen as changed on the next ingest and re-extracted once. That
    is safe -- src/ingest.py purges the old rows by file_id, and by source_path
    when the id itself moved (articles without a DOI are keyed sha256:<hash>) --
    but it is not free. Compress first, then ingest:

        .\compress_articles.ps1  ->  python src/pipeline.py

    Files keep their names; nothing in the pipeline depends on them beyond
    source_path, which an in-place replace does not change.

    Files are processed largest first. The parallel loop pulls from the queue in
    order, so that ordering is the schedule: it keeps every worker busy to the end
    of the run instead of leaving one huge file for last, and it reclaims the most
    space soonest if the run is stopped part way.

.NOTES
    Verification: %PDF header, %%EOF trailer, and the same page count before and
    after, both counted by Ghostscript. The page count catches a readable but
    truncated output that the trailer check alone would accept.

.EXAMPLE
    .\compress_articles.ps1                             # compress ../articles
    .\compress_articles.ps1 -WhatIf                     # dry run: list what would be done
    .\compress_articles.ps1 -Preset printer             # gentler on figures
    .\compress_articles.ps1 -Jobs 8                     # override the background-friendly default
    .\compress_articles.ps1 -Source "D:\papers" -Force  # re-compress (lossy: each pass degrades images)
#>
param(
    [string]$Source,
    [ValidateSet('screen','ebook','printer','prepress')][string]$Preset = 'ebook',
    [int]$Jobs = 0,
    [string]$LogPath,
    [Alias('DryRun')][switch]$WhatIf,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# --- Config ------------------------------------------------------------------
$TmpSuffix       = '.gstmp'   # deliberately not *.pdf -- ingest globs for that
$MinOutputBytes  = 1024       # anything smaller than this is not a real PDF
$GsTimeoutMs     = 1800 * 1000
$CountTimeoutMs  = 120 * 1000
$DefaultJobs     = 4          # this runs for a long time in the background; leave the box usable
$LogFields       = @('timestamp','file','status','in_bytes','out_bytes','pct_saved','preset','pages')

# --- Helpers -----------------------------------------------------------------

function Get-ConfigValue {
    <# Environment first, then the repo's .env -- the same order python-dotenv gives. #>
    param([string]$Key)

    $value = [Environment]::GetEnvironmentVariable($Key)
    if ($value) { return $value }

    $envFile = Join-Path $PSScriptRoot '.env'
    if (Test-Path -LiteralPath $envFile) {
        foreach ($line in Get-Content -LiteralPath $envFile) {
            $text = $line.Trim()
            if (-not $text -or $text.StartsWith('#')) { continue }
            $split = $text.IndexOf('=')
            if ($split -lt 1) { continue }
            if ($text.Substring(0, $split).Trim() -eq $Key) {
                return $text.Substring($split + 1).Trim().Trim('"', "'")
            }
        }
    }
    return $null
}

function Find-Ghostscript {
    <#
    GHOSTSCRIPT_PATH, then PATH, then the default Windows install location -- the
    installer does not add itself to PATH, so looking only at PATH fails on a
    perfectly working installation.
    #>
    $override = Get-ConfigValue 'GHOSTSCRIPT_PATH'
    if ($override) {
        if (Test-Path -LiteralPath $override -PathType Leaf) { return (Resolve-Path -LiteralPath $override).Path }
        throw "GHOSTSCRIPT_PATH points at a missing file: $override"
    }

    foreach ($name in 'gswin64c', 'gswin32c', 'gs') {
        $found = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($found) { return $found.Source }
    }

    $candidates = @()
    foreach ($root in $env:ProgramFiles, ${env:ProgramFiles(x86)}) {
        if (-not $root) { continue }
        $candidates += @(Get-Item -Path (Join-Path $root 'gs\gs*\bin\gswin*c.exe') -ErrorAction SilentlyContinue)
    }
    if ($candidates) { return ($candidates | Sort-Object FullName)[-1].FullName }   # highest version wins

    throw @"
Ghostscript not found.
  Windows: install from https://ghostscript.com/releases/gsdnld.html
  Linux  : apt install ghostscript
  Or set GHOSTSCRIPT_PATH=<full path to gswin64c.exe|gs> in your .env.
"@
}

function Format-CsvValue {
    <# Invariant formatting: on a comma-decimal locale "34,2" would split the row. #>
    param($Value)

    $text = if ($Value -is [double] -or $Value -is [single] -or $Value -is [decimal]) {
        [string]::Format([cultureinfo]::InvariantCulture, '{0}', $Value)
    } else {
        [string]$Value
    }
    if ($text -match '[",\r\n]') { return '"' + $text.Replace('"', '""') + '"' }
    return $text
}

function Import-Ledger {
    <#
    {filename = out_bytes} from the compression log, last entry wins. A missing or
    unreadable log is not fatal -- the worst case is a second compression pass,
    which the size check below still guards against.
    #>
    param([string]$Path)

    $done = @{}
    if (-not (Test-Path -LiteralPath $Path)) { return $done }
    try {
        foreach ($row in Import-Csv -LiteralPath $Path) {
            $bytes = 0
            if ([int64]::TryParse($row.out_bytes, [ref]$bytes)) { $done[$row.file] = $bytes }
        }
    } catch {
        Write-Warning "Could not read $Path : $($_.Exception.Message) -- treating every file as new."
        return @{}
    }
    return $done
}

# --- Setup -------------------------------------------------------------------

# Same location src/config.py resolves: a sibling "articles" folder next to the repo.
if (-not $Source)  { $Source  = Join-Path (Split-Path $PSScriptRoot -Parent) 'articles' }
if (-not $LogPath) { $LogPath = Join-Path $PSScriptRoot 'data\article_compression_log.csv' }
# Ghostscript is CPU-bound and pegs a core per job. Four is the background
# default; -Jobs is the deliberate override for when the machine is idle.
if ($Jobs -le 0)   { $Jobs    = [Math]::Max(1, [Math]::Min($DefaultJobs, [Environment]::ProcessorCount - 1)) }

if (-not (Test-Path -LiteralPath $Source -PathType Container)) {
    Write-Host "[ERROR] Articles folder not found: $Source"
    Write-Host "  The corpus is expected at ..\articles relative to the repo root, or pass -Source."
    exit 1
}
$Source = (Resolve-Path -LiteralPath $Source).Path
$gs = Find-Ghostscript

# Temps from a run that was killed mid-write. Harmless to the rest of the
# pipeline (nothing globs *.gstmp), but they waste disk until swept.
$stale = @(Get-ChildItem -LiteralPath $Source -Filter "*$TmpSuffix" -File -ErrorAction SilentlyContinue)
if ($stale.Count -and $WhatIf) {
    Write-Host "[INFO] $($stale.Count) leftover $TmpSuffix file(s) would be removed."
} elseif ($stale.Count) {
    $swept = 0
    foreach ($file in $stale) {
        try { Remove-Item -LiteralPath $file.FullName -Force; $swept++ } catch {}
    }
    Write-Host "[INFO] Removed $swept leftover $TmpSuffix file(s) from an interrupted run."
}

$pdfs   = Get-ChildItem -LiteralPath $Source -Filter *.pdf -File
$ledger = if ($Force) { @{} } else { Import-Ledger $LogPath }

# Size still matching what we logged means this is the file we produced (or one
# we already decided to leave alone); a different size means it was replaced on
# disk, so look at it again.
#
# Largest first: ForEach-Object -Parallel pulls from this pipeline in order, so
# the sort IS the schedule. Longest-job-first keeps every worker busy to the end
# and front-loads the savings. Name breaks ties to keep runs reproducible.
$todo    = @($pdfs |
    Where-Object { -not ($ledger.ContainsKey($_.Name) -and $_.Length -eq $ledger[$_.Name]) } |
    Sort-Object -Property @{Expression = 'Length'; Descending = $true},
                          @{Expression = 'Name';   Descending = $false})
$skipped = $pdfs.Count - $todo.Count
$totalIn = ($todo | Measure-Object Length -Sum).Sum

Write-Host ("Ghostscript : {0}" -f $gs)
Write-Host ("Source      : {0}   (in place)" -f $Source)
Write-Host ("Preset      : /{0}   Parallel jobs: {1}" -f $Preset, $Jobs)
Write-Host ("Verify      : %PDF header + %%EOF trailer + page count")
Write-Host ("To process  : {0} files, {1:N1} GB  ({2} already done, skipped)" -f $todo.Count, ($totalIn/1GB), $skipped)
if ($todo.Count) {
    Write-Host ("Order       : largest first  ({0:N1} MB -> {1:N1} MB)" -f ($todo[0].Length/1MB), ($todo[-1].Length/1MB))
}

# In place needs room for one temp file per job, not for the whole set. The
# first $Jobs files ARE the $Jobs biggest, so a disk shortfall shows up early.
try {
    $free    = [System.IO.DriveInfo]::new($Source).AvailableFreeSpace
    $peak    = ($todo | Select-Object -First $Jobs | Measure-Object Length -Sum).Sum
    Write-Host ("Free space  : {0:N1} GB" -f ($free/1GB))
    if ($peak -and $free -lt $peak) {
        Write-Warning ("Free space ({0:N1} GB) is below the {1:N1} GB the first {2} files need as temp copies, and those run first." -f ($free/1GB), ($peak/1GB), $Jobs)
    }
} catch {}

Write-Host ("Log         : {0}" -f $LogPath)
Write-Host ''

if ($WhatIf) {
    foreach ($p in $todo) { Write-Host ("  [WOULD] {0,8:N1} MB  {1}" -f ($p.Length/1MB), $p.Name) }
    Write-Host "`n-WhatIf: nothing written."
    return
}
if ($todo.Count -eq 0) { Write-Host 'Nothing to do.'; return }

# --- Run ---------------------------------------------------------------------

$logDir = Split-Path $LogPath -Parent
if ($logDir -and -not (Test-Path -LiteralPath $logDir)) { New-Item -ItemType Directory $logDir -Force | Out-Null }
$isNewLog = -not (Test-Path -LiteralPath $LogPath) -or (Get-Item -LiteralPath $LogPath).Length -eq 0
$writer = [System.IO.StreamWriter]::new($LogPath, $true, [System.Text.UTF8Encoding]::new($false))
if ($isNewLog) { $writer.WriteLine($LogFields -join ',') }

$sw      = [Diagnostics.Stopwatch]::StartNew()
$total   = $todo.Count
$done    = 0
$results = @()

try {
    $results = $todo | ForEach-Object -ThrottleLimit $Jobs -Parallel {
        # Each item runs in a fresh runspace that inherits none of the parent's
        # preference variables, so the script-level 'Stop' does not reach here.
        $ErrorActionPreference = 'Stop'

        $gs        = $using:gs
        $preset    = $using:Preset
        $tmpSuffix = $using:TmpSuffix
        $minBytes  = $using:MinOutputBytes
        $timeoutMs = $using:GsTimeoutMs
        $countMs   = $using:CountTimeoutMs

        function Invoke-Gs {
            <#
            Runs Ghostscript with each argument passed verbatim (ArgumentList, not
            `& $gs -dX=/$var`, which PowerShell would hand over unexpanded), below
            normal priority, with both pipes drained so a chatty malformed PDF
            cannot fill a buffer and deadlock. Returns @{ TimedOut; ExitCode; StdOut }.
            #>
            param([string[]]$Arguments, [int]$Timeout)

            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName               = $gs
            $psi.UseShellExecute        = $false
            $psi.CreateNoWindow         = $true
            $psi.RedirectStandardOutput = $true
            $psi.RedirectStandardError  = $true
            foreach ($arg in $Arguments) { $psi.ArgumentList.Add($arg) }

            $proc = [System.Diagnostics.Process]::Start($psi)
            try {
                try { $proc.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal } catch {}
                $out = $proc.StandardOutput.ReadToEndAsync()
                $null = $proc.StandardError.ReadToEndAsync()

                if (-not $proc.WaitForExit($Timeout)) {
                    # Kill() only requests termination; wait for it so cleanup does
                    # not race a process that still holds the temp file open.
                    try { $proc.Kill($true); $null = $proc.WaitForExit(30000) } catch {}
                    return @{ TimedOut = $true; ExitCode = -1; StdOut = '' }
                }
                $proc.WaitForExit()   # flushes the async readers
                return @{ TimedOut = $false; ExitCode = $proc.ExitCode; StdOut = $out.Result }
            } finally { $proc.Dispose() }
        }

        function Get-PageCount {
            <# Page count via Ghostscript's PDF interpreter; $null if it cannot tell. #>
            param([string]$Path)

            # PostScript string literal: forward slashes, and escape \ ( ) which
            # do appear in article titles.
            $ps = ($Path -replace '\\', '/').Replace('(', '\(').Replace(')', '\)')
            $r = Invoke-Gs -Timeout $countMs -Arguments @(
                '-q', '-dNODISPLAY', '-dNOSAFER', '-dBATCH', '-dNOPAUSE',
                '-c', "($ps) (r) file runpdfbegin pdfpagecount = quit")
            if ($r.TimedOut -or $r.ExitCode -ne 0) { return $null }
            $last = ($r.StdOut -split "`r?`n" | Where-Object { $_.Trim() } | Select-Object -Last 1)
            $n = 0
            if ($last -and [int]::TryParse($last.Trim(), [ref]$n) -and $n -gt 0) { return $n }
            return $null
        }

        $src      = $_.FullName
        $tmp      = $src + $tmpSuffix
        $inBytes  = $_.Length
        $outBytes = $inBytes          # unchanged unless the replace actually happens
        $status   = 'error'
        $pages    = ''

        try {
            $r = Invoke-Gs -Timeout $timeoutMs -Arguments @(
                '-sDEVICE=pdfwrite', '-dNOPAUSE', '-dBATCH', '-dQUIET',
                "-dPDFSETTINGS=/$preset", '-dAutoRotatePages=/None',
                '-o', $tmp, $src)

            if ($r.TimedOut) {
                $status = 'timeout'
            }
            elseif ($r.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $tmp)) {
                $status = 'gs-failed'
            }
            else {
                $len    = (Get-Item -LiteralPath $tmp).Length
                $isPdf  = $false
                $hasEof = $false
                if ($len -ge $minBytes) {
                    $stream = [System.IO.File]::OpenRead($tmp)
                    try {
                        $head = [byte[]]::new(4)
                        $null = $stream.Read($head, 0, 4)
                        $isPdf = ($head[0] -eq 0x25 -and $head[1] -eq 0x50 -and $head[2] -eq 0x44 -and $head[3] -eq 0x46)

                        # %%EOF in the last KB: Ghostscript writes it last.
                        $tailLen = [Math]::Min(1024, $stream.Length)
                        $null = $stream.Seek(-$tailLen, [System.IO.SeekOrigin]::End)
                        $tail = [byte[]]::new($tailLen)
                        $null = $stream.Read($tail, 0, $tailLen)
                        $hasEof = [System.Text.Encoding]::ASCII.GetString($tail).Contains('%%EOF')
                    } finally { $stream.Dispose() }
                }

                if ($len -lt $minBytes -or -not $isPdf) {
                    $status = 'bad-output'
                }
                elseif (-not $hasEof) {
                    $status = 'verify-failed'
                }
                elseif ($len -ge $inBytes) {
                    # Already well compressed, or mostly vector/text -- typical for
                    # born-digital papers. Keep the original: no gain, only risk.
                    $status = 'kept-original'
                }
                else {
                    # Only counted for files we would actually replace: two extra
                    # Ghostscript passes are wasted on anything already rejected.
                    $inPages  = Get-PageCount $src
                    $outPages = Get-PageCount $tmp
                    if ($inPages) { $pages = $inPages }

                    if (-not $inPages -or $inPages -ne $outPages) {
                        $status = 'page-mismatch'
                    }
                    else {
                        try {
                            # Atomic within a filesystem: the file is either the
                            # original or the compressed version, never a mix.
                            [System.IO.File]::Move($tmp, $src, $true)
                            $status   = 'compressed'
                            $outBytes = $len
                        } catch {
                            # Typically the PDF is open in a reader and Windows refuses.
                            $status = 'locked'
                        }
                    }
                }
            }
        } catch {
            $status = 'error'
        } finally {
            # Never leave a temp file behind. Defender often scans the file the
            # instant Ghostscript closes it, so retry with a short back-off;
            # anything still locked is swept at the start of the next run.
            for ($attempt = 1; $attempt -le 5; $attempt++) {
                if (-not (Test-Path -LiteralPath $tmp)) { break }
                try { Remove-Item -LiteralPath $tmp -Force; break }
                catch { Start-Sleep -Milliseconds (200 * $attempt) }
            }
        }

        [pscustomobject]@{
            timestamp = (Get-Date).ToString('s')
            file      = $_.Name
            status    = $status
            in_bytes  = $inBytes
            out_bytes = $outBytes
            pct_saved = if ($inBytes) { [math]::Round(100 * (1 - $outBytes / $inBytes), 1) } else { 0.0 }
            preset    = $preset
            pages     = $pages
        }
    } | ForEach-Object {
        # Logged in the parent as each file finishes -- one writer, no contention,
        # and an interrupted run resumes from where it stopped.
        $row = $_
        $writer.WriteLine((($LogFields | ForEach-Object { Format-CsvValue $row.$_ }) -join ','))
        $writer.Flush()

        $done++
        Write-Host ("  [{0,4}/{1}] {2,-17} {3,8:N1} -> {4,8:N1} MB  {5}" -f `
            $done, $total, $row.status, ($row.in_bytes/1MB), ($row.out_bytes/1MB), $row.file)
        $row
    }
} finally {
    $writer.Dispose()
}

# --- Summary -----------------------------------------------------------------

$sw.Stop()
$inSum  = ($results | Measure-Object in_bytes  -Sum).Sum
$outSum = ($results | Measure-Object out_bytes -Sum).Sum

Write-Host ''
Write-Host ("{0} files in {1:hh\:mm\:ss}" -f @($results).Count, $sw.Elapsed)
if ($inSum) {
    Write-Host ("{0:N1} GB -> {1:N1} GB   ({2:N1}% saved)" -f ($inSum/1GB), ($outSum/1GB), (100 * (1 - $outSum/[double]$inSum)))
}

$results | Group-Object status | Sort-Object Count -Descending |
    ForEach-Object { Write-Host ("  {0,-18} {1}" -f $_.Name, $_.Count) }

$compressed = @($results | Where-Object status -eq 'compressed').Count
$untouched  = @($results).Count - $compressed
if ($untouched) {
    Write-Host ''
    Write-Host "$untouched file(s) were left exactly as they were (not compressed, not damaged)."
}
if ($compressed) {
    Write-Host "$compressed article(s) changed on disk -- the next pipeline run re-ingests them."
}
Write-Host ("Log: {0}" -f $LogPath)
