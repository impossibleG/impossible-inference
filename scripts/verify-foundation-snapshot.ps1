[CmdletBinding()]
param(
    [string]$RepositoryRoot = (Join-Path $PSScriptRoot "..")
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path $RepositoryRoot).Path
$referencePath = Join-Path $repoRoot "foundation-sync.json"
$reference = Get-Content -LiteralPath $referencePath -Raw | ConvertFrom-Json
$manifestPath = Join-Path $repoRoot $reference.manifest_path
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json

if ($reference.schema_version -ne 1 -or $manifest.schema_version -ne 1) {
    throw "unsupported foundation sync schema"
}
if (-not $reference.provisional -or -not $manifest.provisional) {
    throw "the bootstrap expects a provisional reviewed snapshot"
}
if ($reference.expected_source_tree_sha256 -cne $manifest.provisional_source_tree_sha256) {
    throw "foundation tree identity does not match the reviewed reference"
}

$vendorRoot = (Resolve-Path (Join-Path $repoRoot $reference.vendor_path)).Path
$treeEntries = [Collections.Generic.List[object]]::new()
$manifestPaths = [string[]]@($manifest.files | ForEach-Object { [string]$_.path })
[Array]::Sort($manifestPaths, [StringComparer]::Ordinal)
foreach ($manifestPath in $manifestPaths) {
    $matchingEntries = @($manifest.files | Where-Object { [string]$_.path -ceq $manifestPath })
    if ($matchingEntries.Count -ne 1) { throw "foundation manifest paths must be unique" }
    $entry = $matchingEntries[0]
    $relative = ([string]$entry.path).Replace('/', [IO.Path]::DirectorySeparatorChar)
    $candidate = [IO.Path]::GetFullPath((Join-Path $vendorRoot $relative))
    $prefix = $vendorRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $candidate.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "foundation manifest path escapes the vendor root"
    }
    if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        throw "foundation snapshot file is missing: $($entry.path)"
    }
    $file = Get-Item -LiteralPath $candidate
    if ($file.Length -ne [long]$entry.size) {
        throw "foundation snapshot size mismatch: $($entry.path)"
    }
    $digest = (Get-FileHash -Algorithm SHA256 -LiteralPath $candidate).Hash.ToLowerInvariant()
    if ($digest -cne [string]$entry.sha256) {
        throw "foundation snapshot digest mismatch: $($entry.path)"
    }
    $treeEntries.Add([ordered]@{
        path = [string]$entry.path
        sha256 = $digest
        size = [long]$file.Length
    })
}

$treeMaterial = ($treeEntries | ForEach-Object {
    "$($_.path)`t$($_.sha256)`t$($_.size)`n"
}) -join ""
$computedTree = [Convert]::ToHexString(
    [Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($treeMaterial))
).ToLowerInvariant()
if ($computedTree -cne [string]$manifest.provisional_source_tree_sha256) {
    throw "foundation source-tree digest does not match independently verified entries"
}

$actualFiles = @(Get-ChildItem -LiteralPath $vendorRoot -Recurse -File | ForEach-Object {
    [IO.Path]::GetRelativePath($vendorRoot, $_.FullName).Replace('\', '/')
} | Where-Object { $_ -ne "source-sync.json" } | Sort-Object)
$expectedFiles = @($manifest.files.path | Sort-Object)
if (Compare-Object -ReferenceObject $expectedFiles -DifferenceObject $actualFiles) {
    throw "foundation snapshot contains an unreviewed or omitted payload file"
}

Write-Output "Foundation snapshot verified: $computedTree"
