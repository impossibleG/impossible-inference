[CmdletBinding()]
param([switch]$Check)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$noticePath = Join-Path $repositoryRoot "THIRD_PARTY_NOTICES.md"
$licensePath = Join-Path $repositoryRoot "THIRD_PARTY_LICENSES.txt"

Push-Location $repositoryRoot
try {
    $metadataText = (& cargo metadata --locked --format-version 1 | Out-String)
    if ($LASTEXITCODE -ne 0) { throw "cargo metadata failed" }
} finally {
    Pop-Location
}
$metadata = $metadataText | ConvertFrom-Json
$workspace = [Collections.Generic.HashSet[string]]::new([string[]]$metadata.workspace_members)
$dependencies = @($metadata.packages | Where-Object {
    -not $workspace.Contains([string]$_.id)
} | Sort-Object name, version, source)

$notice = [Collections.Generic.List[string]]::new()
$notice.Add("# Third-party notices")
$notice.Add("")
$notice.Add("Generated from the locked Rust dependency graph by ``scripts/generate-third-party-notices.ps1``. Do not edit by hand.")
$notice.Add("")
$notice.Add("Impossible Inferences is distributed under MIT OR Apache-2.0. The inventory below records each dependency's declared license; complete mapped texts are in ``THIRD_PARTY_LICENSES.txt``. ``cargo deny`` validates the graph against ``deny.toml``.")
$notice.Add("")
$notice.Add("| Package | Version | Declared license | Source |")
$notice.Add("| --- | --- | --- | --- |")
foreach ($package in $dependencies) {
    $declared = if ([string]::IsNullOrWhiteSpace([string]$package.license)) { "NOASSERTION" } else { [string]$package.license }
    $source = if ([string]$package.source -like "registry+*") { "crates.io" } elseif ([string]$package.source -like "git+*") { "Git" } else { "other" }
    $notice.Add("| ``$($package.name)`` | $($package.version) | ``$declared`` | $source |")
}
$noticeContent = ($notice -join "`n") + "`n"

function Normalize-Text([string]$Text) {
    return (($Text.Replace("`r`n", "`n").Replace("`r", "`n").Split("`n") |
        ForEach-Object { $_.TrimEnd() }) -join "`n").TrimEnd() + "`n"
}

function Get-Digest([string]$Text) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

$filePattern = '^(?i)(AUTHORS|COPYING|COPYRIGHT|LICENCE|LICENSE|NOTICE|UNLICENSE)(?:[._-].*)?$'
$texts = @{}
$upstreamTexts = @{}
$records = [Collections.Generic.List[object]]::new()
foreach ($package in $dependencies) {
    $root = Split-Path -Parent ([string]$package.manifest_path)
    $files = @(Get-ChildItem -LiteralPath $root -File | Where-Object { $_.Name -match $filePattern })
    if (-not [string]::IsNullOrWhiteSpace([string]$package.license_file)) {
        $declaredFile = Join-Path $root ([string]$package.license_file)
        if (Test-Path -LiteralPath $declaredFile -PathType Leaf) { $files += Get-Item -LiteralPath $declaredFile }
    }
    $references = [Collections.Generic.List[object]]::new()
    foreach ($file in @($files | Sort-Object Name, FullName -Unique)) {
        $text = Normalize-Text ([IO.File]::ReadAllText($file.FullName))
        $digest = Get-Digest $text
        $texts[$digest] = $text
        $references.Add([ordered]@{ name = $file.Name; digest = $digest; origin = "crate" })
    }
    $repository = [string]$package.repository
    if ($references.Count -gt 0 -and $repository) { $upstreamTexts[$repository] = @($references) }
    $records.Add([ordered]@{
        name = [string]$package.name
        version = [string]$package.version
        license = if ([string]::IsNullOrWhiteSpace([string]$package.license)) { "NOASSERTION" } else { [string]$package.license }
        repository = if ($repository) { $repository } else { "NOASSERTION" }
        authors = @($package.authors | ForEach-Object { [string]$_ })
        references = $references
    })
}

foreach ($record in $records) {
    if ($record.references.Count -eq 0 -and $upstreamTexts.ContainsKey([string]$record.repository)) {
        foreach ($reference in $upstreamTexts[[string]$record.repository]) {
            $record.references.Add([ordered]@{ name = $reference.name; digest = $reference.digest; origin = "same-upstream" })
        }
    }
    if ($record.references.Count -eq 0) {
        if ([string]$record.license -notmatch '(?i)(^|\W)MIT(\W|$)') {
            throw "dependency $($record.name) $($record.version) has no redistributable license text"
        }
        $permission = [IO.File]::ReadAllText((Join-Path $repositoryRoot "LICENSE-MIT"))
        $permission = $permission.Substring($permission.IndexOf("Permission is hereby granted"))
        $attribution = if ($record.authors.Count -gt 0) { $record.authors -join "; " } else { $record.repository }
        $text = Normalize-Text "MIT License`n`nUpstream attribution: $attribution`n`n$permission"
        $digest = Get-Digest $text
        $texts[$digest] = $text
        $record.references.Add([ordered]@{ name = "MIT-metadata-fallback"; digest = $digest; origin = "declared-license" })
    }
}

$bundle = [Collections.Generic.List[string]]::new()
$bundle.Add("THIRD-PARTY COPYRIGHT, NOTICE, AND LICENSE TEXTS")
$bundle.Add("")
$bundle.Add("Generated from Cargo.lock. Identical texts are stored once by SHA-256.")
$bundle.Add("")
$bundle.Add("PACKAGE INDEX")
$bundle.Add("=============")
foreach ($record in $records) {
    $bundle.Add("")
    $bundle.Add("PACKAGE: $($record.name) $($record.version)")
    $bundle.Add("DECLARED-LICENSE: $($record.license)")
    $bundle.Add("UPSTREAM: $($record.repository)")
    foreach ($reference in $record.references) { $bundle.Add("TEXT: $($reference.digest) [$($reference.origin)] $($reference.name)") }
}
$bundle.Add("")
$bundle.Add("DEDUPLICATED TEXTS")
$bundle.Add("==================")
foreach ($digest in @($texts.Keys | Sort-Object)) {
    $bundle.Add("")
    $bundle.Add("--------------------------------------------------------------------------------")
    $bundle.Add("TEXT-SHA256: $digest")
    $bundle.Add("--------------------------------------------------------------------------------")
    $bundle.Add($texts[$digest].TrimEnd())
}
$licenseContent = ($bundle -join "`n") + "`n"

if ($Check) {
    if (-not (Test-Path -LiteralPath $noticePath -PathType Leaf) -or
        [IO.File]::ReadAllText($noticePath).Replace("`r`n", "`n") -cne $noticeContent) {
        throw "THIRD_PARTY_NOTICES.md is stale"
    }
    if (-not (Test-Path -LiteralPath $licensePath -PathType Leaf) -or
        [IO.File]::ReadAllText($licensePath).Replace("`r`n", "`n") -cne $licenseContent) {
        throw "THIRD_PARTY_LICENSES.txt is stale"
    }
    Write-Output "Third-party notices are current for $($dependencies.Count) dependencies."
    exit 0
}

[IO.File]::WriteAllText($noticePath, $noticeContent, [Text.UTF8Encoding]::new($false))
[IO.File]::WriteAllText($licensePath, $licenseContent, [Text.UTF8Encoding]::new($false))
Write-Output "Generated notices and $($texts.Count) deduplicated license texts for $($dependencies.Count) dependencies."
