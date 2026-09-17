$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$fixture = Join-Path ([IO.Path]::GetTempPath()) ("impossible-inferences-foundation-" + [guid]::NewGuid().ToString("N"))
$pwsh = (Get-Process -Id $PID).Path

try {
    New-Item -ItemType Directory -Path (Join-Path $fixture "scripts") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $fixture "vendor") -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $repositoryRoot "scripts/verify-foundation-snapshot.ps1") -Destination (Join-Path $fixture "scripts/verify-foundation-snapshot.ps1")
    Copy-Item -LiteralPath (Join-Path $repositoryRoot "foundation-sync.json") -Destination (Join-Path $fixture "foundation-sync.json")
    Copy-Item -LiteralPath (Join-Path $repositoryRoot "vendor/impossible-server") -Destination (Join-Path $fixture "vendor/impossible-server") -Recurse

    & $pwsh -NoProfile -File (Join-Path $fixture "scripts/verify-foundation-snapshot.ps1") -RepositoryRoot $fixture | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "baseline foundation fixture did not verify" }

    $noticePath = Join-Path $fixture "vendor/impossible-server/NOTICE.md"
    [IO.File]::AppendAllText($noticePath, "mutation`n", [Text.UTF8Encoding]::new($false))
    $manifestPath = Join-Path $fixture "vendor/impossible-server/source-sync.json"
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    $entry = @($manifest.files | Where-Object path -CEQ "NOTICE.md")
    if ($entry.Count -ne 1) { throw "fixture NOTICE entry is not unique" }
    $entry[0].size = (Get-Item -LiteralPath $noticePath).Length
    $entry[0].sha256 = (Get-FileHash -LiteralPath $noticePath -Algorithm SHA256).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText(
        $manifestPath,
        (($manifest | ConvertTo-Json -Depth 16) + "`n"),
        [Text.UTF8Encoding]::new($false)
    )

    & $pwsh -NoProfile -File (Join-Path $fixture "scripts/verify-foundation-snapshot.ps1") -RepositoryRoot $fixture 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        throw "foundation verifier accepted altered entries under the old aggregate tree claim"
    }
    Write-Output "Foundation snapshot adversarial mutation was rejected."
} finally {
    if (Test-Path -LiteralPath $fixture) {
        $resolvedFixture = [IO.Path]::GetFullPath($fixture)
        $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        if (-not $resolvedFixture.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase) -or
            -not [IO.Path]::GetFileName($resolvedFixture).StartsWith("impossible-inferences-foundation-", [StringComparison]::Ordinal)) {
            throw "refusing unsafe foundation-test cleanup path"
        }
        Remove-Item -LiteralPath $resolvedFixture -Recurse -Force
    }
}
