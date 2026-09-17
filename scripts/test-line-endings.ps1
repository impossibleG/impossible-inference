$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$fixture = Join-Path ([IO.Path]::GetTempPath()) ("impossible-inferences-clone-" + [guid]::NewGuid().ToString("N"))

try {
    & git -c core.autocrlf=true clone --quiet --no-local -- $repositoryRoot $fixture
    if ($LASTEXITCODE -ne 0) { throw "fresh clone fixture failed" }
    & pwsh -NoProfile -File (Join-Path $fixture "scripts/verify-foundation-snapshot.ps1") -RepositoryRoot $fixture | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "foundation snapshot changed under core.autocrlf=true" }
    $changes = @(& git -C $fixture status --porcelain=v1)
    if ($LASTEXITCODE -ne 0 -or $changes.Count -ne 0) {
        throw "fresh clone has line-ending drift"
    }
    Write-Output "Fresh core.autocrlf=true clone preserved deterministic snapshot bytes."
} finally {
    if (Test-Path -LiteralPath $fixture) {
        $resolvedFixture = [IO.Path]::GetFullPath($fixture)
        $resolvedTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        if (-not $resolvedFixture.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase) -or
            -not [IO.Path]::GetFileName($resolvedFixture).StartsWith("impossible-inferences-clone-", [StringComparison]::Ordinal)) {
            throw "refusing unsafe clone-test cleanup path"
        }
        Remove-Item -LiteralPath $resolvedFixture -Recurse -Force
    }
}
