[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$SkipCleanCheck
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Assert-CleanTree {
    Push-Location $repositoryRoot
    try {
        $status = & git status --porcelain=v1 --untracked-files=all
        if ($LASTEXITCODE -ne 0) { throw "git status failed" }
        if ($status) { throw "tracked or nonignored untracked files are present" }
    } finally {
        Pop-Location
    }
}

if (-not $SkipCleanCheck) { Assert-CleanTree }

$cargoManifest = Get-Content (Join-Path $repositoryRoot "Cargo.toml") -Raw
$versionMatch = [regex]::Match($cargoManifest, '(?m)^version = "([0-9]+\.[0-9]+\.[0-9]+)"$')
if (-not $versionMatch.Success) { throw "workspace version was not found" }
$version = $versionMatch.Groups[1].Value

if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)) {
    $target = "x86_64-pc-windows-msvc"
    $sourceBinary = Join-Path $repositoryRoot "target/release/impossible-inferences-server.exe"
    $binaryName = "impossible-inferences.exe"
    $archiveExtension = ".zip"
    $platformDirectory = "windows"
    $launchers = @("setup.ps1", "serve.ps1")
} elseif ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)) {
    $target = "x86_64-unknown-linux-gnu"
    $sourceBinary = Join-Path $repositoryRoot "target/release/impossible-inferences-server"
    $binaryName = "impossible-inferences"
    $archiveExtension = ".tar.gz"
    $platformDirectory = "linux"
    $launchers = @("setup.sh", "serve.sh")
} else {
    throw "native packaging supports only Windows x86-64 and Linux x86-64"
}

if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne [System.Runtime.InteropServices.Architecture]::X64) {
    throw "native packaging supports only x86-64"
}

if (-not $SkipBuild) {
    Push-Location $repositoryRoot
    try {
        & cargo build --locked --release -p impossible-inferences-server
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } finally {
        Pop-Location
    }
}
if (-not (Test-Path -LiteralPath $sourceBinary -PathType Leaf)) { throw "release binary is missing" }

$distRoot = Join-Path $repositoryRoot "dist"
$bundleName = "impossible-inferences-v$version-$target"
$stage = Join-Path $distRoot $bundleName
$archive = Join-Path $distRoot ($bundleName + $archiveExtension)
$checksum = $archive + ".sha256"

New-Item -ItemType Directory -Path $distRoot -Force | Out-Null
if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
if (Test-Path -LiteralPath $archive) { Remove-Item -LiteralPath $archive -Force }
if (Test-Path -LiteralPath $checksum) { Remove-Item -LiteralPath $checksum -Force }
New-Item -ItemType Directory -Path (Join-Path $stage "docs") -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $stage "manifests") -Force | Out-Null

Copy-Item -LiteralPath $sourceBinary -Destination (Join-Path $stage $binaryName)
Copy-Item -LiteralPath (Join-Path $repositoryRoot "packaging/README.md") -Destination (Join-Path $stage "README.md")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "LICENSE-APACHE") -Destination $stage
Copy-Item -LiteralPath (Join-Path $repositoryRoot "LICENSE-MIT") -Destination $stage
Copy-Item -LiteralPath (Join-Path $repositoryRoot "NOTICE.md") -Destination $stage
Copy-Item -LiteralPath (Join-Path $repositoryRoot "THIRD_PARTY_NOTICES.md") -Destination $stage
Copy-Item -LiteralPath (Join-Path $repositoryRoot "THIRD_PARTY_LICENSES.txt") -Destination $stage
Copy-Item -LiteralPath (Join-Path $repositoryRoot "manifests/artifacts-v1.json") -Destination (Join-Path $stage "manifests")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs/architecture.md") -Destination (Join-Path $stage "docs")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs/artifacts.md") -Destination (Join-Path $stage "docs")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs/configuration.md") -Destination (Join-Path $stage "docs")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs/grpc-mcp-api.md") -Destination (Join-Path $stage "docs")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs/http-websocket-api.md") -Destination (Join-Path $stage "docs")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs/product-contract.md") -Destination (Join-Path $stage "docs")
foreach ($launcher in $launchers) {
    Copy-Item -LiteralPath (Join-Path $repositoryRoot "packaging/$platformDirectory/$launcher") -Destination $stage
}

$forbidden = Get-ChildItem -LiteralPath $stage -Recurse -File | Where-Object {
    $_.Extension -in @(".gguf", ".onnx", ".safetensors", ".key", ".pem", ".log")
}
if ($forbidden) { throw "package staging contains a forbidden runtime or private artifact" }

if ($archiveExtension -eq ".zip") {
    Compress-Archive -LiteralPath $stage -DestinationPath $archive -CompressionLevel Optimal
} else {
    & tar -czf $archive -C $distRoot $bundleName
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$hash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
Set-Content -LiteralPath $checksum -Value "$hash  $([System.IO.Path]::GetFileName($archive))" -Encoding utf8NoBOM
if (-not $SkipCleanCheck) { Assert-CleanTree }
Write-Output $archive
Write-Output $checksum
