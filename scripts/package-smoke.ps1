[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$distRoot = Join-Path $repositoryRoot "dist"

if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)) {
    $archives = @(Get-ChildItem -LiteralPath $distRoot -Filter "impossible-inferences-*-x86_64-pc-windows-msvc.zip" -File)
    $binaryName = "impossible-inferences.exe"
    $launchers = @("setup.ps1", "serve.ps1")
} elseif ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)) {
    $archives = @(Get-ChildItem -LiteralPath $distRoot -Filter "impossible-inferences-*-x86_64-unknown-linux-gnu.tar.gz" -File)
    $binaryName = "impossible-inferences"
    $launchers = @("setup.sh", "serve.sh")
} else {
    throw "package smoke supports only Windows and Linux"
}
if ($archives.Count -ne 1) { throw "expected exactly one current-platform native archive" }
$archive = $archives[0]
$checksumPath = $archive.FullName + ".sha256"
if (-not (Test-Path -LiteralPath $checksumPath -PathType Leaf)) { throw "package checksum is missing" }
$expected = ((Get-Content -LiteralPath $checksumPath -Raw).Trim() -split '\s+')[0]
$actual = (Get-FileHash -LiteralPath $archive.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actual -cne $expected) { throw "package checksum does not match" }

$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ("impossible-inferences-package-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $temporaryRoot | Out-Null
try {
    if ($archive.Extension -eq ".zip") {
        Expand-Archive -LiteralPath $archive.FullName -DestinationPath $temporaryRoot
    } else {
        & tar -xzf $archive.FullName -C $temporaryRoot
        if ($LASTEXITCODE -ne 0) { throw "package extraction failed" }
    }
    $roots = @(Get-ChildItem -LiteralPath $temporaryRoot -Directory)
    if ($roots.Count -ne 1) { throw "package must contain exactly one root directory" }
    $bundle = $roots[0].FullName
    $required = @(
        $binaryName,
        "README.md",
        "LICENSE-APACHE",
        "LICENSE-MIT",
        "NOTICE.md",
        "THIRD_PARTY_NOTICES.md",
        "THIRD_PARTY_LICENSES.txt",
        "manifests/artifacts-v1.json",
        "docs/configuration.md",
        "docs/grpc-mcp-api.md",
        "docs/http-websocket-api.md"
    ) + $launchers
    foreach ($relative in $required) {
        if (-not (Test-Path -LiteralPath (Join-Path $bundle $relative) -PathType Leaf)) {
            throw "package is missing $relative"
        }
    }
    $forbidden = Get-ChildItem -LiteralPath $bundle -Recurse -File | Where-Object {
        $_.Extension -in @(".gguf", ".onnx", ".safetensors", ".key", ".pem", ".log") -or
        $_.FullName -match '[\\/]runtime-artifacts[\\/]'
    }
    if ($forbidden) { throw "package contains a forbidden runtime or private artifact" }
    & (Join-Path $bundle $binaryName) --version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "packaged binary smoke failed" }
} finally {
    $resolvedTemporary = [IO.Path]::GetFullPath($temporaryRoot)
    $resolvedBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if (-not $resolvedTemporary.StartsWith($resolvedBase, [StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to remove a temporary path outside the system temporary directory"
    }
    if (Test-Path -LiteralPath $resolvedTemporary) {
        Remove-Item -LiteralPath $resolvedTemporary -Recurse -Force
    }
}
Write-Output "Native package checksum, contents, privacy exclusions, and binary smoke passed."
