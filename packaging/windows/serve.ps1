[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "impossible-inferences.exe") serve --artifact-root (Join-Path $PSScriptRoot "runtime-artifacts")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
