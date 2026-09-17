[CmdletBinding()]
param([switch]$Offline)

$ErrorActionPreference = "Stop"
$arguments = @("setup", "--artifact-root", (Join-Path $PSScriptRoot "runtime-artifacts"))
if ($Offline) { $arguments += "--offline" }
& (Join-Path $PSScriptRoot "impossible-inferences.exe") @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
