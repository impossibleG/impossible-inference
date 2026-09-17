[CmdletBinding()]
param(
    [string]$ArtifactRoot = "runtime-artifacts",
    [switch]$Offline
)

$ErrorActionPreference = "Stop"
$arguments = @(
    "run", "--locked", "-p", "impossible-inferences-server", "--",
    "setup", "--artifact-root", $ArtifactRoot
)
if ($Offline) { $arguments += "--offline" }
& cargo @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
