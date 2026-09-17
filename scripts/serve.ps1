[CmdletBinding()]
param(
    [string]$ArtifactRoot = "runtime-artifacts"
)

$ErrorActionPreference = "Stop"
& cargo run --locked -p impossible-inferences-server -- serve --artifact-root $ArtifactRoot
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
