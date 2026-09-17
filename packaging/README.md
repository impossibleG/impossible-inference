# Impossible Inference native package

This package contains the Impossible Inference server binary and its exact v0.1 API and artifact
contracts. It does not contain a model or native inference runtime.

Run one setup command, then one serve command from this directory:

- Windows PowerShell: `./setup.ps1`, then `./serve.ps1`.
- Linux: `./setup.sh`, then `./serve.sh`.

Setup downloads only the model and runtime identities pinned in `manifests/artifacts-v1.json`,
verifies their byte lengths and SHA-256 digests, and installs them beneath the package-local
ignored `runtime-artifacts` directory. After setup, `./setup.ps1 -Offline` or `./setup.sh
--offline` verifies and reconstructs the installation without network access. Ordinary serve does
not download anything.

HTTP/MCP listens on `127.0.0.1:8080`; gRPC listens on `127.0.0.1:50051`. See `docs/` for the API,
configuration, artifact, architecture, and product contracts.
