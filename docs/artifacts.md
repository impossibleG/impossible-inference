# Curated v0.1 artifacts

Version 0.1 pins one small model and one CPU runtime build per supported operating system. Setup
uses the committed `manifests/artifacts-v1.json`; it never resolves a mutable "latest" URL.

The model is the official Qwen `Qwen2.5-0.5B-Instruct-GGUF` Q4_K_M file at immutable revision
`9217f5db79a29953eb74d5343926648285ec7e67`. The repository identifies it as Apache-2.0, and its
official metadata reports 491,400,032 bytes with SHA-256
`74a4da8c9fdbcd15bd1f6d01d621410d31c6fc00986f5eb687824e7b93d7a9db`.

The runtime is the official llama.cpp `b10964` CPU release underlying semantic release `v0.4.1`,
source commit `b29c606e28a01b1bc8c1351026a0fa6e616bf6c4`, licensed under MIT. The manifest pins the official
Windows x64 CPU ZIP and Ubuntu x64 CPU tarball by URL, byte length, and the SHA-256 digests published
by GitHub's release API.

`impossible-inferences setup` downloads both artifacts into a managed staging directory, checks the
exact byte length and SHA-256 while streaming, extracts the runtime without accepting escaping
archive paths, writes an installation record, and atomically promotes the complete installation.
The verified runtime archive is retained so `setup --offline` can reconstruct extracted runtime
files without network access. Ordinary `serve`, `doctor`, and `status` commands never download.

The initial Impossible Server snapshot does not supply this installer. This is product-owned code.
The first release supports only Windows x86-64 and Linux x86-64 CPU targets; other targets fail
before any download.
