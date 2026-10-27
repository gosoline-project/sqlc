# Database API Guide

This package exposes sqlc-owned database APIs directly.

## Current APIs

- Use `ProvideDB(...)` and `NewDB(...)` to create database handles
- Use `ProvideDBFromSettings(...)`, `NewDBFromSettings(...)`, and `NewDBWithSettings(...)` for settings-based construction
- Use `NewClientWithDB(logger, sqlc.WrapDB(db, driverName), executor, qbConfig)` when starting from `*sql.DB`
- Use `tx.SQLTx()` for direct transaction access
- Use `stmt.QueryContext(...)` for prepared queries

## Direct Handle Access

- `(*DB).SQLDB()` exposes the underlying `*sql.DB`
- `Tx.SQLTx()` exposes the underlying `*sql.Tx`

## Notes

- Query, scan, named parameter binding, and placeholder behavior are now implemented internally by sqlc
- Downstream code should rely on sqlc-owned types and `database/sql` handles only

## Go and dependency security baseline

- The module's overall minimum is Go `1.26.10`, intentionally raised from Go `1.24.0`; downstream builds using Go `1.24` or `1.25` must upgrade. Use a patched toolchain on your selected release line: at least Go `1.26.10` on Go `1.26`, or Go `1.27.3` on Go `1.27` for [GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809). The `go.mod` directive enforces only the overall lower bound: it does not reject or replace vulnerable Go `1.27.0`–`1.27.2` toolchains. CI reads the minimum from `go.mod`, and both development tool files select Go `1.26.10`.
- Security updates retain the existing SQL drivers, Goose, and Gosoline APIs: `filippo.io/edwards25519` `v1.1.1`, `github.com/klauspost/compress` `v1.18.7`, `golang.org/x/crypto` `v0.56.0`, and `golang.org/x/text` `v0.41.0`. The selected crypto dependency requires `x/net` `v0.57.0`, `x/sys` `v0.47.0`, and `x/sync` `v0.22.0` through module version selection.
- A `govulncheck` `v1.9.0` source scan on Go `1.26.10` with the vulnerability database updated on 2026-10-21 reports no imported-package or reachable-symbol vulnerabilities. It retains module-only advisory [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932), which concerns unmaintained `golang.org/x/crypto/openpgp` packages not imported by this module and has no fixed release.
- Runtime validation covered real MySQL and PostgreSQL authentication, named writes, transactions, and UTF-8 readback; Prometheus zstd responses; malformed S2 dictionary rejection; Edwards multiscalar operations; and a loopback SSH handshake. These checks do not establish the safety of unused OpenPGP packages or of every downstream application.
