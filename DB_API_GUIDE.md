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
- For SQLc database metric names, dimensions and emission behavior, see [the SQLc metric documentation](https://gosoline-project.github.io/docs/how-to/databases-sql/sqlc/).

## Go and dependency security baseline

- The module's overall minimum is Go `1.26.11`, raised from the previous main-branch minimum of Go `1.25.0`; downstream builds using Go `1.24` or `1.25` must upgrade. Use a patched toolchain on your selected release line: at least Go `1.26.11` on Go `1.26`, or Go `1.27.4` on Go `1.27` for [GO-2026-6859](https://pkg.go.dev/vuln/GO-2026-6859). The `go.mod` directive enforces only the overall lower bound: it does not reject or replace vulnerable Go `1.27.0`–`1.27.3` toolchains. CI reads the minimum from `go.mod`, and both development tool files select Go `1.26.11`.
- The security update retains the existing SQL drivers and Goose. The current graph selects Gosoline `v0.69.3-0.20261116142819-dd575e2276c4`, needed for the OpenTelemetry log `v0.21.0` API. Security dependencies include `filippo.io/edwards25519` `v1.1.1`, `github.com/klauspost/compress` `v1.20.2`, and `golang.org/x/net` `v0.62.0`. The Gosoline revision also changes its guard interface, removes CrateDB support, changes Avro codec and S3 checksum behavior, and updates telemetry and compression. Its default Avro codec still has no map-allocation limit; see [Gosoline's compatibility notes](https://github.com/justtrackio/gosoline/blob/dd575e2276c4a048ad9fe1a68857d0c0da899b09/README.md#requirements-and-upgrade-compatibility) before upgrading downstream integrations.
- An October 2026 `govulncheck` `v1.9.0` source scan of the earlier dependency graph on Go `1.26.10` with the vulnerability database updated on 2026-10-21 reported no imported-package or reachable-symbol vulnerabilities. It retained module-only advisory [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932), which concerns unmaintained `golang.org/x/crypto/openpgp` packages not imported by that module graph and has no fixed release. Rescan the current graph rather than assuming the same result.
- Earlier runtime validation covered real MySQL and PostgreSQL authentication, named writes, transactions, and UTF-8 readback; Prometheus zstd responses; malformed S2 dictionary rejection; Edwards multiscalar operations; and a loopback SSH handshake. These historical checks do not establish behavior of the current dependency graph, the safety of unused OpenPGP packages, or the safety of every downstream application.
