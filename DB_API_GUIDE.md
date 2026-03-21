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
