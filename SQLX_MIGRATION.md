# SQLX Migration

This package now provides sqlc-owned public APIs so downstream code does not need
to import `github.com/jmoiron/sqlx`.

## Preferred APIs

- Replace `ProvideConnection(...)` with `ProvideDB(...)`
- Replace `NewConnection(...)` with `NewDB(...)`
- Replace `ProvideConnectionFromSettings(...)` with `ProvideDBFromSettings(...)`
- Replace `NewConnectionFromSettings(...)` with `NewDBFromSettings(...)`
- Replace `NewConnectionWithInterfaces(...)` with `NewDBWithSettings(...)`
- Replace `NewClientWithInterfaces(logger, sqlxDB, executor, qbConfig)` with `NewClientWithDB(logger, sqlc.WrapDB(db, driverName), executor, qbConfig)` when starting from `*sql.DB`
- Replace `tx.SqlTx()` with `tx.SQLTx()`
- Replace `stmt.QueryxContext(...)` with `stmt.QueryContext(...)`

## Direct Handle Access

- `(*DB).SQLDB()` exposes the underlying `*sql.DB`
- `Tx.SQLTx()` exposes the underlying `*sql.Tx`

## Notes

- Deprecated sqlx-based compatibility helpers remain available during the migration window
- Query, scan, named parameter binding, and placeholder behavior stay unchanged while the internal sqlx-backed adapter remains in place
- Complete sqlx removal is tracked separately by `sqlc-j7r`
