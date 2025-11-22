package sqlc

import (
	"context"
)

// InsertQueryBuilderG is a generic wrapper around InsertQueryBuilder that provides
// type-safe operations for inserting records. Instead of requiring explicit type
// assertions, the generic builder works directly with the specified type.
//
// Example usage:
//
//	type User struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//
//	// Insert a single user
//	result, err := IntoG[User]("users").
//	    WithClient(client).
//	    Records(user).
//	    Exec(ctx)
//
//	// Insert multiple users
//	result, err := IntoG[User]("users").
//	    WithClient(client).
//	    Records(user1, user2, user3).
//	    Exec(ctx)
type InsertQueryBuilderG[T any] struct {
	qb *InsertQueryBuilder
}

// IntoG creates a new generic InsertQueryBuilder for the specified table.
// This is the entry point for building type-safe INSERT queries.
//
// Example:
//
//	IntoG[User]("users")                   // INSERT INTO `users`
//	IntoG[Order]("orders")                 // INSERT INTO `orders`
func IntoG[T any](table string) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: Into(table),
	}
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Exec() method.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := IntoG[User]("users").WithClient(client).Records(user)
//	result, err := query.Exec(ctx)
func (q *InsertQueryBuilderG[T]) WithClient(client Querier) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.WithClient(client),
	}
}

// WithConfig sets a custom configuration for struct tags and placeholders.
// Returns a new query builder with the specified configuration.
//
// Example:
//
//	config := &Config{StructTag: "json", Placeholder: "$"}
//	query := IntoG[User]("users").WithConfig(config).Records(user)
func (q *InsertQueryBuilderG[T]) WithConfig(config *Config) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.WithConfig(config),
	}
}

// Insert sets the query to use INSERT mode (default).
// Returns a new query builder configured for INSERT operations.
//
// Example:
//
//	IntoG[User]("users").Insert().Records(user)  // INSERT INTO `users` ...
func (q *InsertQueryBuilderG[T]) Insert() *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Insert(),
	}
}

// Replace sets the query to use REPLACE mode.
// REPLACE works like INSERT, except that if an old row in the table has the same value
// as a new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted before the new row is inserted.
// Returns a new query builder configured for REPLACE operations.
//
// Example:
//
//	IntoG[User]("users").Replace().Records(user)  // REPLACE INTO `users` ...
func (q *InsertQueryBuilderG[T]) Replace() *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Replace(),
	}
}

// Ignore adds the IGNORE modifier to the insert query.
// With IGNORE, rows that would cause duplicate-key errors are silently skipped.
// Returns a new query builder with IGNORE modifier set.
//
// Example:
//
//	IntoG[User]("users").Ignore().Records(user)  // INSERT IGNORE INTO `users` ...
func (q *InsertQueryBuilderG[T]) Ignore() *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Ignore(),
	}
}

// LowPriority adds the LOW_PRIORITY modifier to the insert query.
// LOW_PRIORITY delays execution until no other clients are reading from the table.
// Returns a new query builder with LOW_PRIORITY modifier set.
//
// Example:
//
//	IntoG[User]("users").LowPriority().Records(user)  // INSERT LOW_PRIORITY INTO `users` ...
func (q *InsertQueryBuilderG[T]) LowPriority() *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.LowPriority(),
	}
}

// HighPriority adds the HIGH_PRIORITY modifier to the insert query.
// HIGH_PRIORITY overrides the effect of --low-priority-updates server option.
// Returns a new query builder with HIGH_PRIORITY modifier set.
//
// Example:
//
//	IntoG[User]("users").HighPriority().Records(user)  // INSERT HIGH_PRIORITY INTO `users` ...
func (q *InsertQueryBuilderG[T]) HighPriority() *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.HighPriority(),
	}
}

// Delayed adds the DELAYED modifier to the insert query.
// DELAYED causes the server to put the row(s) in a buffer and return immediately.
// Returns a new query builder with DELAYED modifier set.
//
// Example:
//
//	IntoG[User]("users").Delayed().Records(user)  // INSERT DELAYED INTO `users` ...
func (q *InsertQueryBuilderG[T]) Delayed() *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Delayed(),
	}
}

// OnDuplicateKeyUpdate adds an ON DUPLICATE KEY UPDATE clause to the insert query.
// This clause specifies what to do when a duplicate key error occurs.
// Accepts one or more Assignment values that specify column updates.
// Returns a new query builder with the ON DUPLICATE KEY UPDATE clause added.
//
// Example with value:
//
//	IntoG[User]("users").
//		Records(user).
//		OnDuplicateKeyUpdate(
//			sqlc.Assign("count", 10),  // Set count to specific value
//		)
//
// Example with expression:
//
//	IntoG[User]("users").
//		Records(user).
//		OnDuplicateKeyUpdate(
//			sqlc.AssignExpr("count", "count + 1"),  // Increment count
//		)
func (q *InsertQueryBuilderG[T]) OnDuplicateKeyUpdate(assignments ...Assignment) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.OnDuplicateKeyUpdate(assignments...),
	}
}

// Columns sets the column list for the insert query.
// This method replaces any previously set columns.
// Column names will be automatically quoted in the generated SQL.
// Returns a new query builder with the specified columns.
//
// Example:
//
//	IntoG[User]("users").Columns("id", "name", "email")
//	// INSERT INTO `users` (`id`, `name`, `email`) VALUES (...)
func (q *InsertQueryBuilderG[T]) Columns(cols ...string) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Columns(cols...),
	}
}

// Values adds a single row of values to the insert query.
// The number of values must match the number of columns.
// Multiple calls to Values will add multiple rows (bulk insert).
// Uses positional parameters (?) in the generated SQL.
// Returns a new query builder with the values added.
//
// Example:
//
//	IntoG[User]("users").
//		Columns("id", "name", "email").
//		Values(1, "John", "john@example.com").
//		Values(2, "Jane", "jane@example.com")
func (q *InsertQueryBuilderG[T]) Values(values ...any) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Values(values...),
	}
}

// ValuesRows adds multiple rows of values to the insert query in a single call.
// Each row must have the same number of values, matching the number of columns.
// Uses positional parameters (?) in the generated SQL.
// Returns a new query builder with the rows added.
//
// Example:
//
//	IntoG[User]("users").
//		Columns("id", "name").
//		ValuesRows(
//			[]any{1, "John"},
//			[]any{2, "Jane"},
//			[]any{3, "Bob"},
//		)
func (q *InsertQueryBuilderG[T]) ValuesRows(rows ...[]any) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.ValuesRows(rows...),
	}
}

// ValuesMaps adds multiple rows using maps where keys are column names and values are the data.
// If columns have not been set, they will be inferred from the first map's keys (sorted alphabetically).
// All maps must contain the same keys.
// Uses named parameters (`:key`) and NamedExec for execution.
// Returns a new query builder with the map data added.
//
// Example:
//
//	IntoG[User]("users").ValuesMaps(
//		map[string]any{"id": 1, "name": "John", "email": "john@example.com"},
//		map[string]any{"id": 2, "name": "Jane", "email": "jane@example.com"},
//	)
func (q *InsertQueryBuilderG[T]) ValuesMaps(maps ...map[string]any) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.ValuesMaps(maps...),
	}
}

// Records adds one or more struct records to the insert query.
// Accepts variadic parameters - each parameter can be:
//   - A struct or pointer to struct with `db` tags
//   - A slice or array of structs/pointers with `db` tags
//
// Slice/array arguments are automatically flattened, so Records([]T{r1, r2}) is equivalent to Records(r1, r2).
// All structs must have the same structure and `db` tags.
// If columns have not been explicitly set, they will be inferred from the first struct.
// Returns a new query builder with the records added.
//
// Example with single record:
//
//	user := User{ID: 1, Name: "John", Email: "john@example.com"}
//	IntoG[User]("users").WithClient(client).Records(user).Exec(ctx)
//
// Example with multiple records:
//
//	IntoG[User]("users").WithClient(client).Records(user1, user2, user3).Exec(ctx)
//
// Example with slice:
//
//	users := []User{user1, user2, user3}
//	IntoG[User]("users").WithClient(client).Records(users).Exec(ctx)
func (q *InsertQueryBuilderG[T]) Records(records ...any) *InsertQueryBuilderG[T] {
	return &InsertQueryBuilderG[T]{
		qb: q.qb.Records(records...),
	}
}

// ToSql generates the final SQL INSERT query string with positional parameters.
// Returns the SQL string, parameters slice, and any error encountered during building.
//
// Example:
//
//	sql, args, err := IntoG[User]("users").
//		Records(user).
//		ToSql()
func (q *InsertQueryBuilderG[T]) ToSql() (query string, params []any, err error) {
	return q.qb.ToSql()
}

// ToNamedSql generates SQL with named parameters for use with NamedExec.
// Returns the SQL string and the untouched records/maps (single item or slice).
//
// Example:
//
//	sql, params, err := IntoG[User]("users").Records(user).ToNamedSql()
func (q *InsertQueryBuilderG[T]) ToNamedSql() (query string, params []any, err error) {
	return q.qb.ToNamedSql()
}

// Exec executes the insert query using the attached client.
// Returns the result (with LastInsertId and RowsAffected) and any error.
// Requires that a client has been set via WithClient().
//
// Example:
//
//	result, err := IntoG[User]("users").
//		WithClient(client).
//		Records(user).
//		Exec(ctx)
//
//	lastID, _ := result.LastInsertId()
//	rowsAffected, _ := result.RowsAffected()
func (q *InsertQueryBuilderG[T]) Exec(ctx context.Context) (Result, error) {
	return q.qb.Exec(ctx)
}
