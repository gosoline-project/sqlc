package sqlc

import (
	"context"
)

// UpdateQueryBuilderG is a generic wrapper around UpdateQueryBuilder that provides
// type-safe operations for updating records. Instead of requiring explicit type
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
//	// Update using a record
//	user := User{Name: "John", Email: "john@example.com"}
//	result, err := UpdateG[User]("users").
//	    WithClient(client).
//	    SetRecord(user).
//	    Where("id = ?", 123).
//	    Exec(ctx)
//
//	// Update using Set
//	result, err := UpdateG[User]("users").
//	    WithClient(client).
//	    Set("name", "John").
//	    Where("id = ?", 123).
//	    Exec(ctx)
type UpdateQueryBuilderG[T any] struct {
	qb *UpdateQueryBuilder
}

// UpdateG creates a new generic UpdateQueryBuilder for the specified table.
// This is the entry point for building type-safe UPDATE queries.
//
// Example:
//
//	UpdateG[User]("users")                   // UPDATE `users`
//	UpdateG[Order]("orders")                 // UPDATE `orders`
func UpdateG[T any](table string) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: Update(table),
	}
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Exec() method.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := UpdateG[User]("users").WithClient(client).Set("name", "John")
//	result, err := query.Exec(ctx)
func (q *UpdateQueryBuilderG[T]) WithClient(client Querier) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.WithClient(client),
	}
}

// WithConfig sets a custom configuration for struct tags and placeholders.
// Returns a new query builder with the specified configuration.
//
// Example:
//
//	config := &Config{StructTag: "json", Placeholder: "$"}
//	query := UpdateG[User]("users").WithConfig(config).Set(...)
func (q *UpdateQueryBuilderG[T]) WithConfig(config *Config) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.WithConfig(config),
	}
}

// Set adds a single column assignment to the update query.
// The value will be parameterized in the generated SQL.
// Multiple calls to Set will add multiple assignments.
// Returns a new query builder with the assignment added.
//
// Example:
//
//	UpdateG[User]("users").
//		Set("name", "John").
//		Set("email", "john@example.com")
//	// UPDATE `users` SET `name` = ?, `email` = ?
func (q *UpdateQueryBuilderG[T]) Set(column string, value any) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.Set(column, value),
	}
}

// SetExpr adds a single column assignment using a raw SQL expression.
// The expression will be inserted directly into the SQL without parameterization.
// Multiple calls to SetExpr will add multiple assignments.
// Returns a new query builder with the assignment added.
//
// Example:
//
//	UpdateG[User]("users").
//		SetExpr("count", "count + 1").
//		SetExpr("updated_at", "NOW()")
//	// UPDATE `users` SET `count` = count + 1, `updated_at` = NOW()
func (q *UpdateQueryBuilderG[T]) SetExpr(column string, expression string) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.SetExpr(column, expression),
	}
}

// SetMap adds multiple column assignments from a map where keys are column names
// and values are the data to set.
// Values are extracted during SQL generation and use positional parameters.
// Returns a new query builder with the map data added.
//
// Example:
//
//	UpdateG[User]("users").SetMap(
//		map[string]any{"name": "John", "email": "john@example.com"},
//	)
//	// UPDATE `users` SET `email` = ?, `name` = ?
func (q *UpdateQueryBuilderG[T]) SetMap(m map[string]any) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.SetMap(m),
	}
}

// SetRecord adds column assignments from a struct record.
// The struct must have `db` tags to identify column mappings.
// Values are extracted during SQL generation and use positional parameters.
// Returns a new query builder with the record added.
//
// Example:
//
//	user := User{Name: "John", Email: "john@example.com"}
//	UpdateG[User]("users").SetRecord(user).Where("id = ?", 1)
//	// UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?
func (q *UpdateQueryBuilderG[T]) SetRecord(record T) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.SetRecord(record),
	}
}

// Where adds a WHERE condition to the query.
// Multiple Where() calls are combined with AND.
// Accepts either:
//   - A raw SQL string with placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//   - An Eq map for creating equality conditions from column-value pairs
//
// Returns a new query builder with the condition added.
//
// Example:
//
//	Where("status = ?", "active")                    // WHERE status = ?
//	Where(Col("age").Gt(18))                         // WHERE `age` > ?
//	Where(And(Col("a").Eq(1), Col("b").Eq(2)))       // WHERE (`a` = ? AND `b` = ?)
//	Where(Eq{"status": "active", "role": "admin"})   // WHERE (`role` = ? AND `status` = ?)
func (q *UpdateQueryBuilderG[T]) Where(condition any, params ...any) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.Where(condition, params...),
	}
}

// OrderBy sets the ORDER BY clause for the query.
// Accepts strings (column names with optional ASC/DESC) or *Expression objects.
// Replaces any previously set ORDER BY clause.
// Returns a new query builder with the ORDER BY clause set.
//
// Note: ORDER BY in UPDATE queries is typically used with LIMIT to control
// which rows are updated when you want to update only a subset of matching rows.
//
// Example:
//
//	OrderBy("created_at DESC")                      // ORDER BY `created_at` DESC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
func (q *UpdateQueryBuilderG[T]) OrderBy(cols ...any) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.OrderBy(cols...),
	}
}

// Limit sets the maximum number of rows to update.
// Returns a new query builder with the LIMIT clause set.
//
// Note: LIMIT in UPDATE queries restricts the number of rows that will be updated.
// This is commonly used with ORDER BY to update a specific subset of rows.
//
// Example:
//
//	Limit(10)   // LIMIT 10
//	OrderBy("created_at ASC").Limit(5)  // Update only the 5 oldest rows
func (q *UpdateQueryBuilderG[T]) Limit(limit int) *UpdateQueryBuilderG[T] {
	return &UpdateQueryBuilderG[T]{
		qb: q.qb.Limit(limit),
	}
}

// ToSql generates the final SQL UPDATE query string with positional parameters.
// Returns the SQL string, parameters slice, and any error encountered during building.
//
// Example:
//
//	sql, args, err := UpdateG[User]("users").
//		Set("name", "John").
//		Where("id = ?", 1).
//		ToSql()
//	// sql: "UPDATE `users` SET `name` = ? WHERE id = ?"
//	// args: []any{"John", 1}
func (q *UpdateQueryBuilderG[T]) ToSql() (query string, params []any, err error) {
	return q.qb.ToSql()
}

// Exec executes the update query using the attached client.
// Returns the result (with RowsAffected) and any error.
// Requires that a client has been set via WithClient().
//
// Example:
//
//	result, err := UpdateG[User]("users").
//		WithClient(client).
//		Set("name", "John").
//		Where("id = ?", 1).
//		Exec(ctx)
//
//	rowsAffected, _ := result.RowsAffected()
func (q *UpdateQueryBuilderG[T]) Exec(ctx context.Context) (Result, error) {
	return q.qb.Exec(ctx)
}
