package sqlc

import (
	"context"
)

// DeleteQueryBuilderG is a generic wrapper around DeleteQueryBuilder that provides
// type-safe operations for deleting records. The generic type parameter helps with
// code organization and type consistency, even though DELETE queries don't return data.
//
// Example usage:
//
//	type User struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//
//	// Delete with simple condition
//	result, err := DeleteG[User]("users").
//	    WithClient(client).
//	    Where("status = ?", "inactive").
//	    Exec(ctx)
//
//	// Delete with limit
//	result, err := DeleteG[User]("users").
//	    WithClient(client).
//	    Where("created_at < ?", cutoffDate).
//	    OrderBy("created_at ASC").
//	    Limit(1000).
//	    Exec(ctx)
type DeleteQueryBuilderG[T any] struct {
	qb *DeleteQueryBuilder
}

// DeleteG creates a new generic DeleteQueryBuilder for the specified table.
// This is the entry point for building type-safe DELETE queries.
//
// Example:
//
//	DeleteG[User]("users")                   // DELETE FROM `users`
//	DeleteG[Order]("orders")                 // DELETE FROM `orders`
func DeleteG[T any](table string) *DeleteQueryBuilderG[T] {
	return &DeleteQueryBuilderG[T]{
		qb: Delete(table),
	}
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Exec() method.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := DeleteG[User]("users").WithClient(client).Where("status = ?", "inactive")
//	result, err := query.Exec(ctx)
func (q *DeleteQueryBuilderG[T]) WithClient(client Querier) *DeleteQueryBuilderG[T] {
	return &DeleteQueryBuilderG[T]{
		qb: q.qb.WithClient(client),
	}
}

// WithConfig sets a custom configuration for struct tags and placeholders.
// Returns a new query builder with the specified configuration.
//
// Example:
//
//	config := &QueryBuilderConfig{StructTag: "json", Placeholder: "$"}
//	query := DeleteG[User]("users").WithConfig(config).Where(...)
func (q *DeleteQueryBuilderG[T]) WithConfig(config *QueryBuilderConfig) *DeleteQueryBuilderG[T] {
	return &DeleteQueryBuilderG[T]{
		qb: q.qb.WithConfig(config),
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
//	Where("status = ?", "inactive")                  // WHERE status = ?
//	Where(Col("age").Lt(18))                         // WHERE `age` < ?
//	Where(And(Col("a").Eq(1), Col("b").Eq(2)))       // WHERE (`a` = ? AND `b` = ?)
//	Where(Eq{"status": "inactive", "role": "guest"}) // WHERE (`role` = ? AND `status` = ?)
func (q *DeleteQueryBuilderG[T]) Where(condition any, params ...any) *DeleteQueryBuilderG[T] {
	return &DeleteQueryBuilderG[T]{
		qb: q.qb.Where(condition, params...),
	}
}

// OrderBy sets the ORDER BY clause for the query.
// Accepts strings (column names with optional ASC/DESC) or *Expression objects.
// Replaces any previously set ORDER BY clause.
// Returns a new query builder with the ORDER BY clause set.
//
// Note: ORDER BY in DELETE queries is typically used with LIMIT to control
// which rows are deleted when you want to delete only a subset of matching rows.
// This is a MySQL-specific feature.
//
// Example:
//
//	OrderBy("created_at ASC")                       // ORDER BY `created_at` ASC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
func (q *DeleteQueryBuilderG[T]) OrderBy(cols ...any) *DeleteQueryBuilderG[T] {
	return &DeleteQueryBuilderG[T]{
		qb: q.qb.OrderBy(cols...),
	}
}

// Limit sets the maximum number of rows to delete.
// Returns a new query builder with the LIMIT clause set.
//
// Note: LIMIT in DELETE queries restricts the number of rows that will be deleted.
// This is commonly used with ORDER BY to delete a specific subset of rows.
// This is a MySQL-specific feature.
//
// Example:
//
//	Limit(10)   // LIMIT 10
//	OrderBy("created_at ASC").Limit(5)  // Delete only the 5 oldest rows
func (q *DeleteQueryBuilderG[T]) Limit(limit int) *DeleteQueryBuilderG[T] {
	return &DeleteQueryBuilderG[T]{
		qb: q.qb.Limit(limit),
	}
}

// ToSql generates the final SQL DELETE query string with positional parameters.
// Returns the SQL string, parameters slice, and any error encountered during building.
//
// Example:
//
//	sql, args, err := DeleteG[User]("users").
//		Where("status = ?", "inactive").
//		ToSql()
//	// sql: "DELETE FROM `users` WHERE status = ?"
//	// args: []any{"inactive"}
func (q *DeleteQueryBuilderG[T]) ToSql() (query string, params []any, err error) {
	return q.qb.ToSql()
}

// Exec executes the delete query using the attached client.
// Returns the result (with RowsAffected) and any error.
// Requires that a client has been set via WithClient().
//
// Example:
//
//	result, err := DeleteG[User]("users").
//		WithClient(client).
//		Where("status = ?", "inactive").
//		Exec(ctx)
//
//	rowsAffected, _ := result.RowsAffected()
func (q *DeleteQueryBuilderG[T]) Exec(ctx context.Context) (Result, error) {
	return q.qb.Exec(ctx)
}
