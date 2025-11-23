package sqlc

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// DeleteQueryBuilder provides a fluent API for building SQL DELETE queries.
// It implements an immutable builder pattern - each method returns a new instance
// rather than modifying the receiver. This allows for query reuse and prevents
// accidental mutations.
//
// All DELETE queries use positional parameters (?).
//
// Example with positional parameters:
//
//	query := Delete("users").
//		Where("status = ?", "inactive")
//	result, err := query.Exec(ctx)
//
// Example with Expression:
//
//	result, err := Delete("users").
//		Where(Col("created_at").Lt("2020-01-01")).
//		Exec(ctx)
//
// Example with ORDER BY and LIMIT (MySQL-specific):
//
//	result, err := Delete("logs").
//		Where("level = ?", "debug").
//		OrderBy("created_at ASC").
//		Limit(1000).
//		Exec(ctx)
type DeleteQueryBuilder struct {
	client       Querier
	table        string
	sqlerWhere   *SqlerWhere
	sqlerOrderBy *SqlerOrderBy
	limitValue   *int
	config       *QueryBuilderConfig // Configuration for struct tags and placeholders
	err          error
}

// Delete creates a new DeleteQueryBuilder for the specified table.
// This is the entry point for building DELETE queries.
//
// Example:
//
//	Delete("users")                   // DELETE FROM `users`
//	Delete("logs")                    // DELETE FROM `logs`
func Delete(table string) *DeleteQueryBuilder {
	cfg := DefaultConfig()
	return &DeleteQueryBuilder{
		table:        table,
		sqlerWhere:   NewSqlerWhere().WithConfig(cfg),
		sqlerOrderBy: NewSqlerOrderBy().WithConfig(cfg),
		config:       cfg,
	}
}

// copyQuery creates a shallow copy of the query builder.
// This is used internally to implement the immutable builder pattern.
// Each builder method creates a copy, modifies it, and returns the new copy.
func (q *DeleteQueryBuilder) copyQuery() *DeleteQueryBuilder {
	// Copy the SqlerWhere by creating a new instance with copied slices
	newSqlerWhere := &SqlerWhere{
		clauses: append([]string{}, q.sqlerWhere.clauses...),
		params:  append([]any{}, q.sqlerWhere.params...),
		config:  q.sqlerWhere.config,
		err:     q.sqlerWhere.err,
	}

	// Copy the SqlerOrderBy
	newSqlerOrderBy := &SqlerOrderBy{
		clauses: append([]string{}, q.sqlerOrderBy.clauses...),
		config:  q.sqlerOrderBy.config,
		err:     q.sqlerOrderBy.err,
	}

	newQuery := &DeleteQueryBuilder{
		client:       q.client,
		table:        q.table,
		sqlerWhere:   newSqlerWhere,
		sqlerOrderBy: newSqlerOrderBy,
		config:       q.config,
		err:          q.err,
	}

	if q.limitValue != nil {
		val := *q.limitValue
		newQuery.limitValue = &val
	}

	return newQuery
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Exec() method.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := Delete("users").WithClient(client).Where("status = ?", "inactive")
//	result, err := query.Exec(ctx)
func (q *DeleteQueryBuilder) WithClient(client Querier) *DeleteQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.client = client

	return newQuery
}

// WithConfig sets a custom configuration for struct tags and placeholders.
// Returns a new query builder with the specified configuration.
//
// Example:
//
//	config := &QueryBuilderConfig{StructTag: "json", Placeholder: "$"}
//	query := Delete("users").WithConfig(config).Where(...)
func (q *DeleteQueryBuilder) WithConfig(config *QueryBuilderConfig) *DeleteQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.config = config
	newQuery.sqlerWhere.WithConfig(config)
	newQuery.sqlerOrderBy.WithConfig(config)

	return newQuery
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
//	Where("status = ?", "inactive").Where("age < ?", 18) // WHERE status = ? AND age < ?
//	Where(Eq{"status": "inactive", "role": "guest"}) // WHERE (`role` = ? AND `status` = ?)
func (q *DeleteQueryBuilder) Where(condition any, params ...any) *DeleteQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerWhere.Where(condition, params...)

	// Propagate any error from SqlerWhere to the query builder
	if newQuery.sqlerWhere.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerWhere.err
	}

	return newQuery
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
//	OrderBy(Col("priority").Desc())                 // ORDER BY `priority` DESC
func (q *DeleteQueryBuilder) OrderBy(cols ...any) *DeleteQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerOrderBy.OrderBy(cols...)

	// Propagate any error from SqlerOrderBy to the query builder
	if newQuery.sqlerOrderBy.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerOrderBy.err
	}

	return newQuery
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
func (q *DeleteQueryBuilder) Limit(limit int) *DeleteQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.limitValue = &limit

	return newQuery
}

// ToSql generates the final SQL DELETE query string with positional parameters.
// Returns the SQL string, parameters slice, and any error encountered during building.
//
// Example:
//
//	sql, args, err := Delete("users").
//		Where("status = ?", "inactive").
//		ToSql()
//	// sql: "DELETE FROM `users` WHERE status = ?"
//	// args: []any{"inactive"}
//
// Example with ORDER BY and LIMIT:
//
//	sql, args, err := Delete("logs").
//		Where("level = ?", "debug").
//		OrderBy("created_at ASC").
//		Limit(1000).
//		ToSql()
//	// sql: "DELETE FROM `logs` WHERE level = ? ORDER BY `created_at` ASC LIMIT ?"
//	// args: []any{"debug", 1000}
func (q *DeleteQueryBuilder) ToSql() (query string, params []any, err error) {
	// Check if the query has any errors from previous operations
	if q.err != nil {
		return "", nil, q.err
	}

	if q.table == "" {
		return "", nil, errors.New("table name is required")
	}

	return q.buildDeleteSql()
}

// buildDeleteSql builds the final DELETE SQL query string with positional parameters.
func (q *DeleteQueryBuilder) buildDeleteSql() (query string, params []any, err error) {
	params = []any{}
	paramIndex := 0 // Track parameter index for numbered placeholders (0-based)

	var sql strings.Builder

	// DELETE FROM clause
	sql.WriteString("DELETE FROM ")
	sql.WriteString(quoteIdentifier(q.table, q.config.IdentifierQuote))

	// WHERE clause
	var whereSQL string
	var whereArgs []any
	if whereSQL, whereArgs, err = q.sqlerWhere.toSqlWithStartIndex(paramIndex); err != nil {
		return "", nil, fmt.Errorf("could not build WHERE clause: %w", err)
	}
	if whereSQL != "" {
		sql.WriteString(" WHERE ")
		sql.WriteString(whereSQL)
		params = append(params, whereArgs...)
		paramIndex += len(whereArgs)
	}

	// ORDER BY clause
	var orderSQL string
	if orderSQL, err = q.sqlerOrderBy.ToSql(); err != nil {
		return "", nil, fmt.Errorf("could not build ORDER BY clause: %w", err)
	}
	if orderSQL != "" {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(orderSQL)
	}

	// LIMIT clause
	if q.limitValue != nil {
		sql.WriteString(fmt.Sprintf(" LIMIT %s", q.config.PlaceholderFormat(paramIndex)))
		params = append(params, *q.limitValue)
	}

	return sql.String(), params, nil
}

// Exec executes the delete query using the attached client.
// Returns the result (with RowsAffected) and any error.
// Requires that a client has been set via WithClient().
//
// Example with positional parameters:
//
//	result, err := Delete("users").
//		WithClient(client).
//		Where("status = ?", "inactive").
//		Exec(ctx)
//
// Example with Expression:
//
//	result, err := Delete("logs").
//		WithClient(client).
//		Where(Col("created_at").Lt("2020-01-01")).
//		Exec(ctx)
//
// Example with ORDER BY and LIMIT:
//
//	result, err := Delete("logs").
//		WithClient(client).
//		Where("level = ?", "debug").
//		OrderBy("created_at ASC").
//		Limit(1000).
//		Exec(ctx)
func (q *DeleteQueryBuilder) Exec(ctx context.Context) (Result, error) {
	var (
		sql  string
		args []any
		err  error
	)

	if q.client == nil {
		return nil, errors.New("no client set for query execution")
	}

	if sql, args, err = q.ToSql(); err != nil {
		return nil, fmt.Errorf("could not build sql for execution: %w", err)
	}

	return q.client.Exec(ctx, sql, args...)
}
