package sqlc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/justtrackio/gosoline/pkg/refl"
)

// UpdateQueryBuilder provides a fluent API for building SQL UPDATE queries.
// It implements an immutable builder pattern - each method returns a new instance
// rather than modifying the receiver. This allows for query reuse and prevents
// accidental mutations.
//
// All UPDATE queries use positional parameters (?). Values from structs and maps
// are extracted during SQL generation.
//
// Example with positional parameters:
//
//	query := Update("users").
//		Set("name", "John").
//		Set("email", "john@example.com").
//		Where("id = ?", 1)
//	result, err := query.Exec(ctx)
//
// Example with struct:
//
//	user := User{Name: "John", Email: "john@example.com"}
//	result, err := Update("users").SetRecord(user).Where("id = ?", 1).Exec(ctx)
//
// Example with map:
//
//	updates := map[string]any{"name": "John", "email": "john@example.com"}
//	result, err := Update("users").SetMap(updates).Where("id = ?", 1).Exec(ctx)
type UpdateQueryBuilder struct {
	client       Querier
	table        string
	sets         []Assignment
	record       any            // Store record for value extraction
	setMap       map[string]any // Store map for value extraction
	sqlerWhere   *SqlerWhere
	sqlerOrderBy *SqlerOrderBy
	limitValue   *int
	config       *Config // Configuration for struct tags and placeholders
	err          error
}

// Update creates a new UpdateQueryBuilder for the specified table.
// This is the entry point for building UPDATE queries.
//
// Example:
//
//	Update("users")                   // UPDATE `users`
//	Update("orders")                  // UPDATE `orders`
func Update(table string) *UpdateQueryBuilder {
	return &UpdateQueryBuilder{
		table:        table,
		sets:         []Assignment{},
		sqlerWhere:   NewSqlerWhere(),
		sqlerOrderBy: NewSqlerOrderBy(),
		config:       DefaultConfig(),
	}
}

// copyQuery creates a shallow copy of the query builder.
// This is used internally to implement the immutable builder pattern.
// Each builder method creates a copy, modifies it, and returns the new copy.
func (q *UpdateQueryBuilder) copyQuery() *UpdateQueryBuilder {
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
		err:     q.sqlerOrderBy.err,
	}

	// Copy setMap if present
	var newSetMap map[string]any
	if q.setMap != nil {
		newSetMap = make(map[string]any, len(q.setMap))
		for k, v := range q.setMap {
			newSetMap[k] = v
		}
	}

	newQuery := &UpdateQueryBuilder{
		client:       q.client,
		table:        q.table,
		sets:         append([]Assignment{}, q.sets...),
		record:       q.record,
		setMap:       newSetMap,
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
//	query := Update("users").WithClient(client).Set("name", "John")
//	result, err := query.Exec(ctx)
func (q *UpdateQueryBuilder) WithClient(client Querier) *UpdateQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.client = client

	return newQuery
}

// WithConfig sets a custom configuration for struct tags and placeholders.
// Returns a new query builder with the specified configuration.
//
// Example:
//
//	config := &Config{StructTag: "json", Placeholder: "$"}
//	query := Update("users").WithConfig(config).Set(...)
func (q *UpdateQueryBuilder) WithConfig(config *Config) *UpdateQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.config = config
	newQuery.sqlerWhere.WithConfig(config)

	return newQuery
}

// Set adds a single column assignment to the update query.
// The value will be parameterized in the generated SQL.
// Multiple calls to Set will add multiple assignments.
// Returns a new query builder with the assignment added.
//
// Example:
//
//	Update("users").
//		Set("name", "John").
//		Set("email", "john@example.com")
//	// UPDATE `users` SET `name` = ?, `email` = ?
func (q *UpdateQueryBuilder) Set(column string, value any) *UpdateQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sets = append(newQuery.sets, Assign(column, value))

	return newQuery
}

// SetExpr adds a single column assignment using a raw SQL expression.
// The expression will be inserted directly into the SQL without parameterization.
// Multiple calls to SetExpr will add multiple assignments.
// Returns a new query builder with the assignment added.
//
// Example:
//
//	Update("users").
//		SetExpr("count", "count + 1").
//		SetExpr("updated_at", "NOW()")
//	// UPDATE `users` SET `count` = count + 1, `updated_at` = NOW()
func (q *UpdateQueryBuilder) SetExpr(column string, expression string) *UpdateQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sets = append(newQuery.sets, AssignExpr(column, expression))

	return newQuery
}

// SetMap adds multiple column assignments from a map where keys are column names
// and values are the data to set.
// Values are extracted during SQL generation and use positional parameters.
// Returns a new query builder with the map data added.
//
// Example:
//
//	Update("users").SetMap(
//		map[string]any{"name": "John", "email": "john@example.com"},
//	)
//	// UPDATE `users` SET `email` = ?, `name` = ?
func (q *UpdateQueryBuilder) SetMap(m map[string]any) *UpdateQueryBuilder {
	newQuery := q.copyQuery()

	if len(m) == 0 {
		newQuery.err = errors.New("SetMap expects a non-empty map")

		return newQuery
	}

	// Store map for later value extraction
	newQuery.setMap = make(map[string]any, len(m))
	for k, v := range m {
		newQuery.setMap[k] = v
	}

	return newQuery
}

// SetRecord adds column assignments from a struct record.
// The struct must have `db` tags to identify column mappings.
// Values are extracted during SQL generation and use positional parameters.
// Returns a new query builder with the record added.
//
// Example:
//
//	type User struct {
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//	user := User{Name: "John", Email: "john@example.com"}
//	Update("users").SetRecord(user).Where("id = ?", 1)
//	// UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?
func (q *UpdateQueryBuilder) SetRecord(record any) *UpdateQueryBuilder {
	newQuery := q.copyQuery()

	// Get tags from record
	tags := refl.GetTags(record, q.config.StructTag)
	if len(tags) == 0 {
		newQuery.err = errors.New("record has no db tags")

		return newQuery
	}

	// Store record for value extraction
	newQuery.record = record

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
//	Where("status = ?", "active")                    // WHERE status = ?
//	Where(Col("age").Gt(18))                         // WHERE `age` > ?
//	Where(And(Col("a").Eq(1), Col("b").Eq(2)))       // WHERE (`a` = ? AND `b` = ?)
//	Where("status = ?", "active").Where("age > ?", 18) // WHERE status = ? AND age > ?
//	Where(Eq{"status": "active", "role": "admin"})   // WHERE (`role` = ? AND `status` = ?)
func (q *UpdateQueryBuilder) Where(condition any, params ...any) *UpdateQueryBuilder {
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
// Note: ORDER BY in UPDATE queries is typically used with LIMIT to control
// which rows are updated when you want to update only a subset of matching rows.
//
// Example:
//
//	OrderBy("created_at DESC")                      // ORDER BY `created_at` DESC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
//	OrderBy(Col("price").Desc())                    // ORDER BY `price` DESC
func (q *UpdateQueryBuilder) OrderBy(cols ...any) *UpdateQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerOrderBy.OrderBy(cols...)

	// Propagate any error from SqlerOrderBy to the query builder
	if newQuery.sqlerOrderBy.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerOrderBy.err
	}

	return newQuery
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
func (q *UpdateQueryBuilder) Limit(limit int) *UpdateQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.limitValue = &limit

	return newQuery
}

// ToSql generates the final SQL UPDATE query string with positional parameters.
// Returns the SQL string, parameters slice, and any error encountered during building.
//
// Example with Set:
//
//	sql, args, err := Update("users").
//		Set("name", "John").
//		Set("email", "john@example.com").
//		Where("id = ?", 1).
//		ToSql()
//	// sql: "UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?"
//	// args: []any{"John", "john@example.com", 1}
//
// Example with SetMap:
//
//	sql, args, err := Update("users").
//		SetMap(map[string]any{"name": "John", "email": "john@example.com"}).
//		Where("id = ?", 1).
//		ToSql()
//	// sql: "UPDATE `users` SET `email` = ?, `name` = ? WHERE id = ?"
//	// args: []any{"john@example.com", "John", 1}
//
// Example with SetRecord:
//
//	user := User{Name: "John", Email: "john@example.com"}
//	sql, args, err := Update("users").
//		SetRecord(user).
//		Where("id = ?", 1).
//		ToSql()
//	// sql: "UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?"
//	// args: []any{"John", "john@example.com", 1}
func (q *UpdateQueryBuilder) ToSql() (query string, params []any, err error) {
	// Check if the query has any errors from previous operations
	if q.err != nil {
		return "", nil, q.err
	}

	if q.table == "" {
		return "", nil, errors.New("table name is required")
	}

	// Extract SET assignments
	var assignments []Assignment

	// Handle SetMap
	if q.setMap != nil {
		// Sort keys for consistent order
		keys := make([]string, 0, len(q.setMap))
		for k := range q.setMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			assignments = append(assignments, Assign(key, q.setMap[key]))
		}
	}

	// Handle SetRecord
	if q.record != nil {
		tags := refl.GetTags(q.record, q.config.StructTag)
		values, err := extractValuesFromStruct(q.record, tags, q.config.StructTag)
		if err != nil {
			return "", nil, fmt.Errorf("could not extract values from record: %w", err)
		}

		for i, tag := range tags {
			assignments = append(assignments, Assign(tag, values[i]))
		}
	}

	// Handle explicit Set calls
	assignments = append(assignments, q.sets...)

	if len(assignments) == 0 {
		return "", nil, errors.New("at least one SET assignment is required")
	}

	return q.buildUpdateSql(assignments)
}

// buildUpdateSql builds the final UPDATE SQL query string with positional parameters.
func (q *UpdateQueryBuilder) buildUpdateSql(assignments []Assignment) (query string, params []any, err error) {
	params = []any{}
	paramIndex := 0 // Track parameter index for numbered placeholders (0-based)

	var sql strings.Builder

	// UPDATE clause
	sql.WriteString("UPDATE ")
	sql.WriteString(quoteIdentifier(q.table))

	// SET clause
	sql.WriteString(" SET ")

	setClauses := make([]string, len(assignments))
	for i, assignment := range assignments {
		quotedCol := quoteIdentifier(assignment.Column)
		if assignment.IsExpr {
			// Expression - insert directly without parameterization
			setClauses[i] = fmt.Sprintf("%s = %v", quotedCol, assignment.Value)
		} else {
			// Use configured placeholder
			setClauses[i] = fmt.Sprintf("%s = %s", quotedCol, q.config.PlaceholderFormat(paramIndex))
			params = append(params, assignment.Value)
			paramIndex++
		}
	}

	sql.WriteString(strings.Join(setClauses, ", "))

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

// Exec executes the update query using the attached client.
// Values from structs (SetRecord) and maps (SetMap) are extracted and used
// with positional parameters.
// Returns the result (with RowsAffected) and any error.
// Requires that a client has been set via WithClient().
//
// Example with positional parameters:
//
//	result, err := Update("users").
//		WithClient(client).
//		Set("name", "John").
//		Where("id = ?", 1).
//		Exec(ctx)
//
// Example with struct:
//
//	user := User{Name: "John", Email: "john@example.com"}
//	result, err := Update("users").
//		WithClient(client).
//		SetRecord(user).
//		Where("id = ?", 1).
//		Exec(ctx)
//
// Example with map:
//
//	updates := map[string]any{"name": "John", "email": "john@example.com"}
//	result, err := Update("users").
//		WithClient(client).
//		SetMap(updates).
//		Where("id = ?", 1).
//		Exec(ctx)
func (q *UpdateQueryBuilder) Exec(ctx context.Context) (Result, error) {
	var (
		sql  string
		args []any
		err  error
	)

	if q.client == nil {
		return nil, errors.New("no client set for query execution")
	}

	// Always use ToSql to extract values and use positional parameters
	if sql, args, err = q.ToSql(); err != nil {
		return nil, fmt.Errorf("could not build sql for execution: %w", err)
	}

	return q.client.Exec(ctx, sql, args...)
}
