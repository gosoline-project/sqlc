package sqlc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/justtrackio/gosoline/pkg/mapx"
	"github.com/justtrackio/gosoline/pkg/refl"
)

// Assignment represents a column assignment for ON DUPLICATE KEY UPDATE clause.
// It can contain either a direct value or an expression (e.g., "VALUES(col)" or "col + 1").
type Assignment struct {
	Column string // Column name (will be quoted)
	Value  any    // Value or expression
	IsExpr bool   // If true, Value is treated as raw SQL expression (not parameterized)
}

// Assign creates an Assignment with a parameterized value.
// The value will be added to the query parameters.
//
// Example:
//
//	Assign("count", 10)  // `count` = ?  (with param 10)
func Assign(column string, value any) Assignment {
	return Assignment{
		Column: column,
		Value:  value,
		IsExpr: false,
	}
}

// AssignExpr creates an Assignment with a raw SQL expression.
// The expression will be inserted directly into the SQL without parameterization.
//
// Example:
//
//	AssignExpr("count", "count + 1")           // `count` = count + 1
//	AssignExpr("count", "VALUES(count)")       // `count` = VALUES(count)
//	AssignExpr("updated_at", "NOW()")          // `updated_at` = NOW()
func AssignExpr(column string, expression string) Assignment {
	return Assignment{
		Column: column,
		Value:  expression,
		IsExpr: true,
	}
}

// InsertQueryBuilder provides a fluent API for building SQL INSERT queries.
// It implements an immutable builder pattern - each method returns a new instance
// rather than modifying the receiver. This allows for query reuse and prevents
// accidental mutations.
//
// The builder supports two modes:
//   - Positional parameters (using Values/ValuesRows): for explicit value insertion
//   - Named parameters (using Records with single record): for struct-based insertion using NamedExec
//
// Example with positional parameters:
//
//	query := Into("users").
//		Columns("id", "name", "email").
//		Values(1, "John", "john@example.com")
//	result, err := query.Exec(ctx)
//
// Example with named parameters (single struct):
//
//	user := User{ID: 1, Name: "John", Email: "john@example.com"}
//	result, err := Into("users").Records(user).Exec(ctx)
//
// Example with multiple structs (bulk insert):
//
//	result, err := Into("users").Records(user1, user2, user3).Exec(ctx)
type InsertQueryBuilder struct {
	client      Querier
	table       string
	columns     []string
	rows        [][]any
	records     []any            // Store all records (for NamedExec or later extraction)
	maps        []map[string]any // Store maps for ValuesMaps
	useNamed    bool             // Whether to use named parameters
	mode        string           // "INSERT" or "REPLACE"
	ignore      bool             // Whether to use IGNORE modifier
	priority    string           // Priority modifier: "", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"
	onDuplicate []Assignment     // ON DUPLICATE KEY UPDATE assignments
	config      *Config          // Configuration for struct tags and placeholders
	err         error
}

// Into creates a new InsertQueryBuilder for the specified table.
// This is the entry point for building INSERT queries.
//
// Example:
//
//	Into("users")                   // INSERT INTO `users`
//	Into("orders")                  // INSERT INTO `orders`
func Into(table string) *InsertQueryBuilder {
	return &InsertQueryBuilder{
		table:   table,
		columns: []string{},
		rows:    [][]any{},
		mode:    "INSERT", // Default to INSERT mode
		config:  DefaultConfig(),
	}
}

// copyQuery creates a shallow copy of the query builder.
// This is used internally to implement the immutable builder pattern.
// Each builder method creates a copy, modifies it, and returns the new copy.
func (q *InsertQueryBuilder) copyQuery() *InsertQueryBuilder {
	newQuery := &InsertQueryBuilder{
		client:      q.client,
		table:       q.table,
		columns:     append([]string{}, q.columns...),
		records:     append([]any{}, q.records...),
		maps:        append([]map[string]any{}, q.maps...),
		useNamed:    q.useNamed,
		mode:        q.mode,
		ignore:      q.ignore,
		priority:    q.priority,
		onDuplicate: append([]Assignment{}, q.onDuplicate...),
		config:      q.config,
		err:         q.err,
	}

	// Deep copy rows
	newQuery.rows = make([][]any, len(q.rows))
	for i, row := range q.rows {
		newQuery.rows[i] = append([]any{}, row...)
	}

	return newQuery
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Exec() method.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := Into("users").WithClient(client).Values(...)
//	result, err := query.Exec(ctx)
func (q *InsertQueryBuilder) WithClient(client Querier) *InsertQueryBuilder {
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
//	query := Into("users").WithConfig(config).Records(user)
func (q *InsertQueryBuilder) WithConfig(config *Config) *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.config = config

	return newQuery
}

// Insert sets the query to use INSERT mode (default).
// Returns a new query builder configured for INSERT operations.
//
// Example:
//
//	Into("users").Insert().Values(...)  // INSERT INTO `users` ...
func (q *InsertQueryBuilder) Insert() *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.mode = "INSERT"

	return newQuery
}

// Replace sets the query to use REPLACE mode.
// REPLACE works like INSERT, except that if an old row in the table has the same value
// as a new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted before the new row is inserted.
// Returns a new query builder configured for REPLACE operations.
//
// Example:
//
//	Into("users").Replace().Values(...)  // REPLACE INTO `users` ...
func (q *InsertQueryBuilder) Replace() *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.mode = "REPLACE"

	return newQuery
}

// Ignore adds the IGNORE modifier to the insert query.
// With IGNORE, rows that would cause duplicate-key errors are silently skipped.
// Returns a new query builder with IGNORE modifier set.
//
// Example:
//
//	Into("users").Ignore().Values(...)  // INSERT IGNORE INTO `users` ...
func (q *InsertQueryBuilder) Ignore() *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.ignore = true

	return newQuery
}

// LowPriority adds the LOW_PRIORITY modifier to the insert query.
// LOW_PRIORITY delays execution until no other clients are reading from the table.
// Returns a new query builder with LOW_PRIORITY modifier set.
//
// Example:
//
//	Into("users").LowPriority().Values(...)  // INSERT LOW_PRIORITY INTO `users` ...
func (q *InsertQueryBuilder) LowPriority() *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.priority = "LOW_PRIORITY"

	return newQuery
}

// HighPriority adds the HIGH_PRIORITY modifier to the insert query.
// HIGH_PRIORITY overrides the effect of --low-priority-updates server option.
// Returns a new query builder with HIGH_PRIORITY modifier set.
//
// Example:
//
//	Into("users").HighPriority().Values(...)  // INSERT HIGH_PRIORITY INTO `users` ...
func (q *InsertQueryBuilder) HighPriority() *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.priority = "HIGH_PRIORITY"

	return newQuery
}

// Delayed adds the DELAYED modifier to the insert query.
// DELAYED causes the server to put the row(s) in a buffer and return immediately.
// Returns a new query builder with DELAYED modifier set.
//
// Example:
//
//	Into("users").Delayed().Values(...)  // INSERT DELAYED INTO `users` ...
func (q *InsertQueryBuilder) Delayed() *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.priority = "DELAYED"

	return newQuery
}

// OnDuplicateKeyUpdate adds an ON DUPLICATE KEY UPDATE clause to the insert query.
// This clause specifies what to do when a duplicate key error occurs.
// Accepts one or more Assignment values that specify column updates.
// Returns a new query builder with the ON DUPLICATE KEY UPDATE clause added.
//
// Example with value:
//
//	Into("users").
//		Columns("id", "name", "count").
//		Values(1, "John", 5).
//		OnDuplicateKeyUpdate(
//			sqlg.Assign("count", 10),  // Set count to specific value
//		)
//	// INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?)
//	// ON DUPLICATE KEY UPDATE `count` = ?
//
// Example with expression:
//
//	Into("users").
//		Columns("id", "name", "count").
//		Values(1, "John", 5).
//		OnDuplicateKeyUpdate(
//			sqlg.AssignExpr("count", "count + VALUES(count)"),  // Increment count
//		)
//	// INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?)
//	// ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)
func (q *InsertQueryBuilder) OnDuplicateKeyUpdate(assignments ...Assignment) *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.onDuplicate = append(newQuery.onDuplicate, assignments...)

	return newQuery
}

// Columns sets the column list for the insert query.
// This method replaces any previously set columns.
// Column names will be automatically quoted in the generated SQL.
// Returns a new query builder with the specified columns.
//
// Example:
//
//	Into("users").Columns("id", "name", "email")
//	// INSERT INTO `users` (`id`, `name`, `email`) VALUES (...)
func (q *InsertQueryBuilder) Columns(cols ...string) *InsertQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.columns = make([]string, len(cols))

	for i, col := range cols {
		newQuery.columns[i] = quoteIdentifier(col)
	}

	return newQuery
}

// Values adds a single row of values to the insert query.
// The number of values must match the number of columns.
// Multiple calls to Values will add multiple rows (bulk insert).
// Uses positional parameters (?) in the generated SQL.
// Returns a new query builder with the values added.
//
// Example:
//
//	Into("users").
//		Columns("id", "name", "email").
//		Values(1, "John", "john@example.com").
//		Values(2, "Jane", "jane@example.com")
//	// INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)
func (q *InsertQueryBuilder) Values(values ...any) *InsertQueryBuilder {
	newQuery := q.copyQuery()

	// Validate values count matches columns count if columns are set
	if len(newQuery.columns) > 0 && len(values) != len(newQuery.columns) {
		newQuery.err = fmt.Errorf("mismatched values count: expected %d values for %d columns, got %d", len(newQuery.columns), len(newQuery.columns), len(values))

		return newQuery
	}

	newQuery.rows = append(newQuery.rows, append([]any{}, values...))
	newQuery.useNamed = false

	return newQuery
}

// ValuesRows adds multiple rows of values to the insert query in a single call.
// Each row must have the same number of values, matching the number of columns.
// Uses positional parameters (?) in the generated SQL.
// Returns a new query builder with the rows added.
//
// Example:
//
//	Into("users").
//		Columns("id", "name").
//		ValuesRows(
//			[]any{1, "John"},
//			[]any{2, "Jane"},
//			[]any{3, "Bob"},
//		)
//	// INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?), (?, ?)
func (q *InsertQueryBuilder) ValuesRows(rows ...[]any) *InsertQueryBuilder {
	newQuery := q.copyQuery()

	for i, row := range rows {
		// Validate values count matches columns count if columns are set
		if len(newQuery.columns) > 0 && len(row) != len(newQuery.columns) {
			newQuery.err = fmt.Errorf("mismatched values count in row %d: expected %d values for %d columns, got %d", i, len(newQuery.columns), len(newQuery.columns), len(row))

			return newQuery
		}

		newQuery.rows = append(newQuery.rows, append([]any{}, row...))
	}

	newQuery.useNamed = false

	return newQuery
}

// ValuesMaps adds multiple rows using maps where keys are column names and values are the data.
// If columns have not been set, they will be inferred from the first map's keys (sorted alphabetically).
// All maps must contain the same keys.
// Uses named parameters (`:key`) and NamedExec for execution.
// Returns a new query builder with the map data added.
//
// Example with single map:
//
//	Into("users").ValuesMaps(
//		map[string]any{"id": 1, "name": "John", "email": "john@example.com"},
//	)
//	// Uses: INSERT INTO `users` (`email`, `id`, `name`) VALUES (:email, :id, :name)
//	// With NamedExec
//
// Example with multiple maps (batch insert):
//
//	Into("users").ValuesMaps(
//		map[string]any{"id": 1, "name": "John", "email": "john@example.com"},
//		map[string]any{"id": 2, "name": "Jane", "email": "jane@example.com"},
//	)
//	// Uses: INSERT INTO `users` (`email`, `id`, `name`) VALUES (:email, :id, :name)
//	// With NamedExec for batch insert
func (q *InsertQueryBuilder) ValuesMaps(maps ...map[string]any) *InsertQueryBuilder {
	newQuery := q.copyQuery()

	if len(maps) == 0 {
		newQuery.err = errors.New("ValuesMaps expects at least one map")

		return newQuery
	}

	// Infer columns from first map if not set
	if len(newQuery.columns) == 0 {
		keys := make([]string, 0, len(maps[0]))
		for key := range maps[0] {
			keys = append(keys, key)
		}
		// Sort keys alphabetically for consistent column order
		sort.Strings(keys)

		newQuery.columns = make([]string, len(keys))
		for i, key := range keys {
			newQuery.columns[i] = quoteIdentifier(key)
		}
	}

	// Validate all maps have the same keys as columns
	columnNames := make([]string, len(newQuery.columns))
	for i, col := range newQuery.columns {
		columnNames[i] = unquoteIdentifier(col)
	}

	for i, m := range maps {
		// Validate all required columns exist in map
		for _, colName := range columnNames {
			if _, ok := m[colName]; !ok {
				newQuery.err = fmt.Errorf("map %d missing required column: %s", i, colName)

				return newQuery
			}
		}

		// Validate no extra keys in map
		if len(m) != len(columnNames) {
			newQuery.err = fmt.Errorf("map %d has %d keys but %d columns are defined", i, len(m), len(columnNames))

			return newQuery
		}
	}

	// Store maps for later use with NamedExec
	newQuery.maps = append(newQuery.maps, maps...)
	newQuery.useNamed = true

	return newQuery
}

// Records adds one or more struct records to the insert query.
// Accepts variadic parameters - each parameter can be:
//   - A struct or pointer to struct with `db` tags
//   - A slice or array of structs/pointers with `db` tags
//
// Slice/array arguments are automatically flattened, so Records([]User{u1, u2}) is equivalent to Records(u1, u2).
// All structs must have the same structure and `db` tags.
// Records are stored and values are only extracted when ToSql() is called or Exec() passes them to NamedExec.
// If columns have not been explicitly set, they will be inferred from the first struct.
// Returns a new query builder with the records added.
//
// Example with single record:
//
//	user := User{ID: 1, Name: "John", Email: "john@example.com"}
//	Into("users").Records(user).Exec(ctx)
//	// Uses: INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)
//	// With NamedExec
//
// Example with multiple records (variadic):
//
//	user1 := User{ID: 1, Name: "John", Email: "john@example.com"}
//	user2 := User{ID: 2, Name: "Jane", Email: "jane@example.com"}
//	Into("users").Records(user1, user2).Exec(ctx)
//	// INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)
//
// Example with slice of records:
//
//	users := []User{
//		{ID: 1, Name: "John", Email: "john@example.com"},
//		{ID: 2, Name: "Jane", Email: "jane@example.com"},
//	}
//	Into("users").Records(users).Exec(ctx)
//	// INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)
//
// Example with slice of pointers:
//
//	users := []*User{&user1, &user2}
//	Into("users").Records(users).Exec(ctx)
func (q *InsertQueryBuilder) Records(records ...any) *InsertQueryBuilder {
	newQuery := q.copyQuery()
	flattened := refl.Flatten(records...)

	if len(flattened) == 0 {
		newQuery.err = errors.New("Records expects at least one record")

		return newQuery
	}

	// Process first element to get tags and set columns if needed
	firstElem := flattened[0]
	tags := refl.GetTags(firstElem, q.config.StructTag)
	if len(tags) == 0 {
		newQuery.err = errors.New("records have no db tags")

		return newQuery
	}

	// If columns not set, infer from tags
	if len(newQuery.columns) == 0 {
		newQuery.columns = make([]string, len(tags))
		for i, tag := range tags {
			newQuery.columns[i] = quoteIdentifier(tag)
		}
	}

	// Store flattened records without extracting values
	newQuery.records = append(newQuery.records, flattened...)
	// NamedExec supports both single and batch inserts with structs
	newQuery.useNamed = true

	return newQuery
}

// ToSql generates the final SQL INSERT query string with positional parameters.
// Returns the SQL string, parameters slice, and any error encountered during building.
// All inserts use positional parameters (? syntax) and values are extracted from structs.
//
// Example with value-based insert:
//
//	sql, args, err := Into("users").
//		Columns("id", "name", "email").
//		Values(1, "John", "john@example.com").
//		ToSql()
//	// sql: "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)"
//	// args: []any{1, "John", "john@example.com"}
//
// Example with single record:
//
//	sql, args, err := Into("users").Records(user).ToSql()
//	// sql: "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)"
//	// args: []any{1, "John", "john@example.com"}
//
// Example with multiple records:
//
//	sql, args, err := Into("users").Records(user1, user2).ToSql()
//	// sql: "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)"
//	// args: []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com"}
//
// Example with single map:
//
//	sql, args, err := Into("users").ValuesMaps(map[string]any{"id": 1, "name": "John"}).ToSql()
//	// sql: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)"
//	// args: []any{1, "John"}
//
// Example with multiple maps:
//
//	sql, args, err := Into("users").ValuesMaps(map1, map2).ToSql()
//	// sql: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?)"
//	// args: []any{1, "John", 2, "Jane"}
func (q *InsertQueryBuilder) ToSql() (query string, params []any, err error) {
	// Check if the query has any errors from previous operations
	if q.err != nil {
		return "", nil, q.err
	}

	if q.table == "" {
		return "", nil, errors.New("table name is required")
	}

	if len(q.columns) == 0 {
		return "", nil, errors.New("columns are required")
	}

	// Extract values from maps if needed
	if len(q.maps) > 0 {
		if err := q.extractValuesFromMaps(); err != nil {
			return "", nil, err
		}
	}

	// Extract values from records if needed
	if len(q.records) > 0 {
		if err := q.extractValuesFromRecords(); err != nil {
			return "", nil, err
		}
	}

	// For positional parameters, validate rows
	if len(q.rows) == 0 {
		return "", nil, errors.New("at least one row of values is required")
	}

	// Validate all rows have same number of values as columns
	for i, row := range q.rows {
		if len(row) != len(q.columns) {
			return "", nil, fmt.Errorf("row %d has %d values but %d columns are defined", i, len(row), len(q.columns))
		}
	}

	return q.buildInsertSql()
}

// ToNamedSql generates SQL with named parameters for use with NamedExec.
// For Records: The column names from the `db` tags are used as the named parameter names.
// For ValuesMaps: The map keys are used as the named parameter names.
// Returns the SQL string and the untouched records/maps (single item or slice).
// The returned params are what should be passed to NamedExec.
//
// Example with single record:
//
//	sql, params, err := Into("users").Records(user).ToNamedSql()
//	// sql: "INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)"
//	// params: []any{user} // the original struct
//	// Usage: client.NamedExec(ctx, sql, params[0])
//
// Example with multiple records:
//
//	sql, params, err := Into("users").Records(user1, user2).ToNamedSql()
//	// sql: "INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)"
//	// params: []any{user1, user2} // the original structs
//	// Usage: client.NamedExec(ctx, sql, params)
//
// Example with single map:
//
//	sql, params, err := Into("users").ValuesMaps(map[string]any{"id": 1, "name": "John"}).ToNamedSql()
//	// sql: "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)"
//	// params: []any{map[string]any{"id": 1, "name": "John"}} // the original map
//	// Usage: client.NamedExec(ctx, sql, params[0])
//
// Example with multiple maps:
//
//	sql, params, err := Into("users").ValuesMaps(map1, map2).ToNamedSql()
//	// sql: "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)"
//	// params: []any{map1, map2} // the original maps
//	// Usage: client.NamedExec(ctx, sql, params)
func (q *InsertQueryBuilder) ToNamedSql() (query string, params []any, err error) {
	// Check if the query has any errors from previous operations
	if q.err != nil {
		return "", nil, q.err
	}

	var sql strings.Builder

	// Build INSERT prefix with modifiers
	prefix, err := q.buildInsertPrefix()
	if err != nil {
		return "", nil, err
	}
	sql.WriteString(prefix)

	// Columns clause
	sql.WriteString(" (")
	sql.WriteString(strings.Join(q.columns, ", "))
	sql.WriteString(")")

	// VALUES clause with named parameters (single VALUES clause for NamedExec)
	sql.WriteString(" VALUES (")

	// Handle maps
	if len(q.maps) > 0 {
		return q.buildNamedSqlForMaps(&sql)
	}

	// Handle records
	if len(q.records) > 0 {
		return q.buildNamedSqlForRecords(&sql)
	}

	return "", nil, errors.New("no records or maps to insert")
}

// buildNamedSqlForMaps builds the named SQL for map-based inserts.
func (q *InsertQueryBuilder) buildNamedSqlForMaps(sql *strings.Builder) (query string, params []any, err error) {
	// Get the unquoted column names for named parameters
	columnNames := make([]string, len(q.columns))
	for i, col := range q.columns {
		columnNames[i] = unquoteIdentifier(col)
	}

	namedParams := make([]string, len(columnNames))
	for i, colName := range columnNames {
		namedParams[i] = ":" + colName
	}

	sql.WriteString(strings.Join(namedParams, ", "))
	sql.WriteString(")")

	// ON DUPLICATE KEY UPDATE clause for named params
	if len(q.onDuplicate) > 0 {
		duplicateClause, err := q.buildOnDuplicateClauseNamed()
		if err != nil {
			return "", nil, err
		}
		sql.WriteString(duplicateClause)
	}

	// Convert maps to []any for return
	params = make([]any, len(q.maps))
	for i, m := range q.maps {
		params[i] = m
	}

	return sql.String(), params, nil
}

// buildNamedSqlForRecords builds the named SQL for record-based inserts.
func (q *InsertQueryBuilder) buildNamedSqlForRecords(sql *strings.Builder) (query string, params []any, err error) {
	// Get the unquoted column names for named parameters
	tags := refl.GetTags(q.records[0], q.config.StructTag)
	namedParams := make([]string, len(tags))
	for i, tag := range tags {
		namedParams[i] = ":" + tag
	}

	sql.WriteString(strings.Join(namedParams, ", "))
	sql.WriteString(")")

	// ON DUPLICATE KEY UPDATE clause for named params
	if len(q.onDuplicate) > 0 {
		duplicateClause, err := q.buildOnDuplicateClauseNamed()
		if err != nil {
			return "", nil, err
		}
		sql.WriteString(duplicateClause)
	}

	// Return the untouched records (what NamedExec expects)
	return sql.String(), q.records, nil
}

// buildOnDuplicateClauseNamed builds the ON DUPLICATE KEY UPDATE clause for named parameters.
// For named parameters, expressions remain as-is, and values use named placeholders.
func (q *InsertQueryBuilder) buildOnDuplicateClauseNamed() (string, error) {
	if len(q.onDuplicate) == 0 {
		return "", nil
	}

	var parts []string

	for _, assignment := range q.onDuplicate {
		quotedCol := quoteIdentifier(assignment.Column)
		if assignment.IsExpr {
			// Expression - insert directly without parameterization
			parts = append(parts, fmt.Sprintf("%s = %v", quotedCol, assignment.Value))
		} else {
			// Value - use named placeholder (same as column name)
			colName := unquoteIdentifier(assignment.Column)
			parts = append(parts, fmt.Sprintf("%s = :%s", quotedCol, colName))
		}
	}

	clause := " ON DUPLICATE KEY UPDATE " + strings.Join(parts, ", ")

	return clause, nil
}

// Exec executes the insert query using the attached client.
// For struct-based inserts (Records) or map-based inserts (ValuesMaps), this uses NamedExec with named parameters.
// For value-based inserts (Values/ValuesRows), this uses Exec with positional parameters.
// Returns the result (with LastInsertId and RowsAffected) and any error.
// Requires that a client has been set via WithClient().
//
// Example with positional parameters:
//
//	result, err := Into("users").
//		WithClient(client).
//		Columns("id", "name").
//		Values(1, "John").
//		Exec(ctx)
//
// Example with named parameters (single struct):
//
//	result, err := Into("users").
//		WithClient(client).
//		Records(user).
//		Exec(ctx)
//
// Example with named parameters (batch structs):
//
//	result, err := Into("users").
//		WithClient(client).
//		Records(user1, user2, user3).
//		Exec(ctx)
//
// Example with named parameters (single map):
//
//	result, err := Into("users").
//		WithClient(client).
//		ValuesMaps(map[string]any{"id": 1, "name": "John"}).
//		Exec(ctx)
//
// Example with named parameters (batch maps):
//
//	result, err := Into("users").
//		WithClient(client).
//		ValuesMaps(map1, map2, map3).
//		Exec(ctx)
func (q *InsertQueryBuilder) Exec(ctx context.Context) (Result, error) {
	var (
		sql     string
		records []any
		args    []any
		err     error
	)

	if q.client == nil {
		return nil, errors.New("no client set for query execution")
	}

	// For record-based or map-based inserts, use NamedExec (supports both single and batch)
	if q.useNamed && (len(q.records) > 0 || len(q.maps) > 0) {
		if sql, records, err = q.ToNamedSql(); err != nil {
			return nil, fmt.Errorf("could not build sql for execution: %w", err)
		}

		// NamedExec accepts both single item and slice of items
		if len(records) == 1 {
			return q.client.NamedExec(ctx, sql, records[0])
		}

		return q.client.NamedExec(ctx, sql, records)
	}

	// For value-based inserts, use ToSql to extract values and use Exec
	if sql, args, err = q.ToSql(); err != nil {
		return nil, fmt.Errorf("could not build sql for execution: %w", err)
	}

	return q.client.Exec(ctx, sql, args...)
}

// extractValuesFromStruct extracts field values from a struct in the order specified by tags.
// It handles both struct values and pointers to structs.
// Uses mapx.Struct.Read() for simplified struct field extraction.
func extractValuesFromStruct(record any, tags []string, structTag string) (values []any, err error) {
	var (
		structReader *mapx.Struct
		fieldMap     *mapx.MapX
		value        any
		ok           bool
	)

	// Ensure we have a pointer for mapx.Struct
	rv := reflect.ValueOf(record)
	var ptr any

	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, errors.New("record pointer is nil")
		}
		ptr = record
	} else {
		// Create a pointer if we have a value
		ptrVal := reflect.New(rv.Type())
		ptrVal.Elem().Set(rv)
		ptr = ptrVal.Interface()
	}

	// Use mapx.Struct to read the struct fields
	structReader, err = mapx.NewStruct(ptr, &mapx.StructSettings{
		FieldTag: structTag,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create struct reader: %w", err)
	}

	if fieldMap, err = structReader.Read(); err != nil {
		return nil, fmt.Errorf("could not read struct fields: %w", err)
	}

	// Extract values in tag order
	values = make([]any, len(tags))
	msi := fieldMap.Msi()

	for i, tag := range tags {
		if value, ok = msi[tag]; !ok {
			return nil, fmt.Errorf("field with tag '%s' not found in struct", tag)
		}
		values[i] = value
	}

	return values, nil
}

// unquoteIdentifier removes backtick quotes from an identifier.
// For simple identifiers like `id` or `name`, returns the unquoted name.
// For table-qualified identifiers like `users`.`id`, returns just the column name without table prefix.
func unquoteIdentifier(identifier string) string {
	// Remove all backticks
	unquoted := strings.ReplaceAll(identifier, "`", "")

	// If it's table-qualified (contains .), return just the column name
	if idx := strings.LastIndex(unquoted, "."); idx != -1 {
		return unquoted[idx+1:]
	}

	return unquoted
}

// extractValuesFromMaps extracts values from all maps in column order and appends them to q.rows.
func (q *InsertQueryBuilder) extractValuesFromMaps() error {
	// Get the unquoted column names for map key lookup
	columnNames := make([]string, len(q.columns))
	for i, col := range q.columns {
		columnNames[i] = unquoteIdentifier(col)
	}

	// Extract values from all maps in column order
	for _, m := range q.maps {
		row := make([]any, len(columnNames))
		for j, colName := range columnNames {
			row[j] = m[colName]
		}

		q.rows = append(q.rows, row)
	}

	return nil
}

// extractValuesFromRecords extracts values from all records and appends them to q.rows.
func (q *InsertQueryBuilder) extractValuesFromRecords() error {
	// Get tags from first record
	tags := refl.GetTags(q.records[0], q.config.StructTag)

	// Extract values from all records
	for i, record := range q.records {
		values, err := extractValuesFromStruct(record, tags, q.config.StructTag)
		if err != nil {
			return fmt.Errorf("could not extract values from record %d: %w", i, err)
		}

		// Validate values count
		if len(values) != len(q.columns) {
			return fmt.Errorf("mismatched values count in record %d: expected %d values for %d columns, got %d", i, len(q.columns), len(q.columns), len(values))
		}

		q.rows = append(q.rows, values)
	}

	return nil
}

// buildInsertSql builds the final INSERT SQL query string with positional parameters.
func (q *InsertQueryBuilder) buildInsertSql() (query string, params []any, err error) {
	params = []any{}
	paramIndex := 0 // Track parameter index for numbered placeholders (0-based)

	var sql strings.Builder

	// Build INSERT prefix with modifiers
	prefix, err := q.buildInsertPrefix()
	if err != nil {
		return "", nil, err
	}
	sql.WriteString(prefix)

	// Columns clause
	sql.WriteString(" (")
	sql.WriteString(strings.Join(q.columns, ", "))
	sql.WriteString(")")

	// VALUES clause
	sql.WriteString(" VALUES ")

	// Build placeholders for each row
	valueClauses := make([]string, len(q.rows))
	for rowIdx, row := range q.rows {
		placeholders := make([]string, len(q.columns))
		for i := range placeholders {
			placeholders[i] = q.config.PlaceholderFormat(paramIndex)
			paramIndex++
		}
		valueClauses[rowIdx] = "(" + strings.Join(placeholders, ", ") + ")"
		params = append(params, row...)
	}

	sql.WriteString(strings.Join(valueClauses, ", "))

	// ON DUPLICATE KEY UPDATE clause
	if len(q.onDuplicate) > 0 {
		duplicateClause, duplicateParams, err := q.buildOnDuplicateClause(paramIndex)
		if err != nil {
			return "", nil, err
		}
		sql.WriteString(duplicateClause)
		params = append(params, duplicateParams...)
	}

	return sql.String(), params, nil
}

// buildInsertPrefix builds the INSERT/REPLACE prefix with modifiers.
// Returns the prefix string like "INSERT", "INSERT IGNORE", "INSERT LOW_PRIORITY", etc.
func (q *InsertQueryBuilder) buildInsertPrefix() (string, error) {
	// Validate: REPLACE cannot be used with ON DUPLICATE KEY UPDATE
	if q.mode == "REPLACE" && len(q.onDuplicate) > 0 {
		return "", errors.New("REPLACE cannot be used with ON DUPLICATE KEY UPDATE")
	}

	var parts []string
	parts = append(parts, q.mode)

	// Add priority modifier if set
	if q.priority != "" {
		parts = append(parts, q.priority)
	}

	// Add IGNORE modifier if set
	if q.ignore {
		parts = append(parts, "IGNORE")
	}

	// Add INTO table
	parts = append(parts, "INTO")
	parts = append(parts, quoteIdentifier(q.table))

	return strings.Join(parts, " "), nil
}

// buildOnDuplicateClause builds the ON DUPLICATE KEY UPDATE clause.
// Returns the clause string and any additional parameters.
func (q *InsertQueryBuilder) buildOnDuplicateClause(paramIndex int) (clause string, params []any, err error) {
	if len(q.onDuplicate) == 0 {
		return "", nil, nil
	}

	var parts []string
	params = []any{}

	for _, assignment := range q.onDuplicate {
		quotedCol := quoteIdentifier(assignment.Column)
		if assignment.IsExpr {
			// Expression - insert directly without parameterization
			parts = append(parts, fmt.Sprintf("%s = %v", quotedCol, assignment.Value))
		} else {
			// Value - use placeholder
			parts = append(parts, fmt.Sprintf("%s = %s", quotedCol, q.config.PlaceholderFormat(paramIndex)))
			params = append(params, assignment.Value)
			paramIndex++
		}
	}

	clause = " ON DUPLICATE KEY UPDATE " + strings.Join(parts, ", ")

	return clause, params, nil
}
