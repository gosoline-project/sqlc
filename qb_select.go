package sqlg

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/justtrackio/gosoline/pkg/refl"
)

// SelectQueryBuilder provides a fluent API for building SQL SELECT queries.
// It implements an immutable builder pattern - each method returns a new instance
// rather than modifying the receiver. This allows for query reuse and prevents
// accidental mutations.
//
// Example usage:
//
//	query := From("users").
//		Columns("id", "name", "email").
//		Where("status = ?", "active").
//		OrderBy("created_at DESC").
//		Limit(10)
//	sql, args, err := query.ToSql()
type SelectQueryBuilder struct {
	client         Client
	table          string
	tableAlias     string
	projections    []string
	distinct       bool
	whereClauses   []string
	whereParams    []any
	groupByCols    []string
	havingClauses  []string
	havingParams   []any
	orderByClauses []string
	limitValue     *int
	offsetValue    *int
	err            error
}

// From creates a new SelectQueryBuilder for the specified table.
// This is the entry point for building SELECT queries.
//
// Example:
//
//	From("users")                   // SELECT * FROM `users`
//	From("orders").As("o")          // SELECT * FROM `orders` AS o
func From(table string) *SelectQueryBuilder {
	return &SelectQueryBuilder{
		table:          table,
		projections:    []string{},
		whereClauses:   []string{},
		whereParams:    []any{},
		groupByCols:    []string{},
		havingClauses:  []string{},
		havingParams:   []any{},
		orderByClauses: []string{},
	}
}

// copyQuery creates a shallow copy of the query builder.
// This is used internally to implement the immutable builder pattern.
// Each builder method creates a copy, modifies it, and returns the new copy.
func (q *SelectQueryBuilder) copyQuery() *SelectQueryBuilder {
	newQuery := &SelectQueryBuilder{
		client:         q.client,
		table:          q.table,
		tableAlias:     q.tableAlias,
		projections:    append([]string{}, q.projections...),
		distinct:       q.distinct,
		whereClauses:   append([]string{}, q.whereClauses...),
		whereParams:    append([]any{}, q.whereParams...),
		groupByCols:    append([]string{}, q.groupByCols...),
		havingClauses:  append([]string{}, q.havingClauses...),
		havingParams:   append([]any{}, q.havingParams...),
		orderByClauses: append([]string{}, q.orderByClauses...),
		err:            q.err,
	}
	if q.limitValue != nil {
		val := *q.limitValue
		newQuery.limitValue = &val
	}
	if q.offsetValue != nil {
		val := *q.offsetValue
		newQuery.offsetValue = &val
	}

	return newQuery
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Select() or Get() methods.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := From("users").WithClient(client).Limit(10)
//	err := query.Select(ctx, &users)
func (q *SelectQueryBuilder) WithClient(client Client) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.client = client

	return newQuery
}

// As sets an alias for the table in the FROM clause.
// Returns a new query builder with the table alias set.
//
// Example:
//
//	From("users").As("u")           // FROM `users` AS u
//	From("order_items").As("oi")    // FROM `order_items` AS oi
func (q *SelectQueryBuilder) As(alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.tableAlias = alias

	return newQuery
}

// Columns replaces the current column list with the specified columns.
// Accepts strings (column names) or *Expression objects for more complex selections.
// Returns a new query builder with the updated column list.
//
// Example:
//
//	Columns("id", "name", "email")              // SELECT `id`, `name`, `email`
//	Columns(Col("id"), Col("name").As("user_name")) // SELECT `id`, `name` AS user_name
//	Columns(Col("id").Count().As("total"))      // SELECT COUNT(`id`) AS total
func (q *SelectQueryBuilder) Columns(cols ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.projections = []string{}

	for i, col := range cols {
		switch v := col.(type) {
		case string:
			newQuery.projections = append(newQuery.projections, quoteIdentifier(v))
		case *Expression:
			newQuery.projections = append(newQuery.projections, v.toSQL())
		default:
			newQuery.err = fmt.Errorf("invalid type for Columns argument %d: expected string or *Expression, got %T", i, col)

			return newQuery
		}
	}

	return newQuery
}

// Column appends a single column to the existing projection list.
// Unlike Columns(), this adds to the list rather than replacing it.
// Accepts a string (column name) or *Expression object.
// Returns a new query builder with the column added.
//
// Example:
//
//	From("users").
//		Column("id").
//		Column("name").
//		Column(Col("email").As("contact"))  // SELECT `id`, `name`, `email` AS contact
func (q *SelectQueryBuilder) Column(col any) *SelectQueryBuilder {
	newQuery := q.copyQuery()

	switch v := col.(type) {
	case string:
		newQuery.projections = append(newQuery.projections, quoteIdentifier(v))
	case *Expression:
		newQuery.projections = append(newQuery.projections, v.toSQL())
	default:
		newQuery.err = fmt.Errorf("invalid type for Column argument: expected string or *Expression, got %T", col)

		return newQuery
	}

	return newQuery
}

// ForType automatically sets the column list based on struct field tags.
// It uses the `db` struct tag to determine which columns to select.
// Returns a new query builder with columns set based on the struct type.
//
// Example:
//
//	type User struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//	From("users").ForType(&User{})  // SELECT `id`, `name`, `email`
func (q *SelectQueryBuilder) ForType(t any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	tags := refl.GetTags(t, dbStructTag)

	var err error
	var cols []any

	if cols, err = refl.InterfaceToInterfaceSlice(tags); err != nil {
		newQuery.err = fmt.Errorf("could not convert tags to slice of interface: %w", err)

		return newQuery
	}

	return newQuery.Columns(cols...)
}

// Distinct adds the DISTINCT keyword to the SELECT clause.
// Returns a new query builder with DISTINCT enabled.
//
// Example:
//
//	From("orders").Distinct().Column("customer_id")  // SELECT DISTINCT `customer_id`
func (q *SelectQueryBuilder) Distinct() *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.distinct = true

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
func (q *SelectQueryBuilder) Where(condition any, params ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()

	switch v := condition.(type) {
	case string:
		newQuery.whereClauses = append(newQuery.whereClauses, v)
		newQuery.whereParams = append(newQuery.whereParams, params...)
	case *Expression:
		// Skip nil expressions (e.g., from Eq() with empty map)
		if v == nil {
			return newQuery
		}
		newQuery.whereClauses = append(newQuery.whereClauses, v.toConditionSQL())
		newQuery.whereParams = append(newQuery.whereParams, v.collectParameters()...)
	case Eq:
		// Handle Eq map type - convert to expressions
		if len(v) == 0 {
			return newQuery // Empty map is a no-op
		}

		// Sort keys for deterministic SQL generation
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Create equality expressions for each key-value pair
		expressions := make([]*Expression, len(keys))
		for i, key := range keys {
			expressions[i] = Col(key).Eq(v[key])
		}

		// Single condition doesn't need AND wrapping
		var expr *Expression
		if len(expressions) == 1 {
			expr = expressions[0]
		} else {
			expr = And(expressions...)
		}

		newQuery.whereClauses = append(newQuery.whereClauses, expr.toConditionSQL())
		newQuery.whereParams = append(newQuery.whereParams, expr.collectParameters()...)
	default:
		newQuery.err = fmt.Errorf("invalid type for Where condition: expected string or *Expression, got %T", condition)

		return newQuery
	}

	return newQuery
}

// GroupBy sets the GROUP BY columns for the query.
// Accepts strings (column names) or *Expression objects.
// Replaces any previously set GROUP BY clause.
// Returns a new query builder with the GROUP BY clause set.
//
// Example:
//
//	GroupBy("status")                           // GROUP BY `status`
//	GroupBy("country", "city")                  // GROUP BY `country`, `city`
//	GroupBy(Col("DATE(created_at)"))            // GROUP BY DATE(created_at)
func (q *SelectQueryBuilder) GroupBy(cols ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.groupByCols = []string{}

	for i, col := range cols {
		switch v := col.(type) {
		case string:
			newQuery.groupByCols = append(newQuery.groupByCols, quoteIdentifier(v))
		case *Expression:
			newQuery.groupByCols = append(newQuery.groupByCols, v.toSQL())
		default:
			newQuery.err = fmt.Errorf("invalid type for GroupBy argument %d: expected string or *Expression, got %T", i, col)

			return newQuery
		}
	}

	return newQuery
}

// Having adds a HAVING condition to the query (used with GROUP BY).
// Multiple Having() calls are combined with AND.
// Accepts a raw SQL string with placeholders and corresponding parameter values.
// Returns a new query builder with the HAVING condition added.
//
// Example:
//
//	GroupBy("status").Having("COUNT(*) > ?", 10)    // HAVING COUNT(*) > ?
//	Having("SUM(amount) > ?", 1000)                 // HAVING SUM(amount) > ?
func (q *SelectQueryBuilder) Having(condition string, params ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.havingClauses = append(newQuery.havingClauses, condition)
	newQuery.havingParams = append(newQuery.havingParams, params...)

	return newQuery
}

// OrderBy sets the ORDER BY clause for the query.
// Accepts strings (column names with optional ASC/DESC) or *Expression objects.
// Replaces any previously set ORDER BY clause.
// Returns a new query builder with the ORDER BY clause set.
//
// Example:
//
//	OrderBy("created_at DESC")                      // ORDER BY `created_at` DESC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
//	OrderBy(Col("price").Desc())                    // ORDER BY `price` DESC
//	OrderBy(Col("name").Asc(), Col("id").Desc())    // ORDER BY `name` ASC, `id` DESC
func (q *SelectQueryBuilder) OrderBy(cols ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.orderByClauses = []string{}

	for i, col := range cols {
		switch v := col.(type) {
		case string:
			newQuery.orderByClauses = append(newQuery.orderByClauses, quoteOrderByClause(v))
		case *Expression:
			newQuery.orderByClauses = append(newQuery.orderByClauses, v.toSQL())
		default:
			newQuery.err = fmt.Errorf("invalid type for OrderBy argument %d: expected string or *Expression, got %T", i, col)

			return newQuery
		}
	}

	return newQuery
}

// Limit sets the maximum number of rows to return.
// Returns a new query builder with the LIMIT clause set.
//
// Example:
//
//	Limit(10)   // LIMIT 10
//	Limit(100)  // LIMIT 100
func (q *SelectQueryBuilder) Limit(limit int) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.limitValue = &limit

	return newQuery
}

// Offset sets the number of rows to skip before returning results.
// Typically used with Limit() for pagination.
// Returns a new query builder with the OFFSET clause set.
//
// Example:
//
//	Limit(10).Offset(20)   // LIMIT 10 OFFSET 20 (page 3)
//	Limit(50).Offset(0)    // LIMIT 50 OFFSET 0 (page 1)
func (q *SelectQueryBuilder) Offset(offset int) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.offsetValue = &offset

	return newQuery
}

// ToSql generates the final SQL query string and parameter list.
// Returns the SQL string, parameters slice, and any error encountered during building.
// This method should be called when you need the raw SQL for manual execution.
//
// Example:
//
//	sql, args, err := From("users").
//		Where("status = ?", "active").
//		Limit(10).
//		ToSql()
//	// sql: "SELECT * FROM `users` WHERE status = ? LIMIT ?"
//	// args: []any{"active", 10}
func (q *SelectQueryBuilder) ToSql() (query string, params []any, err error) {
	// Check if the query has any errors from previous operations
	if q.err != nil {
		return "", nil, q.err
	}

	if q.table == "" {
		return "", nil, errors.New("table name is required")
	}

	var sql strings.Builder
	params = []any{}

	// SELECT clause
	sql.WriteString("SELECT ")
	if q.distinct {
		sql.WriteString("DISTINCT ")
	}

	if len(q.projections) == 0 {
		sql.WriteString("*")
	} else {
		sql.WriteString(strings.Join(q.projections, ", "))
	}

	// FROM clause
	sql.WriteString(" FROM ")
	sql.WriteString(quoteIdentifier(q.table))
	if q.tableAlias != "" {
		sql.WriteString(" AS ")
		sql.WriteString(q.tableAlias)
	}

	// WHERE clause
	if len(q.whereClauses) > 0 {
		sql.WriteString(" WHERE ")
		sql.WriteString(strings.Join(q.whereClauses, " AND "))
		params = append(params, q.whereParams...)
	}

	// GROUP BY clause
	if len(q.groupByCols) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(q.groupByCols, ", "))
	}

	// HAVING clause
	if len(q.havingClauses) > 0 {
		sql.WriteString(" HAVING ")
		sql.WriteString(strings.Join(q.havingClauses, " AND "))
		params = append(params, q.havingParams...)
	}

	// ORDER BY clause
	if len(q.orderByClauses) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(q.orderByClauses, ", "))
	}

	// LIMIT clause
	if q.limitValue != nil {
		sql.WriteString(" LIMIT ?")
		params = append(params, *q.limitValue)
	}

	// OFFSET clause
	if q.offsetValue != nil {
		sql.WriteString(" OFFSET ?")
		params = append(params, *q.offsetValue)
	}

	return sql.String(), params, nil
}

// Select executes the query and scans all results into the provided destination.
// The destination should be a pointer to a slice of structs.
//
// If no columns have been explicitly set via Columns() or Column(), Select will
// automatically call ForType() to map struct fields to database columns using the
// `db` struct tag. If columns have been explicitly set, those columns will be used
// as-is without calling ForType().
//
// Returns an error if the client is not set or if the query fails.
//
// Example with automatic column detection:
//
//	var users []User
//	err := From("users").
//		WithClient(client).
//		Where("status = ?", "active").
//		Select(ctx, &users)  // Automatically selects columns based on User struct
//
// Example with explicit columns:
//
//	var users []User
//	err := From("users").
//		Columns("id", "name").  // Explicit columns - ForType() not called
//		WithClient(client).
//		Select(ctx, &users)
func (q *SelectQueryBuilder) Select(ctx context.Context, dest any) error {
	if err := validatePointer(dest, "Select"); err != nil {
		return err
	}

	if q.client == nil {
		return errors.New("no client set for query execution")
	}

	qb := q
	if len(q.projections) == 0 {
		qb = qb.ForType(dest)
	}

	var err error
	var sql string
	var args []any

	if sql, args, err = qb.ToSql(); err != nil {
		return fmt.Errorf("could not build sql for execution: %w", err)
	}

	return qb.client.Select(ctx, dest, sql, args...)
}

// Get executes the query and scans exactly one result into the provided destination.
// The destination should be a pointer to a single struct.
//
// If no columns have been explicitly set via Columns() or Column(), Get will
// automatically call ForType() to map struct fields to database columns using the
// `db` struct tag. If columns have been explicitly set, those columns will be used
// as-is without calling ForType().
//
// Returns an error if the client is not set, if no rows are found,
// or if more than one row is returned.
//
// Example with automatic column detection:
//
//	var user User
//	err := From("users").
//		WithClient(client).
//		Where("id = ?", 123).
//		Get(ctx, &user)  // Automatically selects columns based on User struct
//
// Example with explicit columns:
//
//	var user User
//	err := From("users").
//		Columns("id", "name").  // Explicit columns - ForType() not called
//		WithClient(client).
//		Where("id = ?", 123).
//		Get(ctx, &user)
func (q *SelectQueryBuilder) Get(ctx context.Context, dest any) error {
	if err := validatePointer(dest, "Get"); err != nil {
		return err
	}

	// Get should receive a single struct, not a slice
	rv := reflect.ValueOf(dest)
	// Get the element that the pointer points to
	elem := rv.Elem()
	if elem.Kind() == reflect.Slice {
		return fmt.Errorf("Get: destination must be a single struct, not a slice (got %T). Use Select() for multiple results or pass a pointer to a struct", dest)
	}

	if q.client == nil {
		return errors.New("no client set for query execution")
	}

	qb := q
	if len(q.projections) == 0 {
		qb = qb.ForType(dest)
	}

	var err error
	var sql string
	var args []any

	if sql, args, err = qb.ToSql(); err != nil {
		return fmt.Errorf("could not build sql for execution: %w", err)
	}

	return qb.client.Get(ctx, dest, sql, args...)
}

// validatePointer checks if the provided value is a pointer.
// Returns a descriptive error if the value is not a pointer.
func validatePointer(v any, funcName string) error {
	if v == nil {
		return fmt.Errorf("%s: destination cannot be nil", funcName)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer {
		return fmt.Errorf("%s: destination must be a pointer, got %T (use &%T instead)", funcName, v, v)
	}

	if rv.IsNil() {
		return fmt.Errorf("%s: destination pointer cannot be nil", funcName)
	}

	return nil
}
