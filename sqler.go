package sqlc

import (
	"fmt"
	"sort"
	"strings"
)

type (
	Sqler interface {
		ToSql() (query string, params []any, err error)
	}
)

// SqlerWhere handles WHERE clause construction for SQL queries.
// It extracts the where logic to be reusable across different query builders.
// Multiple Where() calls are combined with AND.
type SqlerWhere struct {
	clauses []string
	params  []any
	err     error
}

// NewSqlerWhere creates a new SqlerWhere instance.
func NewSqlerWhere() *SqlerWhere {
	return &SqlerWhere{
		clauses: []string{},
		params:  []any{},
	}
}

// IsEmpty returns true if no WHERE conditions have been added.
func (s *SqlerWhere) IsEmpty() bool {
	return len(s.clauses) == 0
}

// Where adds a WHERE condition to the query.
// Multiple Where() calls are combined with AND.
// Accepts either:
//   - A raw SQL string with placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//   - An Eq map for creating equality conditions from column-value pairs
//
// Returns the same SqlerWhere instance for method chaining.
//
// Example:
//
//	Where("status = ?", "active")                    // WHERE status = ?
//	Where(Col("age").Gt(18))                         // WHERE `age` > ?
//	Where(And(Col("a").Eq(1), Col("b").Eq(2)))       // WHERE (`a` = ? AND `b` = ?)
//	Where("status = ?", "active").Where("age > ?", 18) // WHERE status = ? AND age > ?
//	Where(Eq{"status": "active", "role": "admin"})   // WHERE (`role` = ? AND `status` = ?)
func (s *SqlerWhere) Where(condition any, params ...any) *SqlerWhere {
	switch v := condition.(type) {
	case string:
		s.clauses = append(s.clauses, v)
		s.params = append(s.params, params...)
	case *Expression:
		// Skip nil expressions (e.g., from Eq() with empty map)
		if v == nil {
			return s
		}
		s.clauses = append(s.clauses, v.toConditionSQL())
		s.params = append(s.params, v.collectParameters()...)
	case Eq:
		// Handle Eq map type - convert to expressions
		if len(v) == 0 {
			return s // Empty map is a no-op
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

		s.clauses = append(s.clauses, expr.toConditionSQL())
		s.params = append(s.params, expr.collectParameters()...)
	default:
		s.err = fmt.Errorf("invalid type for Where condition: expected string or *Expression, got %T", condition)

		return s
	}

	return s
}

// ToSql generates the WHERE clause SQL fragment and parameter list.
// Returns the WHERE clause (without the "WHERE" keyword), parameters, and any error encountered.
// If there are no where clauses, it returns an empty string for the query.
//
// Example:
//
//	sqlerWhere := NewSqlerWhere()
//	sqlerWhere.Where("status = ?", "active").Where("age > ?", 18)
//	sql, params, err := sqlerWhere.ToSql()
//	// sql: "status = ? AND age > ?"
//	// params: []any{"active", 18}
func (s *SqlerWhere) ToSql() (query string, params []any, err error) {
	if s.err != nil {
		return "", nil, s.err
	}

	if len(s.clauses) == 0 {
		return "", []any{}, nil
	}

	return strings.Join(s.clauses, " AND "), s.params, nil
}

// SqlerGroupBy handles GROUP BY clause construction for SQL queries.
// It extracts the group by logic to be reusable across different query builders.
type SqlerGroupBy struct {
	clauses []string
	err     error
}

// NewSqlerGroupBy creates a new SqlerGroupBy instance.
func NewSqlerGroupBy() *SqlerGroupBy {
	return &SqlerGroupBy{
		clauses: []string{},
	}
}

// IsEmpty returns true if no GROUP BY columns have been added.
func (s *SqlerGroupBy) IsEmpty() bool {
	return len(s.clauses) == 0
}

// GroupBy sets the GROUP BY columns for the query.
// Accepts strings (column names) or *Expression objects.
// Replaces any previously set GROUP BY clause.
// Returns the same SqlerGroupBy instance for method chaining.
//
// Example:
//
//	GroupBy("status")                           // GROUP BY `status`
//	GroupBy("country", "city")                  // GROUP BY `country`, `city`
//	GroupBy(Col("DATE(created_at)"))            // GROUP BY DATE(created_at)
func (s *SqlerGroupBy) GroupBy(cols ...any) *SqlerGroupBy {
	s.clauses = []string{}

	for i, col := range cols {
		switch v := col.(type) {
		case string:
			s.clauses = append(s.clauses, quoteIdentifier(v))
		case *Expression:
			s.clauses = append(s.clauses, v.toSQL())
		default:
			s.err = fmt.Errorf("invalid type for GroupBy argument %d: expected string or *Expression, got %T", i, col)

			return s
		}
	}

	return s
}

// ToSql generates the GROUP BY clause SQL fragment.
// Returns the GROUP BY clause (without the "GROUP BY" keywords), and any error encountered.
// If there are no group by columns, it returns an empty string for the query.
//
// Example:
//
//	sqlerGroupBy := NewSqlerGroupBy()
//	sqlerGroupBy.GroupBy("status", "country")
//	sql, err := sqlerGroupBy.ToSql()
//	// sql: "`status`, `country`"
func (s *SqlerGroupBy) ToSql() (query string, err error) {
	if s.err != nil {
		return "", s.err
	}

	if len(s.clauses) == 0 {
		return "", nil
	}

	return strings.Join(s.clauses, ", "), nil
}

// SqlerHaving handles HAVING clause construction for SQL queries.
// It extracts the having logic to be reusable across different query builders.
// Multiple Having() calls are combined with AND.
type SqlerHaving struct {
	clauses []string
	params  []any
	err     error
}

// NewSqlerHaving creates a new SqlerHaving instance.
func NewSqlerHaving() *SqlerHaving {
	return &SqlerHaving{
		clauses: []string{},
		params:  []any{},
	}
}

// IsEmpty returns true if no HAVING conditions have been added.
func (s *SqlerHaving) IsEmpty() bool {
	return len(s.clauses) == 0
}

// Having adds a HAVING condition to the query (used with GROUP BY).
// Multiple Having() calls are combined with AND.
// Accepts either:
//   - A raw SQL string with placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//
// Returns the same SqlerHaving instance for method chaining.
//
// Example:
//
//	Having("COUNT(*) > ?", 10)                       // HAVING COUNT(*) > ?
//	Having("SUM(amount) > ?", 1000)                  // HAVING SUM(amount) > ?
//	Having(Col("COUNT(*)").Gt(10))                   // HAVING COUNT(*) > ?
//	Having(And(Col("COUNT(*)").Gt(5), Col("SUM(amount)").Gt(1000))) // HAVING (COUNT(*) > ? AND SUM(amount) > ?)
func (s *SqlerHaving) Having(condition any, params ...any) *SqlerHaving {
	switch v := condition.(type) {
	case string:
		s.clauses = append(s.clauses, v)
		s.params = append(s.params, params...)
	case *Expression:
		// Skip nil expressions
		if v == nil {
			return s
		}
		s.clauses = append(s.clauses, v.toConditionSQL())
		s.params = append(s.params, v.collectParameters()...)
	default:
		s.err = fmt.Errorf("invalid type for Having condition: expected string or *Expression, got %T", condition)

		return s
	}

	return s
}

// ToSql generates the HAVING clause SQL fragment and parameter list.
// Returns the HAVING clause (without the "HAVING" keyword), parameters, and any error encountered.
// If there are no having clauses, it returns an empty string for the query.
//
// Example:
//
//	sqlerHaving := NewSqlerHaving()
//	sqlerHaving.Having("COUNT(*) > ?", 10).Having("SUM(amount) > ?", 1000)
//	sql, params, err := sqlerHaving.ToSql()
//	// sql: "COUNT(*) > ? AND SUM(amount) > ?"
//	// params: []any{10, 1000}
func (s *SqlerHaving) ToSql() (query string, params []any, err error) {
	if s.err != nil {
		return "", nil, s.err
	}

	if len(s.clauses) == 0 {
		return "", []any{}, nil
	}

	return strings.Join(s.clauses, " AND "), s.params, nil
}

// SqlerOrderBy handles ORDER BY clause construction for SQL queries.
// It extracts the order by logic to be reusable across different query builders.
type SqlerOrderBy struct {
	clauses []string
	err     error
}

// NewSqlerOrderBy creates a new SqlerOrderBy instance.
func NewSqlerOrderBy() *SqlerOrderBy {
	return &SqlerOrderBy{
		clauses: []string{},
	}
}

// IsEmpty returns true if no ORDER BY clauses have been added.
func (s *SqlerOrderBy) IsEmpty() bool {
	return len(s.clauses) == 0
}

// OrderBy sets the ORDER BY clause for the query.
// Accepts strings (column names with optional ASC/DESC) or *Expression objects.
// Replaces any previously set ORDER BY clause.
// Returns the same SqlerOrderBy instance for method chaining.
//
// Example:
//
//	OrderBy("created_at DESC")                      // ORDER BY `created_at` DESC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
//	OrderBy(Col("price").Desc())                    // ORDER BY `price` DESC
//	OrderBy(Col("name").Asc(), Col("id").Desc())    // ORDER BY `name` ASC, `id` DESC
func (s *SqlerOrderBy) OrderBy(cols ...any) *SqlerOrderBy {
	s.clauses = []string{}

	for i, col := range cols {
		switch v := col.(type) {
		case string:
			s.clauses = append(s.clauses, quoteOrderByClause(v))
		case *Expression:
			s.clauses = append(s.clauses, v.toSQL())
		default:
			s.err = fmt.Errorf("invalid type for OrderBy argument %d: expected string or *Expression, got %T", i, col)

			return s
		}
	}

	return s
}

// ToSql generates the ORDER BY clause SQL fragment.
// Returns the ORDER BY clause (without the "ORDER BY" keywords), and any error encountered.
// If there are no order by clauses, it returns an empty string for the query.
//
// Example:
//
//	sqlerOrderBy := NewSqlerOrderBy()
//	sqlerOrderBy.OrderBy("name ASC", "created_at DESC")
//	sql, err := sqlerOrderBy.ToSql()
//	// sql: "`name` ASC, `created_at` DESC"
func (s *SqlerOrderBy) ToSql() (query string, err error) {
	if s.err != nil {
		return "", s.err
	}

	if len(s.clauses) == 0 {
		return "", nil
	}

	return strings.Join(s.clauses, ", "), nil
}
