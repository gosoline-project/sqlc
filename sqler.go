package sqlc

import (
	"fmt"
	"sort"
	"strings"

	"github.com/justtrackio/gosoline/pkg/funk"
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
	config  *QueryBuilderConfig
	err     error
}

// NewSqlerWhere creates a new SqlerWhere instance.
func NewSqlerWhere() *SqlerWhere {
	return &SqlerWhere{
		clauses: []string{},
		params:  []any{},
		config:  DefaultConfig(),
	}
}

// IsEmpty returns true if no WHERE conditions have been added.
func (s *SqlerWhere) IsEmpty() bool {
	return len(s.clauses) == 0
}

// WithConfig sets the config for placeholder formatting and struct tag reading.
// Returns the same SqlerWhere instance for method chaining.
//
// Example:
//
//	config := &QueryBuilderConfig{Placeholder: "$"}
//	sqlerWhere := NewSqlerWhere()
//	sqlerWhere.WithConfig(config).Where("status = ?", "active")
//	sql, params, err := sqlerWhere.ToSql()
//	// sql: "status = $1"
//	// params: []any{"active"}
func (s *SqlerWhere) WithConfig(config *QueryBuilderConfig) *SqlerWhere {
	s.config = config

	return s
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
		s.clauses = append(s.clauses, v.toConditionSQL(s.config.IdentifierQuote))
		s.params = append(s.params, v.collectParameters()...)
	case Eq:
		// Handle Eq map type - convert to expressions
		if len(v) == 0 {
			return s // Empty map is a no-op
		}

		// Sort keys for deterministic SQL generation
		keys := funk.Keys(v)
		sort.Strings(keys)

		// Create equality expressions for each key-value pair
		expressions := funk.Map(keys, func(key string) *Expression {
			return Col(key).Eq(v[key])
		})

		// Single condition doesn't need AND wrapping
		var expr *Expression
		if len(expressions) == 1 {
			expr = expressions[0]
		} else {
			expr = And(expressions...)
		}

		s.clauses = append(s.clauses, expr.toConditionSQL(s.config.IdentifierQuote))
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
// Uses the configured placeholder format (set via WithConfig).
//
// Example:
//
//	sqlerWhere := NewSqlerWhere()
//	sqlerWhere.Where("status = ?", "active").Where("age > ?", 18)
//	sql, params, err := sqlerWhere.ToSql()
//	// sql: "status = ? AND age > ?"
//	// params: []any{"active", 18}
//
// With custom config:
//
//	config := &QueryBuilderConfig{Placeholder: "$"}
//	sqlerWhere := NewSqlerWhere()
//	sqlerWhere.WithConfig(config).Where("status = ?", "active").Where("age > ?", 18)
//	sql, params, err := sqlerWhere.ToSql()
//	// sql: "status = $1 AND age > $2"
//	// params: []any{"active", 18}
func (s *SqlerWhere) ToSql() (query string, params []any, err error) {
	return s.toSqlWithStartIndex(1)
}

// toSqlWithStartIndex generates the WHERE clause SQL fragment with a custom starting parameter index.
// This is used internally by query builders to maintain parameter index continuity across multiple clauses.
func (s *SqlerWhere) toSqlWithStartIndex(startIndex int) (query string, params []any, err error) {
	if s.err != nil {
		return "", nil, s.err
	}

	if len(s.clauses) == 0 {
		return "", []any{}, nil
	}

	sql := strings.Join(s.clauses, " AND ")

	// If using default "?" placeholder, no need to replace
	if s.config.Placeholder == "?" {
		return sql, s.params, nil
	}

	// Replace all "?" placeholders with numbered placeholders
	paramIndex := startIndex
	for i := 0; i < len(s.params); i++ {
		placeholder := s.config.PlaceholderFormat(paramIndex)
		// Replace first occurrence of "?"
		sql = strings.Replace(sql, "?", placeholder, 1)
		paramIndex++
	}

	return sql, s.params, nil
}

// SqlerGroupBy handles GROUP BY clause construction for SQL queries.
// It extracts the group by logic to be reusable across different query builders.
type SqlerGroupBy struct {
	clauses []string
	config  *QueryBuilderConfig
	err     error
}

// NewSqlerGroupBy creates a new SqlerGroupBy instance.
func NewSqlerGroupBy() *SqlerGroupBy {
	return &SqlerGroupBy{
		clauses: []string{},
		config:  DefaultConfig(),
	}
}

// IsEmpty returns true if no GROUP BY columns have been added.
func (s *SqlerGroupBy) IsEmpty() bool {
	return len(s.clauses) == 0
}

// WithConfig sets the config for identifier quoting.
// Returns the same SqlerGroupBy instance for method chaining.
//
// Example:
//
//	config := &QueryBuilderConfig{IdentifierQuote: "\""}
//	sqlerGroupBy := NewSqlerGroupBy()
//	sqlerGroupBy.WithConfig(config).GroupBy("status")
//	sql, err := sqlerGroupBy.ToSql()
//	// sql: "\"status\""
func (s *SqlerGroupBy) WithConfig(config *QueryBuilderConfig) *SqlerGroupBy {
	s.config = config

	return s
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
			s.clauses = append(s.clauses, quoteIdentifier(v, s.config.IdentifierQuote))
		case *Expression:
			s.clauses = append(s.clauses, v.toSQL(s.config.IdentifierQuote))
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
	config  *QueryBuilderConfig
	err     error
}

// NewSqlerHaving creates a new SqlerHaving instance.
func NewSqlerHaving() *SqlerHaving {
	return &SqlerHaving{
		clauses: []string{},
		params:  []any{},
		config:  DefaultConfig(),
	}
}

// IsEmpty returns true if no HAVING conditions have been added.
func (s *SqlerHaving) IsEmpty() bool {
	return len(s.clauses) == 0
}

// WithConfig sets the config for placeholder formatting and struct tag reading.
// Returns the same SqlerHaving instance for method chaining.
//
// Example:
//
//	config := &QueryBuilderConfig{Placeholder: "$"}
//	sqlerHaving := NewSqlerHaving()
//	sqlerHaving.WithConfig(config).Having("COUNT(*) > ?", 10)
//	sql, params, err := sqlerHaving.ToSql()
//	// sql: "COUNT(*) > $1"
//	// params: []any{10}
func (s *SqlerHaving) WithConfig(config *QueryBuilderConfig) *SqlerHaving {
	s.config = config

	return s
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
//	Having(Col("*").Count().Gt(10))                  // HAVING COUNT(*) > ?
//	Having(And(Col("*").Count().Gt(5), Col("amount").Sum().Gt(1000))) // HAVING (COUNT(*) > ? AND SUM(`amount`) > ?)
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
		s.clauses = append(s.clauses, v.toConditionSQL(s.config.IdentifierQuote))
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
// Uses the configured placeholder format (set via WithConfig).
//
// Example:
//
//	sqlerHaving := NewSqlerHaving()
//	sqlerHaving.Having("COUNT(*) > ?", 10).Having("SUM(amount) > ?", 1000)
//	sql, params, err := sqlerHaving.ToSql()
//	// sql: "COUNT(*) > ? AND SUM(amount) > ?"
//	// params: []any{10, 1000}
//
// With custom config:
//
//	config := &QueryBuilderConfig{Placeholder: "$"}
//	sqlerHaving := NewSqlerHaving()
//	sqlerHaving.WithConfig(config).Having("COUNT(*) > ?", 10).Having("SUM(amount) > ?", 1000)
//	sql, params, err := sqlerHaving.ToSql()
//	// sql: "COUNT(*) > $1 AND SUM(amount) > $2"
//	// params: []any{10, 1000}
func (s *SqlerHaving) ToSql() (query string, params []any, err error) {
	return s.toSqlWithStartIndex(1)
}

// toSqlWithStartIndex generates the HAVING clause SQL fragment with a custom starting parameter index.
// This is used internally by query builders to maintain parameter index continuity across multiple clauses.
func (s *SqlerHaving) toSqlWithStartIndex(startIndex int) (query string, params []any, err error) {
	if s.err != nil {
		return "", nil, s.err
	}

	if len(s.clauses) == 0 {
		return "", []any{}, nil
	}

	sql := strings.Join(s.clauses, " AND ")

	// If using default "?" placeholder, no need to replace
	if s.config.Placeholder == "?" {
		return sql, s.params, nil
	}

	// Replace all "?" placeholders with numbered placeholders
	paramIndex := startIndex
	for i := 0; i < len(s.params); i++ {
		placeholder := s.config.PlaceholderFormat(paramIndex)
		// Replace first occurrence of "?"
		sql = strings.Replace(sql, "?", placeholder, 1)
		paramIndex++
	}

	return sql, s.params, nil
}

// JoinType represents the type of SQL JOIN operation.
type JoinType string

const (
	// JoinInner represents an INNER JOIN (or simply JOIN).
	JoinInner JoinType = "JOIN"
	// JoinLeft represents a LEFT JOIN.
	JoinLeft JoinType = "LEFT JOIN"
	// JoinRight represents a RIGHT JOIN.
	JoinRight JoinType = "RIGHT JOIN"
	// JoinCross represents a CROSS JOIN (cartesian product, no ON condition).
	JoinCross JoinType = "CROSS JOIN"
	// JoinFullOuter represents a FULL OUTER JOIN.
	JoinFullOuter JoinType = "FULL OUTER JOIN"
	// JoinNatural represents a NATURAL JOIN (automatically matches on same-named columns, no ON condition).
	JoinNatural JoinType = "NATURAL JOIN"
	// JoinNaturalLeft represents a NATURAL LEFT JOIN (no ON condition).
	JoinNaturalLeft JoinType = "NATURAL LEFT JOIN"
	// JoinNaturalRight represents a NATURAL RIGHT JOIN (no ON condition).
	JoinNaturalRight JoinType = "NATURAL RIGHT JOIN"
	// JoinNaturalFull represents a NATURAL FULL OUTER JOIN (no ON condition).
	JoinNaturalFull JoinType = "NATURAL FULL OUTER JOIN"
)

// JoinClause represents a single JOIN clause in a SQL query.
// It stores the join type, table name, optional alias, and the ON condition.
type JoinClause struct {
	joinType  JoinType
	table     string
	alias     string
	condition string      // raw ON condition string
	condExpr  *Expression // expression-based ON condition
	params    []any       // parameters for raw string ON conditions
	config    *QueryBuilderConfig
}

// NewJoinClause creates a new JoinClause with the specified join type, table, alias, and raw ON condition.
// For CROSS JOINs, pass an empty string for condition.
// For Expression-based conditions, use the JoinBuilder fluent API instead.
func NewJoinClause(joinType JoinType, table string, alias string, condition string, params ...any) JoinClause {
	return JoinClause{
		joinType:  joinType,
		table:     table,
		alias:     alias,
		condition: condition,
		params:    params,
	}
}

// isNatural returns true if this join type is a NATURAL JOIN variant.
// NATURAL JOINs do not take an ON condition.
func (j *JoinClause) isNatural() bool {
	return j.joinType == JoinNatural ||
		j.joinType == JoinNaturalLeft ||
		j.joinType == JoinNaturalRight ||
		j.joinType == JoinNaturalFull
}

// toSqlWithStartIndex generates the SQL fragment for this JOIN clause with a custom starting parameter index.
// Returns the SQL string, parameters, and any error encountered.
func (j *JoinClause) toSqlWithStartIndex(startIndex int) (query string, params []any, err error) {
	quote := identifierQuote
	if j.config != nil {
		quote = j.config.IdentifierQuote
	}

	var sqlBuilder strings.Builder

	sqlBuilder.WriteString(string(j.joinType))
	sqlBuilder.WriteString(" ")
	sqlBuilder.WriteString(quoteIdentifier(j.table, quote))

	if j.alias != "" {
		sqlBuilder.WriteString(" AS ")
		sqlBuilder.WriteString(j.alias)
	}

	// CROSS JOIN and NATURAL JOINs do not have an ON condition
	if j.joinType == JoinCross || j.isNatural() {
		return sqlBuilder.String(), nil, nil
	}

	// All other JOINs require an ON condition
	if j.condition == "" && j.condExpr == nil {
		return "", nil, fmt.Errorf("JOIN on table %q requires an ON condition", j.table)
	}

	sqlBuilder.WriteString(" ON ")

	if j.condExpr != nil {
		// Expression-based ON condition
		sqlBuilder.WriteString(j.condExpr.toConditionSQL(quote))
		params = j.condExpr.collectParameters()
	} else {
		// Raw string ON condition — auto-quote identifiers
		sql := quoteConditionIdentifiers(j.condition, quote)
		params = j.params

		// Replace placeholders if not using default "?"
		if j.config != nil && j.config.Placeholder != "?" {
			paramIndex := startIndex
			for i := 0; i < len(params); i++ {
				placeholder := j.config.PlaceholderFormat(paramIndex)
				sql = strings.Replace(sql, "?", placeholder, 1)
				paramIndex++
			}
		}

		sqlBuilder.WriteString(sql)
	}

	return sqlBuilder.String(), params, nil
}

// SqlerJoin handles JOIN clause construction for SQL queries.
// It stores a list of join clauses that are rendered in order between
// the FROM clause and the WHERE clause.
type SqlerJoin struct {
	clauses []JoinClause
	config  *QueryBuilderConfig
	err     error
}

// NewSqlerJoin creates a new SqlerJoin instance.
func NewSqlerJoin() *SqlerJoin {
	return &SqlerJoin{
		clauses: []JoinClause{},
		config:  DefaultConfig(),
	}
}

// IsEmpty returns true if no JOIN clauses have been added.
func (s *SqlerJoin) IsEmpty() bool {
	return len(s.clauses) == 0
}

// WithConfig sets the config for identifier quoting and placeholder formatting.
// Returns the same SqlerJoin instance for method chaining.
func (s *SqlerJoin) WithConfig(config *QueryBuilderConfig) *SqlerJoin {
	s.config = config

	return s
}

// AddJoin appends a JoinClause to the list of joins.
func (s *SqlerJoin) AddJoin(clause JoinClause) {
	clause.config = s.config
	s.clauses = append(s.clauses, clause)
}

// ToSql generates the complete JOIN SQL fragment.
// Returns the SQL string (all JOINs separated by spaces), parameters, and any error encountered.
//
// Example:
//
//	sqlerJoin := NewSqlerJoin()
//	sqlerJoin.AddJoin(NewJoinClause(JoinLeft, "orders", "o", "u.id = o.user_id"))
//	sql, params, err := sqlerJoin.ToSql()
//	// sql: "LEFT JOIN `orders` AS o ON u.id = o.user_id"
//	// params: []any{}
func (s *SqlerJoin) ToSql() (query string, params []any, err error) {
	return s.toSqlWithStartIndex(0)
}

// toSqlWithStartIndex generates the complete JOIN SQL fragment with a custom starting parameter index.
// Returns the SQL string (all JOINs separated by spaces), parameters, and any error encountered.
func (s *SqlerJoin) toSqlWithStartIndex(startIndex int) (query string, params []any, err error) {
	if s.err != nil {
		return "", nil, s.err
	}

	if len(s.clauses) == 0 {
		return "", nil, nil
	}

	var parts []string
	paramIndex := startIndex

	for i := range s.clauses {
		var sql string
		var joinParams []any

		if sql, joinParams, err = s.clauses[i].toSqlWithStartIndex(paramIndex); err != nil {
			return "", nil, fmt.Errorf("could not build JOIN clause %d: %w", i, err)
		}

		parts = append(parts, sql)
		params = append(params, joinParams...)
		paramIndex += len(joinParams)
	}

	return strings.Join(parts, " "), params, nil
}

// SqlerOrderBy handles ORDER BY clause construction for SQL queries.
// It extracts the order by logic to be reusable across different query builders.
type SqlerOrderBy struct {
	clauses []string
	config  *QueryBuilderConfig
	err     error
}

// NewSqlerOrderBy creates a new SqlerOrderBy instance.
func NewSqlerOrderBy() *SqlerOrderBy {
	return &SqlerOrderBy{
		clauses: []string{},
		config:  DefaultConfig(),
	}
}

// IsEmpty returns true if no ORDER BY clauses have been added.
func (s *SqlerOrderBy) IsEmpty() bool {
	return len(s.clauses) == 0
}

// WithConfig sets the config for identifier quoting.
// Returns the same SqlerOrderBy instance for method chaining.
//
// Example:
//
//	config := &QueryBuilderConfig{IdentifierQuote: "\""}
//	sqlerOrderBy := NewSqlerOrderBy()
//	sqlerOrderBy.WithConfig(config).OrderBy("name ASC")
//	sql, err := sqlerOrderBy.ToSql()
//	// sql: "\"name\" ASC"
func (s *SqlerOrderBy) WithConfig(config *QueryBuilderConfig) *SqlerOrderBy {
	s.config = config

	return s
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
			s.clauses = append(s.clauses, quoteOrderByClause(v, s.config.IdentifierQuote))
		case *Expression:
			s.clauses = append(s.clauses, v.toSQL(s.config.IdentifierQuote))
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
