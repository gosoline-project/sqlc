package sqlg

import (
	"fmt"
	"strings"
)

// Expression represents a SQL expression that can be used in queries.
// It supports various types of expressions:
//   - Simple column references (via Col)
//   - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//   - WHERE conditions (Eq, NotEq, Gt, Lt, In, IsNull, etc.)
//   - Composite conditions (AND, OR, NOT)
//   - ORDER BY directions (Asc, Desc)
//   - Aliases (As)
//
// Expressions are immutable - each method returns a new Expression instance.
type Expression struct {
	raw        string
	function   string
	alias      string
	direction  string // for ORDER BY
	condition  string // for WHERE conditions like "IN", "=", etc.
	parameters []any
	isLiteral  bool // if true, raw value is not quoted
	// For composite expressions (AND, OR, NOT)
	operator       string // "AND", "OR", "NOT"
	subExpressions []*Expression
}

// Col creates a new Expression from a column name.
// The column name can be simple ("id"), table-qualified ("users.id"),
// or include JSON operators ("data->'$.email'").
//
// Example:
//
//	Col("name")                  // Simple column
//	Col("users.email")           // Table-qualified
//	Col("metadata->'$.address'") // JSON expression
func Col(name string) *Expression {
	return &Expression{raw: name}
}

// Literal creates a raw SQL expression without any processing or quoting.
// Use this when you need to embed raw SQL that should not be quoted or modified.
//
// Example:
//
//	Literal("CURRENT_TIMESTAMP")
//	Literal("COALESCE(amount, 0)")
func Literal(sql string) *Expression {
	return &Expression{raw: sql}
}

// Lit creates a literal value expression that is not quoted.
// This is useful for positional column references in ORDER BY or literal values.
// The value is used as-is without any quoting or modification, and can be
// combined with other expression methods like Asc(), Desc(), etc.
//
// Accepts strings and numeric types (int, int64, float64, etc.) which are
// converted to their string representation.
//
// Example:
//
//	Lit(1)                       // Literal value: 1
//	Lit(1).Asc()                 // ORDER BY 1 ASC (positional column reference)
//	Lit(2).Desc()                // ORDER BY 2 DESC
//	Lit("CURRENT_TIMESTAMP")     // Literal: CURRENT_TIMESTAMP
//	Lit(3.14)                    // Literal: 3.14
func Lit(value any) *Expression {
	return &Expression{raw: fmt.Sprintf("%v", value), isLiteral: true}
}

// And combines multiple expressions with the AND logical operator.
// Returns a composite expression that evaluates to true only if all sub-expressions are true.
//
// Example:
//
//	And(Col("age").Gt(18), Col("status").Eq("active"))
//	// Generates: (`age` > ? AND `status` = ?)
func And(expressions ...*Expression) *Expression {
	return &Expression{
		operator:       "AND",
		subExpressions: expressions,
	}
}

// Or combines multiple expressions with the OR logical operator.
// Returns a composite expression that evaluates to true if any sub-expression is true.
//
// Example:
//
//	Or(Col("role").Eq("admin"), Col("role").Eq("moderator"))
//	// Generates: (`role` = ? OR `role` = ?)
func Or(expressions ...*Expression) *Expression {
	return &Expression{
		operator:       "OR",
		subExpressions: expressions,
	}
}

// Not negates an expression using the NOT logical operator.
// Returns a composite expression that inverts the truth value of the input expression.
//
// Example:
//
//	Not(Col("deleted").Eq(true))
//	// Generates: NOT (`deleted` = ?)
func Not(expr *Expression) *Expression {
	return &Expression{
		operator:       "NOT",
		subExpressions: []*Expression{expr},
	}
}

// Eq is a map type for creating equality conditions from column-value pairs.
// All conditions are combined with AND. Map keys are sorted for deterministic SQL generation.
// Empty maps are ignored (no WHERE clause added).
//
// Example:
//
//	Eq{"status": "active", "age": 21}
//	// Generates: (`age` = ? AND `status` = ?)
//
//	Eq{"id": 1}
//	// Generates: (`id` = ?)
type Eq map[string]any

// Count wraps the expression in a COUNT() aggregate function.
// Returns a new Expression that counts non-NULL values.
//
// Example:
//
//	Col("id").Count()        // COUNT(`id`)
//	Col("*").Count().As("total") // COUNT(*) AS total
func (e *Expression) Count() *Expression {
	return &Expression{
		raw:      e.raw,
		function: "COUNT",
		alias:    e.alias,
	}
}

// Sum wraps the expression in a SUM() aggregate function.
// Returns a new Expression that sums numeric values.
//
// Example:
//
//	Col("amount").Sum()           // SUM(`amount`)
//	Col("price").Sum().As("total") // SUM(`price`) AS total
func (e *Expression) Sum() *Expression {
	return &Expression{
		raw:      e.raw,
		function: "SUM",
		alias:    e.alias,
	}
}

// Avg wraps the expression in an AVG() aggregate function.
// Returns a new Expression that calculates the average of numeric values.
//
// Example:
//
//	Col("rating").Avg()             // AVG(`rating`)
//	Col("score").Avg().As("average") // AVG(`score`) AS average
func (e *Expression) Avg() *Expression {
	return &Expression{
		raw:      e.raw,
		function: "AVG",
		alias:    e.alias,
	}
}

// Min wraps the expression in a MIN() aggregate function.
// Returns a new Expression that finds the minimum value.
//
// Example:
//
//	Col("price").Min()              // MIN(`price`)
//	Col("created_at").Min().As("earliest") // MIN(`created_at`) AS earliest
func (e *Expression) Min() *Expression {
	return &Expression{
		raw:      e.raw,
		function: "MIN",
		alias:    e.alias,
	}
}

// Max wraps the expression in a MAX() aggregate function.
// Returns a new Expression that finds the maximum value.
//
// Example:
//
//	Col("price").Max()              // MAX(`price`)
//	Col("updated_at").Max().As("latest") // MAX(`updated_at`) AS latest
func (e *Expression) Max() *Expression {
	return &Expression{
		raw:      e.raw,
		function: "MAX",
		alias:    e.alias,
	}
}

// As sets an alias for the expression in the SELECT clause.
// Returns a new Expression with the specified alias.
//
// Example:
//
//	Col("user_name").As("name")          // `user_name` AS name
//	Col("id").Count().As("total_users")  // COUNT(`id`) AS total_users
//	Lit("'constant'").As("value")        // 'constant' AS value
func (e *Expression) As(alias string) *Expression {
	return &Expression{
		raw:       e.raw,
		function:  e.function,
		alias:     alias,
		isLiteral: e.isLiteral,
	}
}

// Asc marks the expression for ascending order in ORDER BY clauses.
// Returns a new Expression with ascending direction.
//
// Example:
//
//	Col("name").Asc()        // `name` ASC
//	Col("created_at").Asc()  // `created_at` ASC
//	Lit("1").Asc()           // 1 ASC
func (e *Expression) Asc() *Expression {
	return &Expression{
		raw:       e.raw,
		function:  e.function,
		alias:     e.alias,
		direction: "ASC",
		isLiteral: e.isLiteral,
	}
}

// Desc marks the expression for descending order in ORDER BY clauses.
// Returns a new Expression with descending direction.
//
// Example:
//
//	Col("price").Desc()      // `price` DESC
//	Col("created_at").Desc() // `created_at` DESC
//	Lit("2").Desc()          // 2 DESC
func (e *Expression) Desc() *Expression {
	return &Expression{
		raw:       e.raw,
		function:  e.function,
		alias:     e.alias,
		direction: "DESC",
		isLiteral: e.isLiteral,
	}
}

// Eq creates an equality condition (column = value).
// Returns a new Expression representing the equality comparison.
//
// Example:
//
//	Col("status").Eq("active")  // `status` = ?
//	Col("age").Eq(21)           // `age` = ?
func (e *Expression) Eq(value any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "=",
		parameters: []any{value},
	}
}

// NotEq creates a not-equal condition (column != value).
// Returns a new Expression representing the inequality comparison.
//
// Example:
//
//	Col("status").NotEq("deleted")  // `status` != ?
//	Col("role").NotEq("guest")      // `role` != ?
func (e *Expression) NotEq(value any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "!=",
		parameters: []any{value},
	}
}

// Gt creates a greater-than condition (column > value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("age").Gt(18)      // `age` > ?
//	Col("price").Gt(100.0) // `price` > ?
func (e *Expression) Gt(value any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  ">",
		parameters: []any{value},
	}
}

// Gte creates a greater-than-or-equal condition (column >= value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("age").Gte(18)      // `age` >= ?
//	Col("score").Gte(70)    // `score` >= ?
func (e *Expression) Gte(value any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  ">=",
		parameters: []any{value},
	}
}

// Lt creates a less-than condition (column < value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("age").Lt(65)      // `age` < ?
//	Col("stock").Lt(10)    // `stock` < ?
func (e *Expression) Lt(value any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "<",
		parameters: []any{value},
	}
}

// Lte creates a less-than-or-equal condition (column <= value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("price").Lte(100.0)  // `price` <= ?
//	Col("quantity").Lte(50)  // `quantity` <= ?
func (e *Expression) Lte(value any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "<=",
		parameters: []any{value},
	}
}

// In creates an IN condition (column IN (values...)).
// Returns a new Expression that checks if the column value is in the provided list.
//
// Example:
//
//	Col("status").In("active", "pending", "approved")  // `status` IN (?, ?, ?)
//	Col("id").In(1, 2, 3, 4, 5)                        // `id` IN (?, ?, ?, ?, ?)
func (e *Expression) In(values ...any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "IN",
		parameters: values,
	}
}

// NotIn creates a NOT IN condition (column NOT IN (values...)).
// Returns a new Expression that checks if the column value is not in the provided list.
//
// Example:
//
//	Col("status").NotIn("deleted", "archived")  // `status` NOT IN (?, ?)
//	Col("type").NotIn("spam", "bot")            // `type` NOT IN (?, ?)
func (e *Expression) NotIn(values ...any) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "NOT IN",
		parameters: values,
	}
}

// IsNull creates an IS NULL condition.
// Returns a new Expression that checks if the column value is NULL.
//
// Example:
//
//	Col("deleted_at").IsNull()  // `deleted_at` IS NULL
//	Col("parent_id").IsNull()   // `parent_id` IS NULL
func (e *Expression) IsNull() *Expression {
	return &Expression{
		raw:       e.raw,
		condition: "IS NULL",
	}
}

// IsNotNull creates an IS NOT NULL condition.
// Returns a new Expression that checks if the column value is not NULL.
//
// Example:
//
//	Col("email").IsNotNull()     // `email` IS NOT NULL
//	Col("verified_at").IsNotNull() // `verified_at` IS NOT NULL
func (e *Expression) IsNotNull() *Expression {
	return &Expression{
		raw:       e.raw,
		condition: "IS NOT NULL",
	}
}

// Like creates a LIKE condition for pattern matching.
// Returns a new Expression that performs pattern matching with the given pattern.
//
// Example:
//
//	Col("name").Like("%john%")    // `name` LIKE ?
//	Col("email").Like("%@example.com") // `email` LIKE ?
func (e *Expression) Like(pattern string) *Expression {
	return &Expression{
		raw:        e.raw,
		condition:  "LIKE",
		parameters: []any{pattern},
	}
}

// toSQL converts the expression to a SQL fragment for SELECT, GROUP BY, or ORDER BY clauses.
// It handles column quoting, function wrapping, aliases, and direction modifiers.
// Literal values (created via Lit()) are not quoted.
func (e *Expression) toSQL() string {
	var sql string
	if e.isLiteral {
		sql = e.raw // Don't quote literal values
	} else {
		sql = quoteIdentifier(e.raw)
	}
	if e.function != "" {
		sql = fmt.Sprintf("%s(%s)", e.function, sql)
	}
	if e.alias != "" {
		sql = fmt.Sprintf("%s AS %s", sql, e.alias)
	}
	if e.direction != "" {
		sql = fmt.Sprintf("%s %s", sql, e.direction)
	}

	return sql
}

// toConditionSQL converts the expression to a WHERE condition SQL fragment.
// It handles simple conditions (=, !=, >, <, etc.), IN/NOT IN, NULL checks,
// and composite expressions (AND, OR, NOT).
func (e *Expression) toConditionSQL() string {
	// Handle composite expressions (AND, OR, NOT)
	if e.operator != "" {
		return e.toCompositeConditionSQL()
	}

	// Handle simple conditions
	if e.condition == "" {
		return quoteIdentifier(e.raw)
	}

	quotedCol := quoteIdentifier(e.raw)

	if e.condition == "IS NULL" || e.condition == "IS NOT NULL" {
		return fmt.Sprintf("%s %s", quotedCol, e.condition)
	}

	if e.condition == "IN" || e.condition == "NOT IN" {
		placeholders := make([]string, len(e.parameters))
		for i := range placeholders {
			placeholders[i] = "?"
		}

		return fmt.Sprintf("%s %s (%s)", quotedCol, e.condition, strings.Join(placeholders, ", "))
	}

	return fmt.Sprintf("%s %s ?", quotedCol, e.condition)
}

// toCompositeConditionSQL handles composite expressions (AND, OR, NOT).
// It recursively processes sub-expressions and combines them with the appropriate operator.
func (e *Expression) toCompositeConditionSQL() string {
	if e.operator == "NOT" {
		if len(e.subExpressions) > 0 {
			return fmt.Sprintf("NOT (%s)", e.subExpressions[0].toConditionSQL())
		}

		return ""
	}

	// AND, OR
	if len(e.subExpressions) == 0 {
		return ""
	}

	parts := make([]string, len(e.subExpressions))
	for i, expr := range e.subExpressions {
		parts[i] = expr.toConditionSQL()
	}

	return fmt.Sprintf("(%s)", strings.Join(parts, fmt.Sprintf(" %s ", e.operator)))
}

// collectParameters recursively collects all parameters from the expression tree.
// For composite expressions (AND, OR, NOT), it collects parameters from all sub-expressions.
// For simple expressions, it returns the expression's own parameters.
func (e *Expression) collectParameters() []any {
	var params []any

	// If this is a composite expression, recursively collect from sub-expressions
	if e.operator != "" {
		for _, subExpr := range e.subExpressions {
			params = append(params, subExpr.collectParameters()...)
		}

		return params
	}

	// Otherwise, return this expression's parameters

	return e.parameters
}
