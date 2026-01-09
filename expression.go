package sqlc

import (
	"fmt"
	"strings"

	"github.com/justtrackio/gosoline/pkg/funk"
)

// Expression represents a SQL expression that can be used in queries.
// It supports various types of expressions:
//   - Simple column references (via Col)
//   - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//   - WHERE conditions (Eq, NotEq, Gt, Lt, In, IsNull, etc.)
//   - Composite conditions (AND, OR, NOT)
//   - ORDER BY directions (Asc, Desc)
//   - Aliases (As)
//   - Bind parameters (via Param)
//
// Expressions are immutable - each method returns a new Expression instance.
type Expression struct {
	raw          string
	function     string
	functionArgs []any         // additional arguments for functions like ROUND(col, 2) - legacy, inline rendering
	funcArgs     []*Expression // new: function arguments as expressions (supports bind params)
	alias        string
	direction    string // for ORDER BY
	condition    string // for WHERE conditions like "IN", "=", etc.
	parameters   []any
	isLiteral    bool // if true, raw value is not quoted
	isParam      bool // if true, this is a bind parameter (renders as "?")
	paramValue   any  // the value for bind parameter
	// For composite expressions (AND, OR, NOT)
	operator       string // "AND", "OR", "NOT"
	subExpressions []*Expression
}

// copy creates a shallow copy of the Expression.
// This helper method reduces code duplication when creating new Expression instances
// with minor modifications.
func (e *Expression) copy() *Expression {
	return &Expression{
		raw:            e.raw,
		function:       e.function,
		functionArgs:   e.functionArgs,
		funcArgs:       e.funcArgs,
		alias:          e.alias,
		direction:      e.direction,
		condition:      e.condition,
		parameters:     e.parameters,
		isLiteral:      e.isLiteral,
		isParam:        e.isParam,
		paramValue:     e.paramValue,
		operator:       e.operator,
		subExpressions: e.subExpressions,
	}
}

// wrapWithFunction wraps an expression in a function, handling both simple columns
// and complex expressions (nested functions, subexpressions, etc.).
// For simple columns, it creates a function with the raw column name.
// For complex expressions, it wraps the entire expression as a subexpression.
func (e *Expression) wrapWithFunction(functionName string) *Expression {
	// If the expression already has a function or is not a simple column, wrap it as a subexpression
	if e.function != "" || len(e.subExpressions) > 0 {
		return &Expression{
			function:       functionName,
			subExpressions: []*Expression{e},
			alias:          e.alias,
		}
	}
	// Simple column case
	return &Expression{
		raw:      e.raw,
		function: functionName,
		alias:    e.alias,
	}
}

// wrapWithFunctionArgs wraps an expression in a function with additional arguments.
// This is used for functions that take extra parameters like ROUND(col, 2), LEFT(col, 3), etc.
// For simple columns, it creates a function with the raw column name and arguments.
// For complex expressions, it wraps the entire expression as a subexpression.
func (e *Expression) wrapWithFunctionArgs(functionName string, args ...any) *Expression {
	// If the expression already has a function or is not a simple column, wrap it as a subexpression
	if e.function != "" || len(e.subExpressions) > 0 {
		return &Expression{
			function:       functionName,
			subExpressions: []*Expression{e},
			functionArgs:   args,
			alias:          e.alias,
		}
	}
	// Simple column case
	return &Expression{
		raw:          e.raw,
		function:     functionName,
		functionArgs: args,
		alias:        e.alias,
	}
}

// applyCondition creates a new Expression with a condition applied.
// This preserves the function, functionArgs, and subExpressions from the original expression.
// Used by comparison operators like Eq, Gt, Lt, In, IsNull, Like, etc.
// Accepts zero or more parameters (use zero for IS NULL, IS NOT NULL).
func (e *Expression) applyCondition(condition string, parameters ...any) *Expression {
	expr := e.copy()
	expr.condition = condition
	expr.parameters = parameters
	// Clear direction and alias as they don't apply to conditions
	expr.direction = ""
	expr.alias = ""

	return expr
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
	return &Expression{raw: sql, isLiteral: true}
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

// Param creates a bind parameter expression.
// When rendered, it outputs "?" and the value is collected by collectParameters().
// Use this when you need to pass a value as a bind parameter in function calls.
//
// Example:
//
//	Param("2026-01")           // Renders as "?" with param value "2026-01"
//	Param(100)                 // Renders as "?" with param value 100
func Param(value any) *Expression {
	return &Expression{
		isParam:    true,
		paramValue: value,
	}
}

// toArg converts any value to an Expression suitable for use as a function argument.
// This is used internally by function helpers to handle mixed argument types:
//   - *Expression: used as-is
//   - string: converted to Param (bind parameter)
//   - other values: converted to Param (bind parameter)
//
// Use Col() explicitly if you need to reference a column by name.
// Use Lit() explicitly if you need to embed a literal value in SQL without binding.
func toArg(arg any) *Expression {
	switch v := arg.(type) {
	case *Expression:
		return v
	default:
		return Param(v)
	}
}

// buildFunc creates a function expression with the given name and arguments.
// Arguments are converted using toArg(), so:
//   - *Expression args are used as-is
//   - string args become bind parameters
//   - other values become bind parameters
//
// This is the recommended way to build function expressions that need bind parameters.
func buildFunc(name string, args ...any) *Expression {
	funcArgs := make([]*Expression, len(args))
	for i, arg := range args {
		funcArgs[i] = toArg(arg)
	}

	return &Expression{
		function: name,
		funcArgs: funcArgs,
	}
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

// Coalesce creates a COALESCE function expression.
// It accepts a list of arguments which can be *Expression, string (column name), or other values (literals).
// Returns a new Expression representing the COALESCE function call.
//
// Example:
//
//	Coalesce(Col("updated_at"), Col("created_at")) // COALESCE(`updated_at`, `created_at`)
//	Coalesce(Col("amount").Sum(), Lit(0))          // COALESCE(SUM(`amount`), 0)
//	Coalesce("nickname", "real_name")              // COALESCE(`nickname`, `real_name`)
func Coalesce(args ...any) *Expression {
	var subExprs []*Expression

	for _, arg := range args {
		switch v := arg.(type) {
		case *Expression:
			subExprs = append(subExprs, v)
		case string:
			subExprs = append(subExprs, Col(v))
		default:
			subExprs = append(subExprs, Lit(v))
		}
	}

	return &Expression{
		function:       "COALESCE",
		subExpressions: subExprs,
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

// As sets an alias for the expression in the SELECT clause.
// Returns a new Expression with the specified alias.
//
// Example:
//
//	Col("user_name").As("name")          // `user_name` AS name
//	Col("id").Count().As("total_users")  // COUNT(`id`) AS total_users
//	Lit("'constant'").As("value")        // 'constant' AS value
func (e *Expression) As(alias string) *Expression {
	expr := e.copy()
	expr.alias = alias

	return expr
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
	expr := e.copy()
	expr.direction = "ASC"

	return expr
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
	expr := e.copy()
	expr.direction = "DESC"

	return expr
}

// Eq creates an equality condition (column = value).
// Returns a new Expression representing the equality comparison.
//
// Example:
//
//	Col("status").Eq("active")  // `status` = ?
//	Col("age").Eq(21)           // `age` = ?
func (e *Expression) Eq(value any) *Expression {
	return e.applyCondition("=", value)
}

// NotEq creates a not-equal condition (column != value).
// Returns a new Expression representing the inequality comparison.
//
// Example:
//
//	Col("status").NotEq("deleted")  // `status` != ?
//	Col("role").NotEq("guest")      // `role` != ?
func (e *Expression) NotEq(value any) *Expression {
	return e.applyCondition("!=", value)
}

// Gt creates a greater-than condition (column > value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("age").Gt(18)      // `age` > ?
//	Col("price").Gt(100.0) // `price` > ?
func (e *Expression) Gt(value any) *Expression {
	return e.applyCondition(">", value)
}

// Gte creates a greater-than-or-equal condition (column >= value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("age").Gte(18)      // `age` >= ?
//	Col("score").Gte(70)    // `score` >= ?
func (e *Expression) Gte(value any) *Expression {
	return e.applyCondition(">=", value)
}

// Lt creates a less-than condition (column < value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("age").Lt(65)      // `age` < ?
//	Col("stock").Lt(10)    // `stock` < ?
func (e *Expression) Lt(value any) *Expression {
	return e.applyCondition("<", value)
}

// Lte creates a less-than-or-equal condition (column <= value).
// Returns a new Expression representing the comparison.
//
// Example:
//
//	Col("price").Lte(100.0)  // `price` <= ?
//	Col("quantity").Lte(50)  // `quantity` <= ?
func (e *Expression) Lte(value any) *Expression {
	return e.applyCondition("<=", value)
}

// In creates an IN condition (column IN (values...)).
// Returns a new Expression that checks if the column value is in the provided list.
//
// Example:
//
//	Col("status").In("active", "pending", "approved")  // `status` IN (?, ?, ?)
//	Col("id").In(1, 2, 3, 4, 5)                        // `id` IN (?, ?, ?, ?, ?)
func (e *Expression) In(values ...any) *Expression {
	return e.applyCondition("IN", values...)
}

// NotIn creates a NOT IN condition (column NOT IN (values...)).
// Returns a new Expression that checks if the column value is not in the provided list.
//
// Example:
//
//	Col("status").NotIn("deleted", "archived")  // `status` NOT IN (?, ?)
//	Col("type").NotIn("spam", "bot")            // `type` NOT IN (?, ?)
func (e *Expression) NotIn(values ...any) *Expression {
	return e.applyCondition("NOT IN", values...)
}

// IsNull creates an IS NULL condition.
// Returns a new Expression that checks if the column value is NULL.
//
// Example:
//
//	Col("deleted_at").IsNull()  // `deleted_at` IS NULL
//	Col("parent_id").IsNull()   // `parent_id` IS NULL
func (e *Expression) IsNull() *Expression {
	return e.applyCondition("IS NULL")
}

// IsNotNull creates an IS NOT NULL condition.
// Returns a new Expression that checks if the column value is not NULL.
//
// Example:
//
//	Col("email").IsNotNull()     // `email` IS NOT NULL
//	Col("verified_at").IsNotNull() // `verified_at` IS NOT NULL
func (e *Expression) IsNotNull() *Expression {
	return e.applyCondition("IS NOT NULL")
}

// Like creates a LIKE condition for pattern matching.
// Returns a new Expression that performs pattern matching with the given pattern.
//
// Example:
//
//	Col("name").Like("%john%")  // `name` LIKE ?
//	Col("email").Like("admin@%") // `email` LIKE ?
func (e *Expression) Like(pattern string) *Expression {
	return e.applyCondition("LIKE", pattern)
}

// NotLike creates a NOT LIKE condition for negated pattern matching.
// Returns a new Expression that performs negated pattern matching with the given pattern.
//
// Example:
//
//	Col("name").NotLike("%test%")      // `name` NOT LIKE ?
//	Col("email").NotLike("%@spam.com") // `email` NOT LIKE ?
func (e *Expression) NotLike(pattern string) *Expression {
	return e.applyCondition("NOT LIKE", pattern)
}

// Between creates a BETWEEN condition (column BETWEEN min AND max).
// Returns a new Expression that checks if the column value is between two values (inclusive).
//
// Example:
//
//	Col("age").Between(18, 65)                    // `age` BETWEEN ? AND ?
//	Col("created_at").Between("2020-01-01", "2020-12-31") // `created_at` BETWEEN ? AND ?
//	Col("price").Between(10.0, 99.99)             // `price` BETWEEN ? AND ?
func (e *Expression) Between(min any, max any) *Expression {
	return e.applyCondition("BETWEEN", min, max)
}

// NotBetween creates a NOT BETWEEN condition (column NOT BETWEEN min AND max).
// Returns a new Expression that checks if the column value is not between two values.
//
// Example:
//
//	Col("age").NotBetween(0, 17)                  // `age` NOT BETWEEN ? AND ?
//	Col("price").NotBetween(100, 1000)            // `price` NOT BETWEEN ? AND ?
func (e *Expression) NotBetween(min any, max any) *Expression {
	return e.applyCondition("NOT BETWEEN", min, max)
}

// toSQL converts the expression to a SQL fragment for SELECT, GROUP BY, or ORDER BY clauses.
// It handles column quoting, function wrapping, aliases, and direction modifiers.
// For expressions with conditions (like Eq, Gt), it delegates to toConditionSQL.
func (e *Expression) toSQL(quote string) string {
	// If expression has a condition or operator, delegate to toConditionSQL to get the full condition
	// This handles cases like IF(col = ?, ...) where the condition needs to be rendered
	if e.condition != "" || e.operator != "" {
		sql := e.toConditionSQL(quote)
		// Add alias if present (toConditionSQL doesn't add aliases)
		if e.alias != "" {
			sql = fmt.Sprintf("%s AS %s", sql, e.alias)
		}

		return sql
	}

	return e.toBaseSQL(quote)
}

// toBaseSQL converts the expression to a SQL fragment without considering conditions.
// This is used internally to avoid infinite recursion between toSQL and toConditionSQL.
func (e *Expression) toBaseSQL(quote string) string {
	var sql string

	// Handle bind parameter expressions
	if e.isParam {
		sql = "?"
	} else if e.isLiteral {
		sql = e.raw // Don't quote literal values
	} else if e.raw != "" {
		sql = quoteIdentifier(e.raw, quote)
	}

	if e.function != "" {
		// Handle functions with funcArgs (new parameter-aware style)
		if len(e.funcArgs) > 0 {
			argStrs := funk.Map(e.funcArgs, func(arg *Expression) string {
				return arg.toSQL(quote)
			})
			sql = fmt.Sprintf("%s(%s)", e.function, strings.Join(argStrs, ", "))
		} else if len(e.subExpressions) > 0 {
			// Handle functions with subExpressions (like CONCAT, CONCAT_WS, COALESCE, CAST)
			argStrs := funk.Map(e.subExpressions, func(subExpr *Expression) string {
				return subExpr.toSQL(quote)
			})
			// CAST uses space separator: CAST(expr AS type)
			separator := ", "
			if e.function == "CAST" {
				separator = " "
			}
			sql = fmt.Sprintf("%s(%s)", e.function, strings.Join(argStrs, separator))
		} else if e.function == "LOCATE" && len(e.functionArgs) > 0 {
			// LOCATE has reversed argument order: LOCATE(substr, str) - legacy handling
			sql = fmt.Sprintf("LOCATE(%v, %s)", e.functionArgs[0], sql)
		} else {
			// Build function arguments normally (legacy inline style)
			args := sql
			if len(e.functionArgs) > 0 {
				argStrs := append([]string{sql}, funk.Map(e.functionArgs, func(arg any) string {
					return fmt.Sprintf("%v", arg)
				})...)
				args = strings.Join(argStrs, ", ")
			}
			sql = fmt.Sprintf("%s(%s)", e.function, args)
		}
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
func (e *Expression) toConditionSQL(quote string) string {
	// Handle composite expressions (AND, OR, NOT)
	if e.operator != "" {
		return e.toCompositeConditionSQL(quote)
	}

	// Handle simple conditions
	if e.condition == "" {
		return e.toBaseSQL(quote) // Use toBaseSQL() to handle functions without conditions
	}

	// Get the column expression (may include function) using toBaseSQL to avoid infinite recursion
	colExpr := e.toBaseSQL(quote)

	if e.condition == "IS NULL" || e.condition == "IS NOT NULL" {
		return fmt.Sprintf("%s %s", colExpr, e.condition)
	}

	if e.condition == "IN" || e.condition == "NOT IN" {
		placeholders := make([]string, len(e.parameters))
		for i := range placeholders {
			placeholders[i] = "?"
		}

		return fmt.Sprintf("%s %s (%s)", colExpr, e.condition, strings.Join(placeholders, ", "))
	}

	if e.condition == "BETWEEN" || e.condition == "NOT BETWEEN" {
		return fmt.Sprintf("%s %s ? AND ?", colExpr, e.condition)
	}

	return fmt.Sprintf("%s %s ?", colExpr, e.condition)
}

// toCompositeConditionSQL handles composite expressions (AND, OR, NOT).
// It recursively processes sub-expressions and combines them with the appropriate operator.
func (e *Expression) toCompositeConditionSQL(quote string) string {
	if e.operator == "NOT" {
		if len(e.subExpressions) > 0 {
			return fmt.Sprintf("NOT (%s)", e.subExpressions[0].toConditionSQL(quote))
		}

		return ""
	}

	// AND, OR
	if len(e.subExpressions) == 0 {
		return ""
	}

	parts := funk.Map(e.subExpressions, func(expr *Expression) string {
		return expr.toConditionSQL(quote)
	})

	return fmt.Sprintf("(%s)", strings.Join(parts, fmt.Sprintf(" %s ", e.operator)))
}

// collectParameters recursively collects all parameters from the expression tree.
// For composite expressions (AND, OR, NOT), it collects parameters from all sub-expressions.
// For function expressions with funcArgs, it collects parameters from all arguments.
// For bind parameter expressions (isParam), it returns the param value.
// For simple expressions with conditions, it returns the expression's own parameters.
func (e *Expression) collectParameters() []any {
	var params []any

	// Handle bind parameter expressions
	if e.isParam {
		return []any{e.paramValue}
	}

	// If this is a composite expression (AND, OR, NOT), recursively collect from sub-expressions
	if e.operator != "" {
		for _, subExpr := range e.subExpressions {
			params = append(params, subExpr.collectParameters()...)
		}

		return params
	}

	// Handle function expressions with funcArgs (new parameter-aware style)
	if len(e.funcArgs) > 0 {
		for _, arg := range e.funcArgs {
			params = append(params, arg.collectParameters()...)
		}
	}

	// Handle function expressions with subExpressions (like CONCAT, COALESCE)
	if e.function != "" && len(e.subExpressions) > 0 {
		for _, subExpr := range e.subExpressions {
			params = append(params, subExpr.collectParameters()...)
		}
	}

	// Add condition parameters (for Eq, Gt, In, etc.)
	params = append(params, e.parameters...)

	return params
}
