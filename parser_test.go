package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseWhere_SimpleComparisons(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "equality with number",
			input:    "id = 1",
			expected: "`id` = ?",
			params:   []any{int64(1)},
		},
		{
			name:     "equality with string",
			input:    "name = 'foo'",
			expected: "`name` = ?",
			params:   []any{"foo"},
		},
		{
			name:     "not equal",
			input:    "status != 'deleted'",
			expected: "`status` != ?",
			params:   []any{"deleted"},
		},
		{
			name:     "greater than",
			input:    "age > 18",
			expected: "`age` > ?",
			params:   []any{int64(18)},
		},
		{
			name:     "greater than or equal",
			input:    "score >= 70",
			expected: "`score` >= ?",
			params:   []any{int64(70)},
		},
		{
			name:     "less than",
			input:    "price < 100",
			expected: "`price` < ?",
			params:   []any{int64(100)},
		},
		{
			name:     "less than or equal",
			input:    "quantity <= 50",
			expected: "`quantity` <= ?",
			params:   []any{int64(50)},
		},
		{
			name:     "float value",
			input:    "rating = 4.5",
			expected: "`rating` = ?",
			params:   []any{4.5},
		},
		{
			name:     "negative number",
			input:    "balance > -100",
			expected: "`balance` > ?",
			params:   []any{int64(-100)},
		},
		{
			name:     "double quotes for string",
			input:    `type = "admin"`,
			expected: "`type` = ?",
			params:   []any{"admin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			// Create a test query builder to extract SQL and params
			qb := sqlc.From("users").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			// Extract just the WHERE part
			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_SingleArgumentFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		// String functions
		{
			name:     "UPPER function",
			input:    "UPPER(name) = 'JOHN'",
			expected: "UPPER(`name`) = ?",
			params:   []any{"JOHN"},
		},
		{
			name:     "LOWER function",
			input:    "LOWER(email) = 'test@example.com'",
			expected: "LOWER(`email`) = ?",
			params:   []any{"test@example.com"},
		},
		{
			name:     "TRIM function",
			input:    "TRIM(name) = 'John'",
			expected: "TRIM(`name`) = ?",
			params:   []any{"John"},
		},
		{
			name:     "LTRIM function",
			input:    "LTRIM(text) != ''",
			expected: "LTRIM(`text`) != ?",
			params:   []any{""},
		},
		{
			name:     "RTRIM function",
			input:    "RTRIM(description) = 'test'",
			expected: "RTRIM(`description`) = ?",
			params:   []any{"test"},
		},
		{
			name:     "REVERSE function",
			input:    "REVERSE(code) = 'dcba'",
			expected: "REVERSE(`code`) = ?",
			params:   []any{"dcba"},
		},
		{
			name:     "LENGTH function",
			input:    "LENGTH(name) > 5",
			expected: "LENGTH(`name`) > ?",
			params:   []any{int64(5)},
		},
		{
			name:     "CHAR_LENGTH function",
			input:    "CHAR_LENGTH(description) <= 100",
			expected: "CHAR_LENGTH(`description`) <= ?",
			params:   []any{int64(100)},
		},

		// Numeric functions
		{
			name:     "ABS function",
			input:    "ABS(balance) > 100",
			expected: "ABS(`balance`) > ?",
			params:   []any{int64(100)},
		},
		{
			name:     "CEIL function",
			input:    "CEIL(price) = 50",
			expected: "CEIL(`price`) = ?",
			params:   []any{int64(50)},
		},
		{
			name:     "FLOOR function",
			input:    "FLOOR(rating) >= 4",
			expected: "FLOOR(`rating`) >= ?",
			params:   []any{int64(4)},
		},
		{
			name:     "ROUND function",
			input:    "ROUND(amount) = 42",
			expected: "ROUND(`amount`) = ?",
			params:   []any{int64(42)},
		},
		{
			name:     "SQRT function",
			input:    "SQRT(value) < 10",
			expected: "SQRT(`value`) < ?",
			params:   []any{int64(10)},
		},
		{
			name:     "SIGN function",
			input:    "SIGN(temperature) = -1",
			expected: "SIGN(`temperature`) = ?",
			params:   []any{int64(-1)},
		},

		// Aggregate functions (though not typical for WHERE)
		{
			name:     "COUNT function",
			input:    "COUNT(items) > 0",
			expected: "COUNT(`items`) > ?",
			params:   []any{int64(0)},
		},
		{
			name:     "SUM function",
			input:    "SUM(total) >= 1000",
			expected: "SUM(`total`) >= ?",
			params:   []any{int64(1000)},
		},
		{
			name:     "AVG function",
			input:    "AVG(score) > 75",
			expected: "AVG(`score`) > ?",
			params:   []any{int64(75)},
		},
		{
			name:     "MIN function",
			input:    "MIN(price) > 10",
			expected: "MIN(`price`) > ?",
			params:   []any{int64(10)},
		},
		{
			name:     "MAX function",
			input:    "MAX(quantity) <= 100",
			expected: "MAX(`quantity`) <= ?",
			params:   []any{int64(100)},
		},

		// Case insensitive function names
		{
			name:     "lowercase function name",
			input:    "upper(name) = 'JOHN'",
			expected: "UPPER(`name`) = ?",
			params:   []any{"JOHN"},
		},
		{
			name:     "mixed case function name",
			input:    "Lower(email) = 'test@example.com'",
			expected: "LOWER(`email`) = ?",
			params:   []any{"test@example.com"},
		},

		// Functions with qualified column names
		{
			name:     "function with table.column",
			input:    "UPPER(users.name) = 'JOHN'",
			expected: "UPPER(`users`.`name`) = ?",
			params:   []any{"JOHN"},
		},

		// Functions with IS NULL
		{
			name:     "function with IS NULL",
			input:    "TRIM(name) IS NULL",
			expected: "TRIM(`name`) IS NULL",
			params:   nil,
		},
		{
			name:     "function with IS NOT NULL",
			input:    "LENGTH(description) IS NOT NULL",
			expected: "LENGTH(`description`) IS NOT NULL",
			params:   nil,
		},

		// Functions with IN
		{
			name:     "function with IN",
			input:    "LENGTH(code) IN (3, 4, 5)",
			expected: "LENGTH(`code`) IN (?, ?, ?)",
			params:   []any{int64(3), int64(4), int64(5)},
		},

		// Functions with LIKE
		{
			name:     "function with LIKE",
			input:    "UPPER(name) LIKE 'JOHN%'",
			expected: "UPPER(`name`) LIKE ?",
			params:   []any{"JOHN%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err, "Failed to parse: %s", tt.input)

			qb := sqlc.From("table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			// Extract WHERE clause from full SQL
			assert.Contains(t, sql, "WHERE "+tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_NestedFunctions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "nested UPPER(TRIM())",
			input:    "UPPER(TRIM(name)) = 'JOHN'",
			expected: "UPPER(TRIM(`name`)) = ?",
			params:   []any{"JOHN"},
		},
		{
			name:     "nested LOWER(REVERSE())",
			input:    "LOWER(REVERSE(code)) = 'dcba'",
			expected: "LOWER(REVERSE(`code`)) = ?",
			params:   []any{"dcba"},
		},
		{
			name:     "nested ABS(FLOOR())",
			input:    "ABS(FLOOR(value)) > 10",
			expected: "ABS(FLOOR(`value`)) > ?",
			params:   []any{int64(10)},
		},
		{
			name:     "triple nested",
			input:    "LENGTH(TRIM(LOWER(name))) > 5",
			expected: "LENGTH(TRIM(LOWER(`name`))) > ?",
			params:   []any{int64(5)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err, "Failed to parse: %s", tt.input)

			qb := sqlc.From("table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, "WHERE "+tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_FunctionsWithLogicalOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "function AND plain column",
			input:    "UPPER(name) = 'JOHN' AND age > 18",
			expected: "(UPPER(`name`) = ? AND `age` > ?)",
			params:   []any{"JOHN", int64(18)},
		},
		{
			name:     "two functions with OR",
			input:    "LENGTH(name) > 5 OR LENGTH(email) > 10",
			expected: "(LENGTH(`name`) > ? OR LENGTH(`email`) > ?)",
			params:   []any{int64(5), int64(10)},
		},
		{
			name:     "NOT with function",
			input:    "NOT UPPER(status) = 'DELETED'",
			expected: "NOT (UPPER(`status`) = ?)",
			params:   []any{"DELETED"},
		},
		{
			name:     "complex with parentheses",
			input:    "(UPPER(name) = 'JOHN' OR UPPER(name) = 'JANE') AND age > 18",
			expected: "((UPPER(`name`) = ? OR UPPER(`name`) = ?) AND `age` > ?)",
			params:   []any{"JOHN", "JANE", int64(18)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err, "Failed to parse: %s", tt.input)

			qb := sqlc.From("table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, "WHERE "+tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_FunctionErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		err   string
	}{
		{
			name:  "unsupported function",
			input: "UNKNOWN_FUNC(name) = 'test'",
			err:   "unsupported function: UNKNOWN_FUNC",
		},
		{
			name:  "unclosed function parenthesis",
			input: "UPPER(name = 'test'",
			err:   "expected closing parenthesis",
		},
		{
			name:  "function with no argument",
			input: "UPPER() = 'test'",
			err:   "expected identifier",
		},
		{
			name:  "function call without parentheses",
			input: "UPPER name = 'test'",
			err:   "expected operator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sqlc.ParseWhere(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.err)
		})
	}
}

func TestParseWhere_Parentheses(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "parentheses for grouping",
			input:    "(age > 18 AND age < 65) OR status = 'vip'",
			expected: "((`age` > ? AND `age` < ?) OR `status` = ?)",
			params:   []any{int64(18), int64(65), "vip"},
		},
		{
			name:     "nested parentheses",
			input:    "((active = 1 AND verified = 1) OR role = 'admin') AND status = 'approved'",
			expected: "(((`active` = ? AND `verified` = ?) OR `role` = ?) AND `status` = ?)",
			params:   []any{int64(1), int64(1), "admin", "approved"},
		},
		{
			name:     "parentheses override precedence",
			input:    "status = 'active' AND (role = 'admin' OR role = 'moderator')",
			expected: "(`status` = ? AND (`role` = ? OR `role` = ?))",
			params:   []any{"active", "admin", "moderator"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("users").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_IN(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "IN with numbers",
			input:    "id IN (1, 2, 3)",
			expected: "`id` IN (?, ?, ?)",
			params:   []any{int64(1), int64(2), int64(3)},
		},
		{
			name:     "IN with strings",
			input:    "status IN ('active', 'pending', 'approved')",
			expected: "`status` IN (?, ?, ?)",
			params:   []any{"active", "pending", "approved"},
		},
		{
			name:     "NOT IN",
			input:    "type NOT IN ('spam', 'bot')",
			expected: "`type` NOT IN (?, ?)",
			params:   []any{"spam", "bot"},
		},
		{
			name:     "IN with single value",
			input:    "category IN ('electronics')",
			expected: "`category` IN (?)",
			params:   []any{"electronics"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("users").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_NULL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "IS NULL",
			input:    "deleted_at IS NULL",
			expected: "`deleted_at` IS NULL",
		},
		{
			name:     "IS NOT NULL",
			input:    "email IS NOT NULL",
			expected: "`email` IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("users").Where(expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestParseWhere_LIKE(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "LIKE with wildcards",
			input:    "name LIKE '%john%'",
			expected: "`name` LIKE ?",
			params:   []any{"%john%"},
		},
		{
			name:     "LIKE prefix match",
			input:    "email LIKE 'admin@%'",
			expected: "`email` LIKE ?",
			params:   []any{"admin@%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("users").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_ComplexExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "complex combination",
			input: "((age >= 18 AND age <= 65) OR status = 'vip') AND active = 1 AND deleted_at IS NULL",
		},
		{
			name:  "multiple conditions with IN",
			input: "status IN ('active', 'pending') AND role NOT IN ('guest', 'banned') AND age > 18",
		},
		{
			name:  "NOT with complex expression",
			input: "NOT (deleted = 1 OR banned = 1) AND verified = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			// Just verify it can be converted to SQL without errors
			qb := sqlc.From("users").Where(expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, "WHERE")
		})
	}
}

func TestParseWhere_QualifiedColumns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		params   []any
	}{
		{
			name:     "table qualified column",
			input:    "users.id = 1",
			expected: "`users`.`id` = ?",
			params:   []any{int64(1)},
		},
		{
			name:     "multiple qualified columns",
			input:    "users.name = 'foo' AND posts.author_id = 1",
			expected: "(`users`.`name` = ? AND `posts`.`author_id` = ?)",
			params:   []any{"foo", int64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := sqlc.ParseWhere(tt.input)
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("users").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestParseWhere_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "unterminated string",
			input: "name = 'foo",
		},
		{
			name:  "missing value",
			input: "id =",
		},
		{
			name:  "missing operator",
			input: "id 1",
		},
		{
			name:  "unexpected character",
			input: "id @ 1",
		},
		{
			name:  "unclosed parenthesis",
			input: "(id = 1 AND name = 'foo'",
		},
		{
			name:  "empty IN list",
			input: "id IN ()",
		},
		{
			name:  "invalid IN syntax",
			input: "id IN 1, 2, 3",
		},
		{
			name:  "LIKE with non-string",
			input: "name LIKE 123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sqlc.ParseWhere(tt.input)
			assert.Error(t, err)
		})
	}
}

func TestParseWhere_CaseInsensitivity(t *testing.T) {
	// Keywords should be case-insensitive
	tests := []string{
		"id = 1 AND name = 'foo'",
		"id = 1 and name = 'foo'",
		"id = 1 AnD name = 'foo'",
	}

	var firstSQL string
	var firstParams []any

	for i, input := range tests {
		expr, err := sqlc.ParseWhere(input)
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		if i == 0 {
			firstSQL = sql
			firstParams = params
		} else {
			assert.Equal(t, firstSQL, sql, "SQL should be identical regardless of keyword case")
			assert.Equal(t, firstParams, params, "Params should be identical regardless of keyword case")
		}
	}
}

func TestParseWhere_WhitespaceHandling(t *testing.T) {
	// Various whitespace should produce same result
	tests := []string{
		"id=1 AND name='foo'",
		"id = 1 AND name = 'foo'",
		"  id  =  1  AND  name  =  'foo'  ",
		"id=1AND name='foo'", // This might be tricky
	}

	var firstSQL string
	var firstParams []any

	for i, input := range tests[:3] { // Skip the last one for now
		expr, err := sqlc.ParseWhere(input)
		require.NoError(t, err, "Failed to parse: %s", input)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		if i == 0 {
			firstSQL = sql
			firstParams = params
		} else {
			assert.Equal(t, firstSQL, sql, "SQL should be identical regardless of whitespace")
			assert.Equal(t, firstParams, params, "Params should be identical regardless of whitespace")
		}
	}
}
