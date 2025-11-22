package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
)

func TestParseWhere_FunctionLimitations(t *testing.T) {
	t.Run("Parser supports single-argument functions", func(t *testing.T) {
		// The parser now supports single-argument SQL functions

		// These will successfully parse:
		inputs := []string{
			"UPPER(name) = 'JOHN'",
			"LENGTH(description) > 100",
			"ABS(balance) > 0",
			"TRIM(email) = 'test@example.com'",
		}

		for _, input := range inputs {
			_, err := sqlc.ParseWhere(input)
			assert.NoError(t, err, "Parser should support single-arg functions: %s", input)
		}
	})

	t.Run("Parser does NOT support multi-argument functions", func(t *testing.T) {
		// Multi-argument functions require the programmatic API

		// These will fail to parse:
		inputs := []string{
			"SUBSTRING(code, 1, 3) = 'ABC'",
			"ROUND(price, 2) = 19.99",
			"CONCAT(first_name, last_name) = 'John Doe'",
			"LOCATE('x', name) > 0",
		}

		for _, input := range inputs {
			_, err := sqlc.ParseWhere(input)
			assert.Error(t, err, "Parser should reject multi-arg functions: %s", input)
		}
	})

	t.Run("Use programmatic API for multi-argument functions", func(t *testing.T) {
		// For queries with multi-argument functions, use the programmatic Expression API

		// Example: WHERE ROUND(price, 2) > 10
		qb := sqlc.From("products").
			Where(sqlc.Col("price").RoundN(2).Gt(10))

		sql, params, err := qb.ToSql()
		assert.NoError(t, err)
		// Function arguments are embedded in SQL, not parameterized
		assert.Contains(t, sql, "ROUND(`price`, 2)")
		assert.Equal(t, []any{10}, params)
	})

	t.Run("Parser focuses on WHERE conditions", func(t *testing.T) {
		// The parser successfully handles standard WHERE conditions:
		inputs := []struct {
			query string
			desc  string
		}{
			{"id = 1 AND name = 'foo'", "Basic AND"},
			{"status IN ('active', 'pending')", "IN clause"},
			{"email IS NOT NULL", "NULL check"},
			{"price > 100 OR discount > 0.5", "OR with comparisons"},
			{"UPPER(name) = 'JOHN'", "Single-arg function"},
			{"LENGTH(TRIM(email)) > 5", "Nested functions"},
		}

		for _, tt := range inputs {
			expr, err := sqlc.ParseWhere(tt.query)
			assert.NoError(t, err, "Should parse: %s", tt.desc)
			assert.NotNil(t, expr)
		}
	})
}

func TestParseWhere_DocumentedCapabilities(t *testing.T) {
	// Document what the parser DOES support well
	t.Run("Supported: All comparison operators", func(t *testing.T) {
		operators := []string{
			"id = 1",
			"age != 18",
			"price > 100",
			"score >= 70",
			"quantity < 50",
			"balance <= 1000",
		}

		for _, op := range operators {
			_, err := sqlc.ParseWhere(op)
			assert.NoError(t, err, "Should parse: %s", op)
		}
	})

	t.Run("Supported: Logical operators with proper precedence", func(t *testing.T) {
		cases := []string{
			"a = 1 AND b = 2",
			"x = 1 OR y = 2",
			"NOT deleted = 1",
			"(a = 1 OR b = 2) AND c = 3",
		}

		for _, c := range cases {
			_, err := sqlc.ParseWhere(c)
			assert.NoError(t, err, "Should parse: %s", c)
		}
	})

	t.Run("Supported: IN and NOT IN", func(t *testing.T) {
		cases := []string{
			"status IN ('active', 'pending')",
			"id IN (1, 2, 3)",
			"type NOT IN ('spam', 'bot')",
		}

		for _, c := range cases {
			_, err := sqlc.ParseWhere(c)
			assert.NoError(t, err, "Should parse: %s", c)
		}
	})

	t.Run("Supported: NULL checks and LIKE", func(t *testing.T) {
		cases := []string{
			"deleted_at IS NULL",
			"email IS NOT NULL",
			"name LIKE '%john%'",
		}

		for _, c := range cases {
			_, err := sqlc.ParseWhere(c)
			assert.NoError(t, err, "Should parse: %s", c)
		}
	})

	t.Run("Not Supported: Multi-argument and date/time functions", func(t *testing.T) {
		// These require the programmatic API
		unsupported := []string{
			"SUBSTRING(code, 1, 3) = 'ABC'",  // Multi-argument
			"ROUND(price, 2) = 19.99",        // Multi-argument
			"CONCAT(a, b) = 'test'",          // Multi-argument
			"YEAR(created_at) = 2024",        // Date/time (not implemented)
			"DATE(timestamp) = '2024-01-01'", // Date/time (not implemented)
		}

		for _, u := range unsupported {
			_, err := sqlc.ParseWhere(u)
			assert.Error(t, err, "Parser should not support: %s", u)
		}
	})

	t.Run("Supported: Single-argument functions", func(t *testing.T) {
		// These are now supported
		supported := []string{
			"UPPER(name) = 'JOHN'",
			"LENGTH(text) > 10",
			"ABS(balance) > 0",
			"TRIM(email) = 'test@example.com'",
			"CEIL(price) = 100",
		}

		for _, s := range supported {
			_, err := sqlc.ParseWhere(s)
			assert.NoError(t, err, "Parser should support: %s", s)
		}
	})
}
