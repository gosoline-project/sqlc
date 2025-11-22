package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpression_StringFunctionsBasic(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "Upper function",
			expr:     sqlc.Col("name").Upper(),
			expected: "UPPER(`name`)",
		},
		{
			name:     "Upper with alias",
			expr:     sqlc.Col("email").Upper().As("EMAIL"),
			expected: "UPPER(`email`) AS EMAIL",
		},
		{
			name:     "Lower function",
			expr:     sqlc.Col("name").Lower(),
			expected: "LOWER(`name`)",
		},
		{
			name:     "Lower with alias",
			expr:     sqlc.Col("EMAIL").Lower().As("email"),
			expected: "LOWER(`EMAIL`) AS email",
		},
		{
			name:     "Length function",
			expr:     sqlc.Col("name").Length(),
			expected: "LENGTH(`name`)",
		},
		{
			name:     "CharLength function",
			expr:     sqlc.Col("text").CharLength(),
			expected: "CHAR_LENGTH(`text`)",
		},
		{
			name:     "Trim function",
			expr:     sqlc.Col("name").Trim(),
			expected: "TRIM(`name`)",
		},
		{
			name:     "Ltrim function",
			expr:     sqlc.Col("text").Ltrim(),
			expected: "LTRIM(`text`)",
		},
		{
			name:     "Rtrim function",
			expr:     sqlc.Col("text").Rtrim(),
			expected: "RTRIM(`text`)",
		},
		{
			name:     "Reverse function",
			expr:     sqlc.Col("text").Reverse(),
			expected: "REVERSE(`text`)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := sqlc.From("test").Columns(tt.expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestExpression_StringFunctionsWithArgs(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "Substring with position and length",
			expr:     sqlc.Col("name").Substring(1, 3),
			expected: "SUBSTRING(`name`, 1, 3)",
		},
		{
			name:     "Left function",
			expr:     sqlc.Col("name").Left(3),
			expected: "LEFT(`name`, 3)",
		},
		{
			name:     "Right function",
			expr:     sqlc.Col("code").Right(4),
			expected: "RIGHT(`code`, 4)",
		},
		{
			name:     "Replace function",
			expr:     sqlc.Col("text").Replace("old", "new"),
			expected: "REPLACE(`text`, 'old', 'new')",
		},
		{
			name:     "Replace with spaces",
			expr:     sqlc.Col("name").Replace(" ", "_"),
			expected: "REPLACE(`name`, ' ', '_')",
		},
		{
			name:     "Repeat function",
			expr:     sqlc.Col("char").Repeat(5),
			expected: "REPEAT(`char`, 5)",
		},
		{
			name:     "Locate function",
			expr:     sqlc.Col("email").Locate("@"),
			expected: "LOCATE('@', `email`)",
		},
		{
			name:     "Lpad function",
			expr:     sqlc.Col("id").Lpad(5, "0"),
			expected: "LPAD(`id`, 5, '0')",
		},
		{
			name:     "Rpad function",
			expr:     sqlc.Col("code").Rpad(10, " "),
			expected: "RPAD(`code`, 10, ' ')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := sqlc.From("test").Columns(tt.expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestExpression_Concat(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "Concat two columns",
			expr:     sqlc.Concat(sqlc.Col("first_name"), sqlc.Col("last_name")),
			expected: "CONCAT(`first_name`, `last_name`)",
		},
		{
			name:     "Concat with literal",
			expr:     sqlc.Concat(sqlc.Col("first_name"), sqlc.Lit("' '"), sqlc.Col("last_name")).As("full_name"),
			expected: "CONCAT(`first_name`, ' ', `last_name`) AS full_name",
		},
		{
			name:     "Concat multiple columns",
			expr:     sqlc.Concat(sqlc.Col("city"), sqlc.Lit("', '"), sqlc.Col("state"), sqlc.Lit("' '"), sqlc.Col("zip")),
			expected: "CONCAT(`city`, ', ', `state`, ' ', `zip`)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := sqlc.From("test").Columns(tt.expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestExpression_ConcatWs(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "ConcatWs with space separator",
			expr:     sqlc.ConcatWs(sqlc.Lit("' '"), sqlc.Col("first_name"), sqlc.Col("last_name")),
			expected: "CONCAT_WS(' ', `first_name`, `last_name`)",
		},
		{
			name:     "ConcatWs with comma separator",
			expr:     sqlc.ConcatWs(sqlc.Lit("', '"), sqlc.Col("city"), sqlc.Col("state"), sqlc.Col("country")).As("location"),
			expected: "CONCAT_WS(', ', `city`, `state`, `country`) AS location",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := sqlc.From("test").Columns(tt.expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestExpression_StringFunctionsChaining(t *testing.T) {
	t.Run("Upper with alias", func(t *testing.T) {
		qb := sqlc.From("users").
			Columns(
				sqlc.Col("email").Upper().As("EMAIL_UPPER"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "UPPER(`email`) AS EMAIL_UPPER")
	})

	t.Run("Trim and Upper", func(t *testing.T) {
		// Note: Can't chain directly, need nested approach or use raw SQL
		qb := sqlc.From("users").
			Columns(
				sqlc.Col("name").Trim().As("trimmed"),
				sqlc.Col("name").Upper().As("upper"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "TRIM(`name`) AS trimmed")
		assert.Contains(t, sql, "UPPER(`name`) AS upper")
	})

	t.Run("Multiple string functions", func(t *testing.T) {
		qb := sqlc.From("products").
			Columns(
				sqlc.Col("name"),
				sqlc.Col("name").Upper().As("name_upper"),
				sqlc.Col("name").Lower().As("name_lower"),
				sqlc.Col("name").Length().As("name_length"),
				sqlc.Col("description").Left(50).As("excerpt"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "UPPER(`name`) AS name_upper")
		assert.Contains(t, sql, "LOWER(`name`) AS name_lower")
		assert.Contains(t, sql, "LENGTH(`name`) AS name_length")
		assert.Contains(t, sql, "LEFT(`description`, 50) AS excerpt")
	})
}

func TestExpression_StringFunctionsInWhere(t *testing.T) {
	t.Run("Case-insensitive search using LOWER", func(t *testing.T) {
		// Note: String functions in WHERE would need to be in WHERE clause
		// This test shows they can be used in SELECT
		qb := sqlc.From("users").
			Columns(
				sqlc.Col("name"),
				sqlc.Col("email").Lower().As("email_lower"),
			).
			Where(sqlc.Col("status").Eq("active"))

		sql, params, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "LOWER(`email`) AS email_lower")
		assert.Equal(t, []any{"active"}, params)
	})
}

func TestExpression_QuoteStringHelper(t *testing.T) {
	// Test that single quotes are properly escaped
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "Replace with single quote in string",
			expr:     sqlc.Col("text").Replace("'", "''"),
			expected: "REPLACE(`text`, '''', '''''')", // '' becomes '''' in SQL, '''' becomes ''''''''
		},
		{
			name:     "Lpad with quote",
			expr:     sqlc.Col("id").Lpad(5, "'"),
			expected: "LPAD(`id`, 5, '''')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := sqlc.From("test").Columns(tt.expr)
			sql, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestExpression_StringFunctionsCombined(t *testing.T) {
	t.Run("Build full address", func(t *testing.T) {
		qb := sqlc.From("addresses").
			Columns(
				sqlc.ConcatWs(
					sqlc.Lit("', '"),
					sqlc.Col("street"),
					sqlc.Col("city"),
					sqlc.Col("state"),
					sqlc.Col("zip"),
				).As("full_address"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "CONCAT_WS(', ', `street`, `city`, `state`, `zip`) AS full_address")
	})

	t.Run("Format name and email", func(t *testing.T) {
		qb := sqlc.From("users").
			Columns(
				sqlc.Concat(
					sqlc.Col("first_name").Upper(),
					sqlc.Lit("' '"),
					sqlc.Col("last_name").Upper(),
				).As("full_name_upper"),
				sqlc.Col("email").Lower().As("email_normalized"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		// Note: UPPER() wraps the column, then CONCAT uses it
		assert.Contains(t, sql, "CONCAT(UPPER(`first_name`), ' ', UPPER(`last_name`)) AS full_name_upper")
		assert.Contains(t, sql, "LOWER(`email`) AS email_normalized")
	})
}
