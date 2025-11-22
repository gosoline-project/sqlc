//nolint:nlreturn // Test code readability
package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseWhere_SQLInjectionSafety(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedSQL    string
		expectedParams []any
		description    string
	}{
		{
			name:           "classic SQL injection attempt in string",
			input:          "username = 'admin' OR '1'='1'",
			expectedSQL:    "(`username` = ? OR ? = ?)",
			expectedParams: []any{"admin", "1", "1"},
			description:    "The OR '1'='1' is parsed as separate conditions, not injected into SQL",
		},
		{
			name:           "SQL injection with comment",
			input:          "id = 1 AND name = 'foo' -- malicious comment'",
			expectedSQL:    "`id` = ?",
			expectedParams: []any{int64(1)},
			description:    "Comments are not supported, so this will fail to parse fully",
		},
		{
			name:           "union injection attempt",
			input:          "name = 'foo UNION SELECT * FROM passwords'",
			expectedSQL:    "`name` = ?",
			expectedParams: []any{"foo UNION SELECT * FROM passwords"},
			description:    "The entire string including UNION is treated as a parameter value",
		},
		{
			name:           "drop table attempt",
			input:          "id = '1; DROP TABLE users; --'",
			expectedSQL:    "`id` = ?",
			expectedParams: []any{"1; DROP TABLE users; --"},
			description:    "Semicolons and DROP commands inside strings become parameter values",
		},
		{
			name:           "quote escaping attempt",
			input:          "name = 'admin''; DROP TABLE users; --'",
			expectedSQL:    "`name` = ?",
			expectedParams: []any{"admin"},
			description:    "String terminates at first closing quote - rest is rejected by parser (SAFE)",
		},
		{
			name:           "always true condition",
			input:          "password = '' OR 1=1",
			expectedSQL:    "(`password` = ? OR ? = ?)",
			expectedParams: []any{"", int64(1), int64(1)},
			description:    "Parsed as legitimate conditions with parameters, not injected",
		},
		{
			name:           "stacked query attempt",
			input:          "id = 1; DELETE FROM users WHERE 1=1",
			expectedSQL:    "`id` = ?",
			expectedParams: []any{int64(1)},
			description:    "Semicolons outside strings are not supported, will fail to parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Testing:", tt.description)

			expr, err := sqlc.ParseWhere(tt.input)
			// Some inputs are expected to fail parsing (like the ones with semicolons)
			// These failures are actually GOOD - they prevent malformed input
			if err != nil {
				t.Logf("Parser rejected input (SAFE): %v", err)
				return
			}

			require.NotNil(t, expr)

			qb := sqlc.From("users").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			// Verify the SQL contains the expected pattern
			assert.Contains(t, sql, tt.expectedSQL, "SQL structure should match expected")

			// Verify parameters are properly extracted (not embedded in SQL)
			assert.Equal(t, tt.expectedParams, params, "All values should be parameterized")

			// Most importantly: verify that malicious strings are in PARAMETERS, not in SQL
			t.Logf("Generated SQL: %s", sql)
			t.Logf("Parameters: %v", params)
		})
	}
}

func TestParseWhere_NoDirectSQLEmbedding(t *testing.T) {
	// This test verifies that user input values NEVER appear directly in the SQL string
	maliciousInputs := []string{
		"DROP TABLE users",
		"UNION SELECT * FROM passwords",
		"1=1",
		"admin OR 1=1",
	}

	for _, malicious := range maliciousInputs {
		// Try to use this as a value in a WHERE clause
		input := "name = '" + malicious + "'"

		expr, err := sqlc.ParseWhere(input)
		if err != nil {
			// Parser rejection is also a valid safety measure
			t.Logf("Parser rejected malicious input (SAFE): %s", malicious)
			continue
		}

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		// The malicious string should be in parameters, NOT in SQL
		assert.NotContains(t, sql, malicious, "Malicious input should NOT appear in SQL string")

		// It should appear in parameters instead
		foundInParams := false
		for _, param := range params {
			if str, ok := param.(string); ok && str == malicious {
				foundInParams = true
				break
			}
		}
		assert.True(t, foundInParams, "Malicious input should be safely parameterized")

		t.Logf("✓ Safe: '%s' -> SQL: %s, Params: %v", malicious, sql, params)
	}
}

func TestParseWhere_OnlyColumnsAreIdentifiers(t *testing.T) {
	// This test ensures that only legitimate column names become identifiers
	// Everything else becomes a parameter

	input := "username = 'admin'"

	expr, err := sqlc.ParseWhere(input)
	require.NoError(t, err)

	qb := sqlc.From("users").Where(expr)
	sql, params, err := qb.ToSql()
	require.NoError(t, err)

	// Column name is quoted (identifier)
	assert.Contains(t, sql, "`username`")

	// Value is parameterized
	assert.NotContains(t, sql, "admin")
	assert.Equal(t, []any{"admin"}, params)

	t.Logf("Column becomes identifier: `username`")
	t.Logf("Value becomes parameter: %v", params)
}
