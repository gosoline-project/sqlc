package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpression_NumericFunctions(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "Abs function",
			expr:     sqlc.Col("balance").Abs(),
			expected: "ABS(`balance`)",
		},
		{
			name:     "Abs with alias",
			expr:     sqlc.Col("temperature").Abs().As("abs_temp"),
			expected: "ABS(`temperature`) AS abs_temp",
		},
		{
			name:     "Ceil function",
			expr:     sqlc.Col("price").Ceil(),
			expected: "CEIL(`price`)",
		},
		{
			name:     "Ceil with alias",
			expr:     sqlc.Col("value").Ceil().As("rounded_up"),
			expected: "CEIL(`value`) AS rounded_up",
		},
		{
			name:     "Floor function",
			expr:     sqlc.Col("price").Floor(),
			expected: "FLOOR(`price`)",
		},
		{
			name:     "Floor with alias",
			expr:     sqlc.Col("value").Floor().As("rounded_down"),
			expected: "FLOOR(`value`) AS rounded_down",
		},
		{
			name:     "Round function",
			expr:     sqlc.Col("price").Round(),
			expected: "ROUND(`price`)",
		},
		{
			name:     "Round with alias",
			expr:     sqlc.Col("amount").Round().As("rounded"),
			expected: "ROUND(`amount`) AS rounded",
		},
		{
			name:     "Sqrt function",
			expr:     sqlc.Col("area").Sqrt(),
			expected: "SQRT(`area`)",
		},
		{
			name:     "Sqrt with alias",
			expr:     sqlc.Col("value").Sqrt().As("square_root"),
			expected: "SQRT(`value`) AS square_root",
		},
		{
			name:     "Sign function",
			expr:     sqlc.Col("balance").Sign(),
			expected: "SIGN(`balance`)",
		},
		{
			name:     "Sign with alias",
			expr:     sqlc.Col("profit").Sign().As("direction"),
			expected: "SIGN(`profit`) AS direction",
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

func TestExpression_AggregateFunctions(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "GroupConcat function",
			expr:     sqlc.Col("name").GroupConcat(),
			expected: "GROUP_CONCAT(`name`)",
		},
		{
			name:     "GroupConcat with alias",
			expr:     sqlc.Col("tag").GroupConcat().As("all_tags"),
			expected: "GROUP_CONCAT(`tag`) AS all_tags",
		},
		{
			name:     "StdDev function",
			expr:     sqlc.Col("score").StdDev(),
			expected: "STDDEV(`score`)",
		},
		{
			name:     "StdDev with alias",
			expr:     sqlc.Col("value").StdDev().As("std_dev"),
			expected: "STDDEV(`value`) AS std_dev",
		},
		{
			name:     "Variance function",
			expr:     sqlc.Col("score").Variance(),
			expected: "VARIANCE(`score`)",
		},
		{
			name:     "Variance with alias",
			expr:     sqlc.Col("value").Variance().As("variance"),
			expected: "VARIANCE(`value`) AS variance",
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

func TestExpression_Rand(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "Rand function",
			expr:     sqlc.Rand(),
			expected: "RAND()",
		},
		{
			name:     "Rand with alias",
			expr:     sqlc.Rand().As("random"),
			expected: "RAND() AS random",
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

func TestExpression_ComplexNumericExpressions(t *testing.T) {
	// Test combining numeric functions with other operations
	t.Run("Abs in WHERE clause", func(t *testing.T) {
		// Note: Abs() is for SELECT, not WHERE. For WHERE use Literal()
		qb := sqlc.From("transactions").
			Columns(sqlc.Col("amount").Abs().As("abs_amount")).
			Where(sqlc.Col("amount").NotEq(0))

		sql, params, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "ABS(`amount`) AS abs_amount")
		assert.Contains(t, sql, "`amount` != ?")
		assert.Equal(t, []any{0}, params)
	})

	t.Run("Multiple aggregate functions", func(t *testing.T) {
		qb := sqlc.From("sales").
			Columns(
				sqlc.Col("amount").Sum().As("total"),
				sqlc.Col("amount").Avg().As("average"),
				sqlc.Col("amount").Min().As("minimum"),
				sqlc.Col("amount").Max().As("maximum"),
				sqlc.Col("amount").StdDev().As("std_dev"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "SUM(`amount`) AS total")
		assert.Contains(t, sql, "AVG(`amount`) AS average")
		assert.Contains(t, sql, "MIN(`amount`) AS minimum")
		assert.Contains(t, sql, "MAX(`amount`) AS maximum")
		assert.Contains(t, sql, "STDDEV(`amount`) AS std_dev")
	})

	t.Run("Round with GroupBy", func(t *testing.T) {
		qb := sqlc.From("prices").
			Columns(
				sqlc.Col("category"),
				sqlc.Col("price").Round().As("rounded_price"),
			).
			GroupBy(sqlc.Col("category"))

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "ROUND(`price`) AS rounded_price")
		assert.Contains(t, sql, "GROUP BY `category`")
	})
}

func TestExpression_RawSQLForComplexFunctions(t *testing.T) {
	// Test functions with multiple arguments using the proper API
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{
			name:     "POW with exponent",
			expr:     sqlc.Col("value").Pow(2).As("squared"),
			expected: "POW(`value`, 2) AS squared",
		},
		{
			name:     "POW with float exponent",
			expr:     sqlc.Col("value").Pow(0.5).As("sqrt"),
			expected: "POW(`value`, 0.5) AS sqrt",
		},
		{
			name:     "MOD with divisor",
			expr:     sqlc.Col("id").Mod(10).As("last_digit"),
			expected: "MOD(`id`, 10) AS last_digit",
		},
		{
			name:     "TRUNCATE with decimal places",
			expr:     sqlc.Col("price").Truncate(2).As("truncated"),
			expected: "TRUNCATE(`price`, 2) AS truncated",
		},
		{
			name:     "TRUNCATE to whole number",
			expr:     sqlc.Col("price").Truncate(0).As("whole"),
			expected: "TRUNCATE(`price`, 0) AS whole",
		},
		{
			name:     "ROUND with decimal places",
			expr:     sqlc.Col("price").RoundN(2).As("rounded"),
			expected: "ROUND(`price`, 2) AS rounded",
		},
		{
			name:     "ROUND without decimals",
			expr:     sqlc.Col("price").Round().As("rounded"),
			expected: "ROUND(`price`) AS rounded",
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

func TestExpression_ComplexNumericCombinations(t *testing.T) {
	t.Run("Multiple functions with args", func(t *testing.T) {
		qb := sqlc.From("calculations").
			Columns(
				sqlc.Col("value").Pow(2).As("squared"),
				sqlc.Col("value").Pow(3).As("cubed"),
				sqlc.Col("price").RoundN(2).As("price_rounded"),
				sqlc.Col("amount").Truncate(1).As("amount_truncated"),
				sqlc.Col("id").Mod(100).As("id_mod"),
			)

		sql, _, err := qb.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "POW(`value`, 2) AS squared")
		assert.Contains(t, sql, "POW(`value`, 3) AS cubed")
		assert.Contains(t, sql, "ROUND(`price`, 2) AS price_rounded")
		assert.Contains(t, sql, "TRUNCATE(`amount`, 1) AS amount_truncated")
		assert.Contains(t, sql, "MOD(`id`, 100) AS id_mod")
	})
}
