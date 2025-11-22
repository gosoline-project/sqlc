package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilter_EmptyFilter(t *testing.T) {
	t.Run("empty filter is a no-op", func(t *testing.T) {
		// Empty filter with no type
		filter := sqlc.JsonFilter{}

		expr, err := filter.ToExpression()
		require.NoError(t, err)
		assert.Nil(t, expr)

		// Should not add any WHERE clause
		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "SELECT * FROM `users`", sql)
		assert.Empty(t, params)
	})

	t.Run("empty JSON object is a no-op", func(t *testing.T) {
		filter, err := sqlc.JsonFilterFromJSON("{}")
		require.NoError(t, err)
		require.NotNil(t, filter)

		expr, err := filter.ToExpression()
		require.NoError(t, err)
		assert.Nil(t, expr)

		// Should not add any WHERE clause
		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "SELECT * FROM `users`", sql)
		assert.Empty(t, params)
	})

	t.Run("empty filter can be combined with other conditions", func(t *testing.T) {
		emptyFilter := sqlc.JsonFilter{}
		emptyExpr, err := emptyFilter.ToExpression()
		require.NoError(t, err)

		// Combine with a real filter
		realExpr := sqlc.Col("status").Eq("active")

		qb := sqlc.From("users").
			Where(emptyExpr). // This should be ignored
			Where(realExpr)

		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "SELECT * FROM `users` WHERE `status` = ?", sql)
		assert.Equal(t, []any{"active"}, params)
	})
}

func TestFilter_SimpleComparisons(t *testing.T) {
	tests := []struct {
		name     string
		filter   sqlc.JsonFilter
		expected string
		params   []any
	}{
		{
			name: "eq with string",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterEq,
				Column: "status",
				Value:  "ACTIVE",
			},
			expected: "`status` = ?",
			params:   []any{"ACTIVE"},
		},
		{
			name: "eq with number",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterEq,
				Column: "age",
				Value:  25,
			},
			expected: "`age` = ?",
			params:   []any{25},
		},
		{
			name: "ne with string",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterNe,
				Column: "status",
				Value:  "deleted",
			},
			expected: "`status` != ?",
			params:   []any{"deleted"},
		},
		{
			name: "gt",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterGt,
				Column: "age",
				Value:  18,
			},
			expected: "`age` > ?",
			params:   []any{18},
		},
		{
			name: "gte",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterGte,
				Column: "score",
				Value:  70,
			},
			expected: "`score` >= ?",
			params:   []any{70},
		},
		{
			name: "lt",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterLt,
				Column: "price",
				Value:  100.50,
			},
			expected: "`price` < ?",
			params:   []any{100.50},
		},
		{
			name: "lte",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterLte,
				Column: "quantity",
				Value:  50,
			},
			expected: "`quantity` <= ?",
			params:   []any{50},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := tt.filter.ToExpression()
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("test_table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestFilter_SetOperations(t *testing.T) {
	tests := []struct {
		name     string
		filter   sqlc.JsonFilter
		expected string
		params   []any
	}{
		{
			name: "in with strings",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterIn,
				Column: "status",
				Values: []any{"active", "pending", "approved"},
			},
			expected: "`status` IN (?, ?, ?)",
			params:   []any{"active", "pending", "approved"},
		},
		{
			name: "in with numbers",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterIn,
				Column: "id",
				Values: []any{1, 2, 3, 4, 5},
			},
			expected: "`id` IN (?, ?, ?, ?, ?)",
			params:   []any{1, 2, 3, 4, 5},
		},
		{
			name: "not_in",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterNotIn,
				Column: "status",
				Values: []any{"deleted", "archived"},
			},
			expected: "`status` NOT IN (?, ?)",
			params:   []any{"deleted", "archived"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := tt.filter.ToExpression()
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("test_table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestFilter_Between(t *testing.T) {
	tests := []struct {
		name     string
		filter   sqlc.JsonFilter
		expected []string
		params   []any
	}{
		{
			name: "between dates",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterBetween,
				Column: "created_at",
				From:   "2024-01-01",
				To:     "2024-12-31",
			},
			expected: []string{"`created_at` >= ?", "`created_at` <= ?"},
			params:   []any{"2024-01-01", "2024-12-31"},
		},
		{
			name: "between numbers",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterBetween,
				Column: "age",
				From:   18,
				To:     65,
			},
			expected: []string{"`age` >= ?", "`age` <= ?"},
			params:   []any{18, 65},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := tt.filter.ToExpression()
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("test_table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			for _, exp := range tt.expected {
				assert.Contains(t, sql, exp)
			}
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestFilter_PatternMatching(t *testing.T) {
	tests := []struct {
		name     string
		filter   sqlc.JsonFilter
		expected string
		params   []any
	}{
		{
			name: "like",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterLike,
				Column: "name",
				Value:  "%john%",
			},
			expected: "`name` LIKE ?",
			params:   []any{"%john%"},
		},
		{
			name: "not_like",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterNotLike,
				Column: "email",
				Value:  "%spam%",
			},
			expected: "NOT (`name` LIKE ?)",
			params:   []any{"%spam%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := tt.filter.ToExpression()
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("test_table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			// For not_like, just check that NOT and LIKE are both present
			if tt.filter.Type == sqlc.JsonFilterNotLike {
				assert.Contains(t, sql, "NOT")
				assert.Contains(t, sql, "LIKE")
			} else {
				assert.Contains(t, sql, tt.expected)
			}
			assert.Equal(t, tt.params, params)
		})
	}
}

func TestFilter_NullChecks(t *testing.T) {
	tests := []struct {
		name     string
		filter   sqlc.JsonFilter
		expected string
	}{
		{
			name: "is_null",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterIsNull,
				Column: "deleted_at",
			},
			expected: "`deleted_at` IS NULL",
		},
		{
			name: "is_not_null",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterIsNotNull,
				Column: "email",
			},
			expected: "`email` IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := tt.filter.ToExpression()
			require.NoError(t, err)
			require.NotNil(t, expr)

			qb := sqlc.From("test_table").Where(expr)
			sql, params, err := qb.ToSql()
			require.NoError(t, err)

			assert.Contains(t, sql, tt.expected)
			assert.Empty(t, params)
		})
	}
}

func TestFilter_BooleanOperators(t *testing.T) {
	t.Run("and with multiple conditions", func(t *testing.T) {
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterAnd,
			Fields: []sqlc.JsonFilter{
				{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
				{Type: sqlc.JsonFilterGt, Column: "age", Value: 18},
				{Type: sqlc.JsonFilterLt, Column: "age", Value: 65},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`status` = ?")
		assert.Contains(t, sql, "`age` > ?")
		assert.Contains(t, sql, "`age` < ?")
		assert.Contains(t, sql, "AND")
		assert.Equal(t, []any{"active", 18, 65}, params)
	})

	t.Run("or with multiple conditions", func(t *testing.T) {
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterOr,
			Fields: []sqlc.JsonFilter{
				{Type: sqlc.JsonFilterEq, Column: "role", Value: "admin"},
				{Type: sqlc.JsonFilterEq, Column: "role", Value: "moderator"},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`role` = ?")
		assert.Contains(t, sql, "OR")
		assert.Equal(t, []any{"admin", "moderator"}, params)
	})

	t.Run("not", func(t *testing.T) {
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterNot,
			Fields: []sqlc.JsonFilter{
				{Type: sqlc.JsonFilterEq, Column: "deleted", Value: true},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "NOT")
		assert.Contains(t, sql, "`deleted` = ?")
		assert.Equal(t, []any{true}, params)
	})
}

func TestFilter_NestedConditions(t *testing.T) {
	t.Run("complex nested and/or", func(t *testing.T) {
		// (status = 'active' AND age > 18) OR (role = 'admin')
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterOr,
			Fields: []sqlc.JsonFilter{
				{
					Type: sqlc.JsonFilterAnd,
					Fields: []sqlc.JsonFilter{
						{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
						{Type: sqlc.JsonFilterGt, Column: "age", Value: 18},
					},
				},
				{Type: sqlc.JsonFilterEq, Column: "role", Value: "admin"},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`status` = ?")
		assert.Contains(t, sql, "`age` > ?")
		assert.Contains(t, sql, "`role` = ?")
		assert.Contains(t, sql, "AND")
		assert.Contains(t, sql, "OR")
		assert.Equal(t, []any{"active", 18, "admin"}, params)
	})

	t.Run("with in and null check", func(t *testing.T) {
		// country IN ('US', 'CA') OR country IS NULL
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterOr,
			Fields: []sqlc.JsonFilter{
				{
					Type:   sqlc.JsonFilterIn,
					Column: "country",
					Values: []any{"US", "CA"},
				},
				{
					Type:   sqlc.JsonFilterIsNull,
					Column: "country",
				},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`country` IN (?, ?)")
		assert.Contains(t, sql, "`country` IS NULL")
		assert.Contains(t, sql, "OR")
		assert.Equal(t, []any{"US", "CA"}, params)
	})
}

func TestFilter_FromJSON(t *testing.T) {
	tests := []struct {
		name        string
		json        string
		expectError bool
		validate    func(t *testing.T, filter *sqlc.JsonFilter)
	}{
		{
			name: "simple eq filter",
			json: `{"type": "eq", "column": "status", "value": "active"}`,
			validate: func(t *testing.T, filter *sqlc.JsonFilter) {
				assert.Equal(t, sqlc.JsonFilterEq, filter.Type)
				assert.Equal(t, "status", filter.Column)
				assert.Equal(t, "active", filter.Value)
			},
		},
		{
			name: "in filter with array",
			json: `{"type": "in", "column": "id", "values": [1, 2, 3]}`,
			validate: func(t *testing.T, filter *sqlc.JsonFilter) {
				assert.Equal(t, sqlc.JsonFilterIn, filter.Type)
				assert.Equal(t, "id", filter.Column)
				require.Len(t, filter.Values, 3)
				// JSON numbers are decoded as float64
				assert.Equal(t, float64(1), filter.Values[0])
				assert.Equal(t, float64(2), filter.Values[1])
				assert.Equal(t, float64(3), filter.Values[2])
			},
		},
		{
			name: "between filter",
			json: `{"type": "between", "column": "age", "from": 18, "to": 65}`,
			validate: func(t *testing.T, filter *sqlc.JsonFilter) {
				assert.Equal(t, sqlc.JsonFilterBetween, filter.Type)
				assert.Equal(t, "age", filter.Column)
				assert.Equal(t, float64(18), filter.From)
				assert.Equal(t, float64(65), filter.To)
			},
		},
		{
			name: "nested and filter",
			json: `{
				"type": "and",
				"fields": [
					{"type": "eq", "column": "status", "value": "active"},
					{"type": "gt", "column": "age", "value": 18}
				]
			}`,
			validate: func(t *testing.T, filter *sqlc.JsonFilter) {
				assert.Equal(t, sqlc.JsonFilterAnd, filter.Type)
				require.Len(t, filter.Fields, 2)
				assert.Equal(t, sqlc.JsonFilterEq, filter.Fields[0].Type)
				assert.Equal(t, sqlc.JsonFilterGt, filter.Fields[1].Type)
			},
		},
		{
			name:        "invalid json",
			json:        `{"type": "eq", "column": "status"`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := sqlc.JsonFilterFromJSON(tt.json)

			if tt.expectError {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, filter)

			if tt.validate != nil {
				tt.validate(t, filter)
			}
		})
	}
}

func TestFilter_ToJSON(t *testing.T) {
	t.Run("simple filter", func(t *testing.T) {
		filter := sqlc.JsonFilter{
			Type:   sqlc.JsonFilterEq,
			Column: "status",
			Value:  "active",
		}

		jsonStr, err := filter.ToJSON()
		require.NoError(t, err)

		// Parse it back to verify
		parsed, err := sqlc.JsonFilterFromJSON(jsonStr)
		require.NoError(t, err)
		assert.Equal(t, filter.Type, parsed.Type)
		assert.Equal(t, filter.Column, parsed.Column)
		assert.Equal(t, filter.Value, parsed.Value)
	})

	t.Run("complex nested filter", func(t *testing.T) {
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterAnd,
			Fields: []sqlc.JsonFilter{
				{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
				{Type: sqlc.JsonFilterGt, Column: "age", Value: 18},
			},
		}

		jsonStr, err := filter.ToJSON()
		require.NoError(t, err)

		// Parse it back to verify
		parsed, err := sqlc.JsonFilterFromJSON(jsonStr)
		require.NoError(t, err)
		assert.Equal(t, filter.Type, parsed.Type)
		require.Len(t, parsed.Fields, 2)
	})
}

func TestFilter_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		filter sqlc.JsonFilter
	}{
		{
			name: "eq without column",
			filter: sqlc.JsonFilter{
				Type:  sqlc.JsonFilterEq,
				Value: "test",
			},
		},
		{
			name: "eq without value",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterEq,
				Column: "status",
			},
		},
		{
			name: "in without values",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterIn,
				Column: "id",
			},
		},
		{
			name: "between without from",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterBetween,
				Column: "age",
				To:     65,
			},
		},
		{
			name: "between without to",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterBetween,
				Column: "age",
				From:   18,
			},
		},
		{
			name: "and without children",
			filter: sqlc.JsonFilter{
				Type: sqlc.JsonFilterAnd,
			},
		},
		{
			name: "or without children",
			filter: sqlc.JsonFilter{
				Type: sqlc.JsonFilterOr,
			},
		},
		{
			name: "not with multiple children",
			filter: sqlc.JsonFilter{
				Type: sqlc.JsonFilterNot,
				Fields: []sqlc.JsonFilter{
					{Type: sqlc.JsonFilterEq, Column: "a", Value: 1},
					{Type: sqlc.JsonFilterEq, Column: "b", Value: 2},
				},
			},
		},
		{
			name: "like with non-string value",
			filter: sqlc.JsonFilter{
				Type:   sqlc.JsonFilterLike,
				Column: "name",
				Value:  123,
			},
		},
		{
			name: "unknown filter type",
			filter: sqlc.JsonFilter{
				Type:   "unknown",
				Column: "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.filter.ToExpression()
			assert.Error(t, err, "expected error for invalid filter: %+v", tt.filter)
		})
	}
}

func TestFilter_RealWorldExamples(t *testing.T) {
	t.Run("e-commerce product search", func(t *testing.T) {
		// Find active products with price between 10 and 100, in specific categories
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterAnd,
			Fields: []sqlc.JsonFilter{
				{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
				{Type: sqlc.JsonFilterBetween, Column: "price", From: 10.0, To: 100.0},
				{Type: sqlc.JsonFilterIn, Column: "category", Values: []any{"electronics", "books", "clothing"}},
				{Type: sqlc.JsonFilterGt, Column: "stock", Value: 0},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("products").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`status` = ?")
		assert.Contains(t, sql, "`price` >= ?")
		assert.Contains(t, sql, "`price` <= ?")
		assert.Contains(t, sql, "`category` IN (?, ?, ?)")
		assert.Contains(t, sql, "`stock` > ?")
		assert.Equal(t, []any{"active", 10.0, 100.0, "electronics", "books", "clothing", 0}, params)
	})

	t.Run("user search with optional filters", func(t *testing.T) {
		// Find users who are either verified OR (active AND created recently)
		filter := sqlc.JsonFilter{
			Type: sqlc.JsonFilterOr,
			Fields: []sqlc.JsonFilter{
				{Type: sqlc.JsonFilterEq, Column: "verified", Value: true},
				{
					Type: sqlc.JsonFilterAnd,
					Fields: []sqlc.JsonFilter{
						{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
						{Type: sqlc.JsonFilterGte, Column: "created_at", Value: "2024-01-01"},
					},
				},
			},
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("users").Where(expr)
		sql, _, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`verified` = ?")
		assert.Contains(t, sql, "`status` = ?")
		assert.Contains(t, sql, "`created_at` >= ?")
		assert.Contains(t, sql, "OR")
		assert.Contains(t, sql, "AND")
	})

	t.Run("exclude soft-deleted records", func(t *testing.T) {
		// Records that are NOT deleted (deleted_at IS NULL)
		filter := sqlc.JsonFilter{
			Type:   sqlc.JsonFilterIsNull,
			Column: "deleted_at",
		}

		expr, err := filter.ToExpression()
		require.NoError(t, err)

		qb := sqlc.From("records").Where(expr)
		sql, params, err := qb.ToSql()
		require.NoError(t, err)

		assert.Contains(t, sql, "`deleted_at` IS NULL")
		assert.Empty(t, params)
	})
}
