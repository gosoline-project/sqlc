package sqlc_test

import (
	"testing"

	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========== SqlerWhere Tests ==========

func TestSqlerWhereWithStringCondition(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where("status = ?", "active")

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "status = ?", sql)
	assert.Equal(t, []any{"active"}, params)
}

func TestSqlerWhereWithMultipleStringConditions(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where("status = ?", "active").
		Where("age >= ?", 18)

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "status = ? AND age >= ?", sql)
	assert.Equal(t, []any{"active", 18}, params)
}

func TestSqlerWhereWithExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Col("age").Gt(18))

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`age` > ?", sql)
	assert.Equal(t, []any{18}, params)
}

func TestSqlerWhereWithMultipleExpressions(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Col("age").Gt(18)).
		Where(sqlc.Col("status").Eq("active"))

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`age` > ? AND `status` = ?", sql)
	assert.Equal(t, []any{18, "active"}, params)
}

func TestSqlerWhereWithEqMap(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Eq{"status": "active", "role": "admin"})

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "(`role` = ? AND `status` = ?)", sql)
	assert.Equal(t, []any{"admin", "active"}, params)
}

func TestSqlerWhereWithEqMapSingleKey(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Eq{"status": "active"})

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`status` = ?", sql)
	assert.Equal(t, []any{"active"}, params)
}

func TestSqlerWhereWithEmptyEqMap(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Eq{})

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
	assert.Empty(t, params)
}

func TestSqlerWhereWithMixedConditions(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where("status = ?", "active").
		Where(sqlc.Col("age").Gt(18)).
		Where(sqlc.Eq{"role": "admin"})

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "status = ? AND `age` > ? AND `role` = ?", sql)
	assert.Equal(t, []any{"active", 18, "admin"}, params)
}

func TestSqlerWhereWithAndExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.And(
		sqlc.Col("age").Gte(18),
		sqlc.Col("status").Eq("active"),
	))

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "(`age` >= ? AND `status` = ?)", sql)
	assert.Equal(t, []any{18, "active"}, params)
}

func TestSqlerWhereWithOrExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Or(
		sqlc.Col("status").Eq("active"),
		sqlc.Col("status").Eq("pending"),
	))

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "(`status` = ? OR `status` = ?)", sql)
	assert.Equal(t, []any{"active", "pending"}, params)
}

func TestSqlerWhereWithNotExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Not(sqlc.Col("deleted").Eq(true)))

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "NOT (`deleted` = ?)", sql)
	assert.Equal(t, []any{true}, params)
}

func TestSqlerWhereWithInExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Col("status").In("active", "pending", "approved"))

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`status` IN (?, ?, ?)", sql)
	assert.Equal(t, []any{"active", "pending", "approved"}, params)
}

func TestSqlerWhereWithIsNullExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(sqlc.Col("deleted_at").IsNull())

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`deleted_at` IS NULL", sql)
	assert.Empty(t, params)
}

func TestSqlerWhereWithNilExpression(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	var nilExpr *sqlc.Expression = nil
	sqlerWhere.Where(nilExpr)

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
	assert.Empty(t, params)
}

func TestSqlerWhereWithInvalidType(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()
	sqlerWhere.Where(123)

	sql, params, err := sqlerWhere.ToSql()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Where condition")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestSqlerWhereEmpty(t *testing.T) {
	sqlerWhere := sqlc.NewSqlerWhere()

	sql, params, err := sqlerWhere.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
	assert.Empty(t, params)
}

// ========== SqlerGroupBy Tests ==========

func TestSqlerGroupByWithSingleColumn(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()
	sqlerGroupBy.GroupBy("status")

	sql, err := sqlerGroupBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`status`", sql)
}

func TestSqlerGroupByWithMultipleColumns(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()
	sqlerGroupBy.GroupBy("status", "country", "city")

	sql, err := sqlerGroupBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`status`, `country`, `city`", sql)
}

func TestSqlerGroupByWithExpression(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()
	sqlerGroupBy.GroupBy(sqlc.Col("DATE(created_at)"))

	sql, err := sqlerGroupBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`DATE(created_at)`", sql)
}

func TestSqlerGroupByWithMixedTypes(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()
	sqlerGroupBy.GroupBy("status", sqlc.Col("DATE(created_at)"))

	sql, err := sqlerGroupBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`status`, `DATE(created_at)`", sql)
}

func TestSqlerGroupByReplacesExisting(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()
	sqlerGroupBy.GroupBy("status")
	sqlerGroupBy.GroupBy("country") // Should replace, not append

	sql, err := sqlerGroupBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`country`", sql)
}

func TestSqlerGroupByWithInvalidType(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()
	sqlerGroupBy.GroupBy(123)

	sql, err := sqlerGroupBy.ToSql()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for GroupBy argument")
	assert.Empty(t, sql)
}

func TestSqlerGroupByEmpty(t *testing.T) {
	sqlerGroupBy := sqlc.NewSqlerGroupBy()

	sql, err := sqlerGroupBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
}

// ========== SqlerHaving Tests ==========

func TestSqlerHavingWithStringCondition(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having("COUNT(*) > ?", 10)

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "COUNT(*) > ?", sql)
	assert.Equal(t, []any{10}, params)
}

func TestSqlerHavingWithMultipleStringConditions(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having("COUNT(*) > ?", 10).
		Having("SUM(amount) > ?", 1000)

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "COUNT(*) > ? AND SUM(amount) > ?", sql)
	assert.Equal(t, []any{10, 1000}, params)
}

func TestSqlerHavingWithExpression(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having(sqlc.Col("*").Count().Gt(10))

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "COUNT(*) > ?", sql)
	assert.Equal(t, []any{10}, params)
}

func TestSqlerHavingWithMultipleExpressions(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having(sqlc.Col("*").Count().Gt(10)).
		Having(sqlc.Col("amount").Sum().Gt(1000))

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "COUNT(*) > ? AND SUM(`amount`) > ?", sql)
	assert.Equal(t, []any{10, 1000}, params)
}

func TestSqlerHavingWithMixedConditions(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having("COUNT(*) > ?", 5).
		Having(sqlc.Col("amount").Sum().Gte(500)).
		Having(sqlc.Col("amount").Sum().Gte(500)).
		Having(sqlc.Col("price").Avg().Lt(100))

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "COUNT(*) > ? AND SUM(`amount`) >= ? AND SUM(`amount`) >= ? AND AVG(`price`) < ?", sql)
	assert.Equal(t, []any{5, 500, 500, 100}, params)
}

func TestSqlerHavingWithAndExpression(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having(sqlc.And(
		sqlc.Col("*").Count().Gt(10),
		sqlc.Col("amount").Sum().Gt(1000),
	))

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "(COUNT(*) > ? AND SUM(`amount`) > ?)", sql)
	assert.Equal(t, []any{10, 1000}, params)
}

func TestSqlerHavingWithOrExpression(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having(sqlc.Or(
		sqlc.Col("amount").Sum().Gt(1000),
		sqlc.Col("price").Avg().Lt(50),
	))

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "(SUM(`amount`) > ? OR AVG(`price`) < ?)", sql)
	assert.Equal(t, []any{1000, 50}, params)
}

func TestSqlerHavingWithComplexExpression(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having(sqlc.And(
		sqlc.Col("*").Count().Gt(10),
		sqlc.Or(
			sqlc.Col("amount").Sum().Gt(1000),
			sqlc.Col("price").Avg().Lt(50),
		),
	))

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "(COUNT(*) > ? AND (SUM(`amount`) > ? OR AVG(`price`) < ?))", sql)
	assert.Equal(t, []any{10, 1000, 50}, params)
}

func TestSqlerHavingWithNilExpression(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	var nilExpr *sqlc.Expression = nil
	sqlerHaving.Having(nilExpr)

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
	assert.Empty(t, params)
}

func TestSqlerHavingWithInvalidType(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()
	sqlerHaving.Having(123)

	sql, params, err := sqlerHaving.ToSql()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Having condition")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestSqlerHavingEmpty(t *testing.T) {
	sqlerHaving := sqlc.NewSqlerHaving()

	sql, params, err := sqlerHaving.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
	assert.Empty(t, params)
}

// ========== SqlerOrderBy Tests ==========

func TestSqlerOrderByWithSingleColumn(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy("created_at DESC")

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`created_at` DESC", sql)
}

func TestSqlerOrderByWithMultipleColumns(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy("name ASC", "created_at DESC")

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`name` ASC, `created_at` DESC", sql)
}

func TestSqlerOrderByWithExpression(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy(sqlc.Col("price").Desc())

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`price` DESC", sql)
}

func TestSqlerOrderByWithMultipleExpressions(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy(sqlc.Col("name").Asc(), sqlc.Col("id").Desc())

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`name` ASC, `id` DESC", sql)
}

func TestSqlerOrderByWithMixedTypes(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy("name ASC", sqlc.Col("created_at").Desc())

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`name` ASC, `created_at` DESC", sql)
}

func TestSqlerOrderByReplacesExisting(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy("name ASC")
	sqlerOrderBy.OrderBy("created_at DESC") // Should replace, not append

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "`created_at` DESC", sql)
}

func TestSqlerOrderByWithLiteralExpression(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy(sqlc.Lit(1).Asc(), sqlc.Lit(2).Desc())

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "1 ASC, 2 DESC", sql)
}

func TestSqlerOrderByWithInvalidType(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()
	sqlerOrderBy.OrderBy(123)

	sql, err := sqlerOrderBy.ToSql()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for OrderBy argument")
	assert.Empty(t, sql)
}

func TestSqlerOrderByEmpty(t *testing.T) {
	sqlerOrderBy := sqlc.NewSqlerOrderBy()

	sql, err := sqlerOrderBy.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "", sql)
}

// TestSqlerHavingProperAggregateUsage demonstrates the correct way to use aggregate functions
func TestSqlerHavingProperAggregateUsage(t *testing.T) {
	t.Run("COUNT(*) using Col(\"*\").Count()", func(t *testing.T) {
		sqlerHaving := sqlc.NewSqlerHaving()
		sqlerHaving.Having(sqlc.Col("*").Count().Gt(10))

		sql, params, err := sqlerHaving.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "COUNT(*) > ?", sql)
		assert.Equal(t, []any{10}, params)
	})

	t.Run("SUM(column) using Col(\"column\").Sum()", func(t *testing.T) {
		sqlerHaving := sqlc.NewSqlerHaving()
		sqlerHaving.Having(sqlc.Col("amount").Sum().Gte(1000))

		sql, params, err := sqlerHaving.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "SUM(`amount`) >= ?", sql)
		assert.Equal(t, []any{1000}, params)
	})

	t.Run("AVG(column) using Col(\"column\").Avg()", func(t *testing.T) {
		sqlerHaving := sqlc.NewSqlerHaving()
		sqlerHaving.Having(sqlc.Col("rating").Avg().Gt(4.5))

		sql, params, err := sqlerHaving.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "AVG(`rating`) > ?", sql)
		assert.Equal(t, []any{4.5}, params)
	})

	t.Run("MAX(column) using Col(\"column\").Max()", func(t *testing.T) {
		sqlerHaving := sqlc.NewSqlerHaving()
		sqlerHaving.Having(sqlc.Col("price").Max().Lt(100))

		sql, params, err := sqlerHaving.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "MAX(`price`) < ?", sql)
		assert.Equal(t, []any{100}, params)
	})

	t.Run("MIN(column) using Col(\"column\").Min()", func(t *testing.T) {
		sqlerHaving := sqlc.NewSqlerHaving()
		sqlerHaving.Having(sqlc.Col("quantity").Min().Gte(1))

		sql, params, err := sqlerHaving.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "MIN(`quantity`) >= ?", sql)
		assert.Equal(t, []any{1}, params)
	})

	t.Run("Complex expression with multiple aggregates", func(t *testing.T) {
		sqlerHaving := sqlc.NewSqlerHaving()
		sqlerHaving.Having(sqlc.And(
			sqlc.Col("*").Count().Gt(10),
			sqlc.Col("amount").Sum().Gte(1000),
			sqlc.Or(
				sqlc.Col("price").Avg().Lt(50),
				sqlc.Col("discount").Max().Gt(20),
			),
		))

		sql, params, err := sqlerHaving.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "(COUNT(*) > ? AND SUM(`amount`) >= ? AND (AVG(`price`) < ? OR MAX(`discount`) > ?))", sql)
		assert.Equal(t, []any{10, 1000, 50, 20}, params)
	})
}
