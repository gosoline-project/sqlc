package sqlc_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gosoline-project/sqlc"
	mocks "github.com/gosoline-project/sqlc/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleDelete(t *testing.T) {
	q := sqlc.Delete("users").
		Where("status = ?", "inactive")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE status = ?", sql)
	assert.Equal(t, []any{"inactive"}, params)
}

func TestDeleteWithoutWhere(t *testing.T) {
	q := sqlc.Delete("users")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// DELETE without WHERE is allowed (deletes all rows)
	assert.Equal(t, "DELETE FROM `users`", sql)
	assert.Equal(t, []any{}, params)
}

func TestDeleteWithMultipleWhereConditions(t *testing.T) {
	q := sqlc.Delete("users").
		Where("status = ?", "inactive").
		Where("created_at < ?", "2020-01-01")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE status = ? AND created_at < ?", sql)
	assert.Equal(t, []any{"inactive", "2020-01-01"}, params)
}

func TestDeleteWithExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Col("age").Lt(18))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE `age` < ?", sql)
	assert.Equal(t, []any{18}, params)
}

func TestDeleteWithComplexExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.And(
			sqlc.Col("status").Eq("inactive"),
			sqlc.Col("created_at").Lt("2020-01-01"),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE (`status` = ? AND `created_at` < ?)", sql)
	assert.Equal(t, []any{"inactive", "2020-01-01"}, params)
}

func TestDeleteWithOrExpression(t *testing.T) {
	q := sqlc.Delete("logs").
		Where(sqlc.Or(
			sqlc.Col("level").Eq("debug"),
			sqlc.Col("level").Eq("trace"),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE (`level` = ? OR `level` = ?)", sql)
	assert.Equal(t, []any{"debug", "trace"}, params)
}

func TestDeleteWithEqMap(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Eq{
			"status": "inactive",
			"role":   "guest",
		})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Eq map keys are sorted alphabetically
	assert.Equal(t, "DELETE FROM `users` WHERE (`role` = ? AND `status` = ?)", sql)
	assert.Equal(t, []any{"guest", "inactive"}, params)
}

func TestDeleteWithOrderBy(t *testing.T) {
	q := sqlc.Delete("logs").
		Where("level = ?", "debug").
		OrderBy("created_at ASC")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE level = ? ORDER BY `created_at` ASC", sql)
	assert.Equal(t, []any{"debug"}, params)
}

func TestDeleteWithOrderByMultipleColumns(t *testing.T) {
	q := sqlc.Delete("logs").
		Where("level = ?", "debug").
		OrderBy("priority DESC", "created_at ASC")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE level = ? ORDER BY `priority` DESC, `created_at` ASC", sql)
	assert.Equal(t, []any{"debug"}, params)
}

func TestDeleteWithOrderByExpression(t *testing.T) {
	q := sqlc.Delete("logs").
		Where("level = ?", "debug").
		OrderBy(sqlc.Col("created_at").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE level = ? ORDER BY `created_at` DESC", sql)
	assert.Equal(t, []any{"debug"}, params)
}

func TestDeleteWithLimit(t *testing.T) {
	q := sqlc.Delete("logs").
		Where("level = ?", "debug").
		Limit(1000)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE level = ? LIMIT ?", sql)
	assert.Equal(t, []any{"debug", 1000}, params)
}

func TestDeleteWithOrderByAndLimit(t *testing.T) {
	q := sqlc.Delete("logs").
		Where("level = ?", "debug").
		OrderBy("created_at ASC").
		Limit(1000)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE level = ? ORDER BY `created_at` ASC LIMIT ?", sql)
	assert.Equal(t, []any{"debug", 1000}, params)
}

func TestDeleteWithLimitOnly(t *testing.T) {
	q := sqlc.Delete("logs").
		Limit(100)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` LIMIT ?", sql)
	assert.Equal(t, []any{100}, params)
}

func TestDeleteWithoutTable(t *testing.T) {
	q := sqlc.Delete("")

	_, _, err := q.ToSql()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name is required")
}

func TestDeleteImmutability(t *testing.T) {
	q1 := sqlc.Delete("users")
	q2 := q1.Where("status = ?", "inactive")
	q3 := q2.Where("age < ?", 18)

	// q1 should remain unchanged
	sql1, params1, err := q1.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `users`", sql1)
	assert.Equal(t, []any{}, params1)

	// q2 should have only first where
	sql2, params2, err := q2.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `users` WHERE status = ?", sql2)
	assert.Equal(t, []any{"inactive"}, params2)

	// q3 should have both where conditions
	sql3, params3, err := q3.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `users` WHERE status = ? AND age < ?", sql3)
	assert.Equal(t, []any{"inactive", 18}, params3)
}

func TestDeleteImmutabilityWithLimit(t *testing.T) {
	q1 := sqlc.Delete("logs")
	q2 := q1.Limit(100)
	q3 := q2.Limit(500)

	// q1 should have no limit
	sql1, params1, err := q1.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `logs`", sql1)
	assert.Equal(t, []any{}, params1)

	// q2 should have limit 100
	sql2, params2, err := q2.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `logs` LIMIT ?", sql2)
	assert.Equal(t, []any{100}, params2)

	// q3 should have limit 500
	sql3, params3, err := q3.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `logs` LIMIT ?", sql3)
	assert.Equal(t, []any{500}, params3)
}

func TestDeleteImmutabilityWithOrderBy(t *testing.T) {
	q1 := sqlc.Delete("logs")
	q2 := q1.OrderBy("created_at ASC")
	q3 := q2.OrderBy("priority DESC")

	// q1 should have no order by
	sql1, params1, err := q1.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `logs`", sql1)
	assert.Equal(t, []any{}, params1)

	// q2 should have created_at order
	sql2, params2, err := q2.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `logs` ORDER BY `created_at` ASC", sql2)
	assert.Equal(t, []any{}, params2)

	// q3 should have priority order (OrderBy replaces)
	sql3, params3, err := q3.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `logs` ORDER BY `priority` DESC", sql3)
	assert.Equal(t, []any{}, params3)
}

func TestDeleteWithClient(t *testing.T) {
	mockClient := mocks.NewQuerier(t)
	ctx := context.Background()

	q := sqlc.Delete("users").
		WithClient(mockClient).
		Where("status = ?", "inactive")

	// Mock expects the exact SQL and params
	mockClient.EXPECT().
		Exec(ctx, "DELETE FROM `users` WHERE status = ?", []any{"inactive"}).
		Return(nil, nil).
		Once()

	_, err := q.Exec(ctx)
	require.NoError(t, err)
}

func TestDeleteWithClientComplexQuery(t *testing.T) {
	mockClient := mocks.NewQuerier(t)
	ctx := context.Background()

	q := sqlc.Delete("logs").
		WithClient(mockClient).
		Where("level = ?", "debug").
		OrderBy("created_at ASC").
		Limit(1000)

	// Mock expects the exact SQL and params
	mockClient.EXPECT().
		Exec(ctx, "DELETE FROM `logs` WHERE level = ? ORDER BY `created_at` ASC LIMIT ?", []any{"debug", 1000}).
		Return(nil, nil).
		Once()

	_, err := q.Exec(ctx)
	require.NoError(t, err)
}

func TestDeleteExecWithoutClient(t *testing.T) {
	ctx := context.Background()

	q := sqlc.Delete("users").
		Where("status = ?", "inactive")

	_, err := q.Exec(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no client set")
}

func TestDeleteWithInvalidWhereExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(123) // Invalid type

	_, _, err := q.ToSql()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Where condition")
}

func TestDeleteWithInvalidOrderByExpression(t *testing.T) {
	q := sqlc.Delete("users").
		OrderBy(123) // Invalid type

	_, _, err := q.ToSql()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for OrderBy argument")
}

func TestDeleteQuotesIdentifiers(t *testing.T) {
	q := sqlc.Delete("my_table").
		Where("my_column = ?", "value")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `my_table` WHERE my_column = ?", sql)
	assert.Equal(t, []any{"value"}, params)
}

func TestDeleteWithInExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Col("status").In("inactive", "suspended", "deleted"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE `status` IN (?, ?, ?)", sql)
	assert.Equal(t, []any{"inactive", "suspended", "deleted"}, params)
}

func TestDeleteWithNotExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Not(sqlc.Col("status").Eq("active")))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE NOT (`status` = ?)", sql)
	assert.Equal(t, []any{"active"}, params)
}

func TestDeleteWithBetweenExpression(t *testing.T) {
	q := sqlc.Delete("logs").
		Where(sqlc.Col("created_at").Between("2020-01-01", "2020-12-31"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `logs` WHERE `created_at` BETWEEN ? AND ?", sql)
	assert.Equal(t, []any{"2020-01-01", "2020-12-31"}, params)
}

func TestDeleteWithNotBetweenExpression(t *testing.T) {
	q := sqlc.Delete("products").
		Where(sqlc.Col("price").NotBetween(100, 1000))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `products` WHERE `price` NOT BETWEEN ? AND ?", sql)
	assert.Equal(t, []any{100, 1000}, params)
}

func TestDeleteWithIsNullExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Col("deleted_at").IsNull())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE `deleted_at` IS NULL", sql)
	assert.Equal(t, []any{}, params)
}

func TestDeleteWithIsNotNullExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Col("email").IsNotNull())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE `email` IS NOT NULL", sql)
	assert.Equal(t, []any{}, params)
}

func TestDeleteWithLikeExpression(t *testing.T) {
	q := sqlc.Delete("users").
		Where(sqlc.Col("email").Like("%@test.com"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE `email` LIKE ?", sql)
	assert.Equal(t, []any{"%@test.com"}, params)
}

func TestDeleteWithNotLikeExpression(t *testing.T) {
	q := sqlc.Delete("spam_emails").
		Where(sqlc.Col("email").NotLike("%@legitimate.com"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `spam_emails` WHERE `email` NOT LIKE ?", sql)
	assert.Equal(t, []any{"%@legitimate.com"}, params)
}

func TestDeleteComplexRealWorld(t *testing.T) {
	// Delete old debug logs, keeping only the 1000 most recent
	q := sqlc.Delete("application_logs").
		Where(sqlc.And(
			sqlc.Col("level").Eq("debug"),
			sqlc.Col("created_at").Lt("2024-01-01"),
		)).
		OrderBy(sqlc.Col("created_at").Asc()).
		Limit(1000)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `application_logs` WHERE (`level` = ? AND `created_at` < ?) ORDER BY `created_at` ASC LIMIT ?", sql)
	assert.Equal(t, []any{"debug", "2024-01-01", 1000}, params)
}

func TestDeleteReuseQuery(t *testing.T) {
	// Base query for deleting inactive users
	baseQuery := sqlc.Delete("users").
		Where("status = ?", "inactive")

	// Different limits for different scenarios
	q1 := baseQuery.Limit(10)
	q2 := baseQuery.Limit(100)

	sql1, params1, err := q1.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `users` WHERE status = ? LIMIT ?", sql1)
	assert.Equal(t, []any{"inactive", 10}, params1)

	sql2, params2, err := q2.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `users` WHERE status = ? LIMIT ?", sql2)
	assert.Equal(t, []any{"inactive", 100}, params2)

	// Base query unchanged
	sqlBase, paramsBase, err := baseQuery.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM `users` WHERE status = ?", sqlBase)
	assert.Equal(t, []any{"inactive"}, paramsBase)
}

func TestDeleteBuilderChaining(t *testing.T) {
	q := sqlc.Delete("users").
		Where("status = ?", "inactive").
		Where("created_at < ?", "2020-01-01").
		OrderBy("created_at ASC").
		Limit(100)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "DELETE FROM `users` WHERE status = ? AND created_at < ? ORDER BY `created_at` ASC LIMIT ?", sql)
	assert.Equal(t, []any{"inactive", "2020-01-01", 100}, params)
}

func ExampleDelete() {
	// Simple delete
	q := sqlc.Delete("users").
		Where("status = ?", "inactive")

	sql, params, _ := q.ToSql()
	fmt.Println(sql)
	fmt.Printf("params: %v\n", params)

	// Output:
	// DELETE FROM `users` WHERE status = ?
	// params: [inactive]
}

func ExampleDeleteQueryBuilder_OrderBy() {
	// Delete oldest debug logs (limit to 1000 rows)
	q := sqlc.Delete("logs").
		Where("level = ?", "debug").
		OrderBy("created_at ASC").
		Limit(1000)

	sql, params, _ := q.ToSql()
	fmt.Println(sql)
	fmt.Printf("params: %v\n", params)

	// Output:
	// DELETE FROM `logs` WHERE level = ? ORDER BY `created_at` ASC LIMIT ?
	// params: [debug 1000]
}

func ExampleDeleteQueryBuilder_Where_expression() {
	// Delete using Expression
	q := sqlc.Delete("users").
		Where(sqlc.And(
			sqlc.Col("status").Eq("inactive"),
			sqlc.Col("created_at").Lt("2020-01-01"),
		))

	sql, params, _ := q.ToSql()
	fmt.Println(sql)
	fmt.Printf("params: %v\n", params)

	// Output:
	// DELETE FROM `users` WHERE (`status` = ? AND `created_at` < ?)
	// params: [inactive 2020-01-01]
}
