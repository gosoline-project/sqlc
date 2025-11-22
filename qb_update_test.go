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

func TestSimpleUpdate(t *testing.T) {
	q := sqlc.Update("users").
		Set("name", "John").
		Set("email", "john@example.com").
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"John", "john@example.com", 1}, params)
}

func TestUpdateWithSetExpr(t *testing.T) {
	q := sqlc.Update("users").
		SetExpr("count", "count + 1").
		SetExpr("updated_at", "NOW()").
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `count` = count + 1, `updated_at` = NOW() WHERE id = ?", sql)
	assert.Equal(t, []any{1}, params)
}

func TestUpdateWithMixedSetAndSetExpr(t *testing.T) {
	q := sqlc.Update("users").
		Set("name", "John").
		SetExpr("count", "count + 1").
		Set("email", "john@example.com").
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `name` = ?, `count` = count + 1, `email` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"John", "john@example.com", 1}, params)
}

func TestUpdateWithSetMap(t *testing.T) {
	q := sqlc.Update("users").
		SetMap(map[string]any{
			"name":  "John",
			"email": "john@example.com",
		}).
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Map keys are sorted alphabetically
	assert.Equal(t, "UPDATE `users` SET `email` = ?, `name` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"john@example.com", "John", 1}, params)
}

func TestUpdateWithSetRecord(t *testing.T) {
	type User struct {
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := User{Name: "John", Email: "john@example.com"}
	q := sqlc.Update("users").
		SetRecord(user).
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Record fields are extracted in tag order
	assert.Equal(t, "UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"John", "john@example.com", 1}, params)
}

func TestUpdateWithSetRecordPointer(t *testing.T) {
	type User struct {
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := &User{Name: "John", Email: "john@example.com"}
	q := sqlc.Update("users").
		SetRecord(user).
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `name` = ?, `email` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"John", "john@example.com", 1}, params)
}

func TestUpdateWithMultipleWhere(t *testing.T) {
	q := sqlc.Update("users").
		Set("name", "John").
		Where("status = ?", "active").
		Where("age > ?", 18)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `name` = ? WHERE status = ? AND age > ?", sql)
	assert.Equal(t, []any{"John", "active", 18}, params)
}

func TestUpdateWithOrderBy(t *testing.T) {
	q := sqlc.Update("users").
		Set("status", "inactive").
		Where("last_login < ?", "2020-01-01").
		OrderBy("last_login ASC").
		Limit(10)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `status` = ? WHERE last_login < ? ORDER BY `last_login` ASC LIMIT ?", sql)
	assert.Equal(t, []any{"inactive", "2020-01-01", 10}, params)
}

func TestUpdateWithLimit(t *testing.T) {
	q := sqlc.Update("users").
		Set("processed", true).
		Where("processed = ?", false).
		Limit(100)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `processed` = ? WHERE processed = ? LIMIT ?", sql)
	assert.Equal(t, []any{true, false, 100}, params)
}

func TestUpdateNoTable(t *testing.T) {
	q := sqlc.Update("").
		Set("name", "John")

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name is required")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestUpdateNoSets(t *testing.T) {
	q := sqlc.Update("users").
		Where("id = ?", 1)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one SET assignment is required")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestUpdateSetMapEmpty(t *testing.T) {
	q := sqlc.Update("users").
		SetMap(map[string]any{})

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SetMap expects a non-empty map")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestUpdateSetRecordNoDbTags(t *testing.T) {
	type User struct {
		Name  string
		Email string
	}

	user := User{Name: "John", Email: "john@example.com"}
	q := sqlc.Update("users").
		SetRecord(user).
		Where("id = ?", 1)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "record has no db tags")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestUpdateImmutability(t *testing.T) {
	base := sqlc.Update("users").Where("id = ?", 1)

	q1 := base.Set("name", "John")
	q2 := base.Set("name", "Jane")

	sql1, params1, err1 := q1.ToSql()
	require.NoError(t, err1)

	sql2, params2, err2 := q2.ToSql()
	require.NoError(t, err2)

	assert.Equal(t, "UPDATE `users` SET `name` = ? WHERE id = ?", sql1)
	assert.Equal(t, []any{"John", 1}, params1)

	assert.Equal(t, "UPDATE `users` SET `name` = ? WHERE id = ?", sql2)
	assert.Equal(t, []any{"Jane", 1}, params2)
}

func TestUpdateErrorPersistsAcrossCalls(t *testing.T) {
	q := sqlc.Update("users").
		SetMap(map[string]any{}) // Error: empty map

	// Chain more methods - error should persist
	q = q.Where("id = ?", 1)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SetMap expects a non-empty map")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestUpdateWithClient(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlc.Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		WithClient(mockClient)

	ctx := context.Background()

	mockClient.On("Exec", ctx, "UPDATE `users` SET `name` = ? WHERE id = ?", []any{"John", 1}).Return(nil, nil)

	_, err := q.Exec(ctx)

	assert.NoError(t, err)
}

func TestUpdateExecWithoutClient(t *testing.T) {
	q := sqlc.Update("users").
		Set("name", "John").
		Where("id = ?", 1)

	ctx := context.Background()

	_, err := q.Exec(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no client set")
}

func TestUpdateExecWithBuildError(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlc.Update("users").
		Where("id = ?", 1).
		WithClient(mockClient) // No SET clause - error

	ctx := context.Background()

	_, err := q.Exec(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not build sql")
}

func TestUpdateExecWithClientError(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlc.Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		WithClient(mockClient)

	ctx := context.Background()

	mockClient.On("Exec", ctx, "UPDATE `users` SET `name` = ? WHERE id = ?", []any{"John", 1}).
		Return(nil, fmt.Errorf("database connection error"))

	_, err := q.Exec(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection error")
}

func TestUpdateWithClientImmutability(t *testing.T) {
	mock1 := mocks.NewClient(t)
	mock2 := mocks.NewClient(t)

	base := sqlc.Update("users").Where("id = ?", 1)
	q1 := base.WithClient(mock1).Set("name", "John")
	q2 := base.WithClient(mock2).Set("name", "Jane")

	ctx := context.Background()

	mock1.On("Exec", ctx, "UPDATE `users` SET `name` = ? WHERE id = ?", []any{"John", 1}).Return(nil, nil).Once()
	mock2.On("Exec", ctx, "UPDATE `users` SET `name` = ? WHERE id = ?", []any{"Jane", 1}).Return(nil, nil).Once()

	_, err1 := q1.Exec(ctx)
	_, err2 := q2.Exec(ctx)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestUpdateDifferentDataTypes(t *testing.T) {
	type Product struct {
		Name     string  `db:"name"`
		Price    float64 `db:"price"`
		InStock  bool    `db:"in_stock"`
		Quantity int64   `db:"quantity"`
	}

	product := Product{
		Name:     "Widget",
		Price:    19.99,
		InStock:  true,
		Quantity: 100,
	}

	q := sqlc.Update("products").
		SetRecord(product).
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `products` SET `name` = ?, `price` = ?, `in_stock` = ?, `quantity` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"Widget", 19.99, true, int64(100), 1}, params)
}

func TestUpdateColumnQuoting(t *testing.T) {
	q := sqlc.Update("users").
		Set("user.name", "John").
		Set("metadata->'$.email'", "john@example.com").
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Table-qualified and JSON columns should be quoted correctly
	assert.Equal(t, "UPDATE `users` SET `user`.`name` = ?, `metadata`->'$.email' = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"John", "john@example.com", 1}, params)
}

func TestUpdateWithWhereExpression(t *testing.T) {
	q := sqlc.Update("users").
		Set("status", "inactive").
		Where(sqlc.Col("age").Gt(65))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `status` = ? WHERE `age` > ?", sql)
	assert.Equal(t, []any{"inactive", 65}, params)
}

func TestUpdateWithComplexWhere(t *testing.T) {
	q := sqlc.Update("users").
		Set("status", "inactive").
		Where(sqlc.And(
			sqlc.Col("age").Gt(65),
			sqlc.Col("last_login").Lt("2020-01-01"),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `status` = ? WHERE (`age` > ? AND `last_login` < ?)", sql)
	assert.Equal(t, []any{"inactive", 65, "2020-01-01"}, params)
}

func TestUpdateWithOrderByExpression(t *testing.T) {
	q := sqlc.Update("users").
		Set("status", "inactive").
		Where("processed = ?", false).
		OrderBy(sqlc.Col("created_at").Asc()).
		Limit(10)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `status` = ? WHERE processed = ? ORDER BY `created_at` ASC LIMIT ?", sql)
	assert.Equal(t, []any{"inactive", false, 10}, params)
}

func TestUpdateWithLikeExpression(t *testing.T) {
	q := sqlc.Update("users").
		Set("verified", true).
		Where(sqlc.Col("email").Like("%@company.com"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `verified` = ? WHERE `email` LIKE ?", sql)
	assert.Equal(t, []any{true, "%@company.com"}, params)
}

func TestUpdateWithNotLikeExpression(t *testing.T) {
	q := sqlc.Update("users").
		Set("suspicious", true).
		Where(sqlc.Col("email").NotLike("%@trusted.com"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `suspicious` = ? WHERE `email` NOT LIKE ?", sql)
	assert.Equal(t, []any{true, "%@trusted.com"}, params)
}

func TestUpdateWithoutWhere(t *testing.T) {
	q := sqlc.Update("users").
		Set("status", "inactive")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// UPDATE without WHERE is valid (updates all rows)
	assert.Equal(t, "UPDATE `users` SET `status` = ?", sql)
	assert.Equal(t, []any{"inactive"}, params)
}

func TestUpdateSetMapImmutability(t *testing.T) {
	base := sqlc.Update("users")

	map1 := map[string]any{"name": "John"}
	map2 := map[string]any{"name": "Jane"}

	q1 := base.SetMap(map1).Where("id = ?", 1)
	q2 := base.SetMap(map2).Where("id = ?", 2)

	sql1, params1, err1 := q1.ToSql()
	require.NoError(t, err1)

	sql2, params2, err2 := q2.ToSql()
	require.NoError(t, err2)

	assert.Equal(t, "UPDATE `users` SET `name` = ? WHERE id = ?", sql1)
	assert.Equal(t, []any{"John", 1}, params1)

	assert.Equal(t, "UPDATE `users` SET `name` = ? WHERE id = ?", sql2)
	assert.Equal(t, []any{"Jane", 2}, params2)
}

func TestUpdatePartialStructFields(t *testing.T) {
	type User struct {
		Name   string `db:"name"`
		Email  string `db:"email"`
		NoTag  string // No db tag
		Status string `db:"status"`
	}

	user := User{
		Name:   "John",
		Email:  "john@example.com",
		NoTag:  "ignored",
		Status: "active",
	}

	q := sqlc.Update("users").
		SetRecord(user).
		Where("id = ?", 1)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Should only include fields with db tags
	assert.Equal(t, "UPDATE `users` SET `name` = ?, `email` = ?, `status` = ? WHERE id = ?", sql)
	assert.Equal(t, []any{"John", "john@example.com", "active", 1}, params)
}

func TestUpdateWithEqMap(t *testing.T) {
	q := sqlc.Update("users").
		Set("status", "inactive").
		Where(sqlc.Eq{"status": "active", "role": "admin"})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Eq map keys are sorted alphabetically
	assert.Equal(t, "UPDATE `users` SET `status` = ? WHERE (`role` = ? AND `status` = ?)", sql)
	assert.Equal(t, []any{"inactive", "admin", "active"}, params)
}

func TestUpdateWithMultipleOrderBy(t *testing.T) {
	q := sqlc.Update("users").
		Set("processed", true).
		Where("processed = ?", false).
		OrderBy("priority DESC", "created_at ASC").
		Limit(50)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "UPDATE `users` SET `processed` = ? WHERE processed = ? ORDER BY `priority` DESC, `created_at` ASC LIMIT ?", sql)
	assert.Equal(t, []any{true, false, 50}, params)
}
