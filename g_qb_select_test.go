package sqlc_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/gosoline-project/sqlc"
	mocks "github.com/gosoline-project/sqlc/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type TestUser struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

func TestGenericSelectToSql(t *testing.T) {
	q := sqlc.FromG[TestUser]("users").
		Where("status = ?", "active").
		Limit(10)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM `users` WHERE status = ? LIMIT ?", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, "active", params[0])
	assert.Equal(t, 10, params[1])
}

func TestGenericSelectWithColumns(t *testing.T) {
	q := sqlc.FromG[TestUser]("users").
		Columns("id", "name").
		Where("status = ?", "active")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE status = ?", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, "active", params[0])
}

func TestGenericSelectGet(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewClient(t)

	expectedUser := &TestUser{ID: 1, Name: "John", Email: "john@example.com"}

	// Mock the Get call using mock.Anything for variadic args
	mockClient.EXPECT().
		Get(ctx, mock.Anything, "SELECT `id`, `name`, `email` FROM `users` WHERE id = ?", mock.Anything).
		RunAndReturn(func(ctx context.Context, dest any, query string, args ...any) error {
			user := dest.(*TestUser)
			*user = *expectedUser

			return nil
		})

	user, err := sqlc.FromG[TestUser]("users").
		WithClient(mockClient).
		Where("id = ?", 1).
		Get(ctx)

	require.NoError(t, err)
	assert.Equal(t, expectedUser, user)
}

func TestGenericSelectGetError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewClient(t)

	// Mock the Get call to return an error
	mockClient.EXPECT().
		Get(ctx, mock.Anything, "SELECT `id`, `name`, `email` FROM `users` WHERE id = ?", mock.Anything).
		Return(sql.ErrNoRows)

	user, err := sqlc.FromG[TestUser]("users").
		WithClient(mockClient).
		Where("id = ?", 999).
		Get(ctx)

	require.Error(t, err)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Equal(t, &TestUser{}, user) // Should return zero value
}

func TestGenericSelectSelect(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewClient(t)

	expectedUsers := []TestUser{
		{ID: 1, Name: "John", Email: "john@example.com"},
		{ID: 2, Name: "Jane", Email: "jane@example.com"},
	}

	// Mock the Select call
	mockClient.EXPECT().
		Select(ctx, mock.Anything, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ?", mock.Anything).
		RunAndReturn(func(ctx context.Context, dest any, query string, args ...any) error {
			users := dest.(*[]TestUser)
			*users = expectedUsers

			return nil
		})

	users, err := sqlc.FromG[TestUser]("users").
		WithClient(mockClient).
		Where("status = ?", "active").
		Select(ctx)

	require.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, expectedUsers, users)
}

func TestGenericSelectSelectEmpty(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewClient(t)

	// Mock the Select call to return empty slice
	mockClient.EXPECT().
		Select(ctx, mock.Anything, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ?", mock.Anything).
		RunAndReturn(func(ctx context.Context, dest any, query string, args ...any) error {
			users := dest.(*[]TestUser)
			*users = []TestUser{}

			return nil
		})

	users, err := sqlc.FromG[TestUser]("users").
		WithClient(mockClient).
		Where("status = ?", "inactive").
		Select(ctx)

	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestGenericSelectBuilderChaining(t *testing.T) {
	q := sqlc.FromG[TestUser]("users").
		As("u").
		Distinct().
		Columns("id", "name").
		Where("status = ?", "active").
		GroupBy("name").
		Having("COUNT(*) > ?", 1).
		OrderBy("name ASC").
		Limit(10).
		Offset(5)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT DISTINCT `id`, `name` FROM `users` AS u WHERE status = ? GROUP BY `name` HAVING COUNT(*) > ? ORDER BY `name` ASC LIMIT ? OFFSET ?", sql)
	assert.Len(t, params, 4)
	assert.Equal(t, "active", params[0])
	assert.Equal(t, 1, params[1])
	assert.Equal(t, 10, params[2])
	assert.Equal(t, 5, params[3])
}

func TestGenericSelectWithExpressions(t *testing.T) {
	q := sqlc.FromG[TestUser]("users").
		Column(sqlc.Col("id")).
		Column(sqlc.Col("name").As("full_name")).
		Where(sqlc.Col("age").Gt(18)).
		OrderBy(sqlc.Col("created_at").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Contains(t, sql, "`id`")
	assert.Contains(t, sql, "`name` AS full_name")
	assert.Contains(t, sql, "`age` > ?")
	assert.Contains(t, sql, "`created_at` DESC")
	assert.Len(t, params, 1)
	assert.Equal(t, 18, params[0])
}
