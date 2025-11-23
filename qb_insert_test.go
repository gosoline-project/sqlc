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

func TestSimpleInsert(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com"}, params)
}

func TestInsertMultipleRows(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John").
		Values(2, "Jane").
		Values(3, "Bob")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?), (?, ?)", sql)
	assert.Equal(t, []any{1, "John", 2, "Jane", 3, "Bob"}, params)
}

func TestInsertValuesRows(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "email").
		ValuesRows(
			[]any{1, "John", "john@example.com"},
			[]any{2, "Jane", "jane@example.com"},
			[]any{3, "Bob", "bob@example.com"},
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com", 3, "Bob", "bob@example.com"}, params)
}

func TestInsertWithRecord(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := User{ID: 1, Name: "John", Email: "john@example.com"}
	q := sqlc.Into("users").Records(user)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// ToSql extracts values and uses positional parameters
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com"}, params)
}

func TestInsertWithRecordPointer(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := &User{ID: 1, Name: "John", Email: "john@example.com"}
	q := sqlc.Into("users").Records(user)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// ToSql extracts values and uses positional parameters
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com"}, params)
}

func TestInsertWithRecords(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user1 := User{ID: 1, Name: "John", Email: "john@example.com"}
	user2 := User{ID: 2, Name: "Jane", Email: "jane@example.com"}
	user3 := User{ID: 3, Name: "Bob", Email: "bob@example.com"}
	q := sqlc.Into("users").Records(user1, user2, user3)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// ToSql extracts values and uses positional parameters with multiple VALUES
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com", 3, "Bob", "bob@example.com"}, params)
}

func TestInsertWithRecordsPointers(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user1 := &User{ID: 1, Name: "John", Email: "john@example.com"}
	user2 := &User{ID: 2, Name: "Jane", Email: "jane@example.com"}
	q := sqlc.Into("users").Records(user1, user2)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// ToSql extracts values and uses positional parameters with multiple VALUES
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com"}, params)
}

func TestInsertWithRecordsSlice(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	users := []User{
		{ID: 1, Name: "John", Email: "john@example.com"},
		{ID: 2, Name: "Jane", Email: "jane@example.com"},
		{ID: 3, Name: "Bob", Email: "bob@example.com"},
	}

	q := sqlc.Into("users").Records(users)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com", 3, "Bob", "bob@example.com"}, params)
}

func TestInsertWithRecordsSlicePointers(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	users := []*User{
		{ID: 1, Name: "John", Email: "john@example.com"},
		{ID: 2, Name: "Jane", Email: "jane@example.com"},
	}

	q := sqlc.Into("users").Records(users)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com"}, params)
}

func TestInsertWithRecordsMixedVariadicAndSlice(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user1 := User{ID: 1, Name: "John", Email: "john@example.com"}
	moreUsers := []User{
		{ID: 2, Name: "Jane", Email: "jane@example.com"},
		{ID: 3, Name: "Bob", Email: "bob@example.com"},
	}

	q := sqlc.Into("users").Records(user1, moreUsers)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com", 3, "Bob", "bob@example.com"}, params)
}

func TestInsertWithRecordsEmptySlice(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	users := []User{}
	q := sqlc.Into("users").Records(users)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expects at least one record")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertMismatchedValuesCount(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "email").
		Values(1, "John") // Only 2 values for 3 columns

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mismatched values count")
	assert.Contains(t, err.Error(), "expected 3 values for 3 columns, got 2")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertValuesRowsMismatchedCount(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		ValuesRows(
			[]any{1, "John"},
			[]any{2}, // Missing value
		)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mismatched values count in row 1")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertNoTable(t *testing.T) {
	q := sqlc.Into("").
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name is required")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertNoRows(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name")

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one row of values is required")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertNoColumns(t *testing.T) {
	q := sqlc.Into("users").
		Values(1, "John")

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mismatched values count: expected 0 values for 0 columns, got 2")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertRecordsEmpty(t *testing.T) {
	q := sqlc.Into("users").Records()

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expects at least one record")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertRecordNoDbTags(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	user := User{ID: 1, Name: "John"}
	q := sqlc.Into("users").Records(user)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "records have no db tags")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertImmutability(t *testing.T) {
	base := sqlc.Into("users").Columns("id", "name")

	q1 := base.Values(1, "John")
	q2 := base.Values(2, "Jane")

	sql1, params1, err1 := q1.ToSql()
	require.NoError(t, err1)

	sql2, params2, err2 := q2.ToSql()
	require.NoError(t, err2)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", sql1)
	assert.Equal(t, []any{1, "John"}, params1)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", sql2)
	assert.Equal(t, []any{2, "Jane"}, params2)
}

func TestInsertErrorPersistsAcrossCalls(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1) // Error: mismatched count

	// Chain more methods - error should persist
	q = q.Values(2, "Jane")

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mismatched values count")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertWithClient(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John").
		WithClient(mockClient)

	ctx := context.Background()

	mockClient.On("Exec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", []any{1, "John"}).Return(nil, nil)

	_, err := q.Exec(ctx)

	assert.NoError(t, err)
}

func TestInsertExecWithoutClient(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John")

	ctx := context.Background()

	_, err := q.Exec(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no client set")
}

func TestInsertExecWithBuildError(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1). // Error: mismatched count
		WithClient(mockClient)

	ctx := context.Background()

	_, err := q.Exec(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not build sql")
}

func TestInsertExecWithClientError(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John").
		WithClient(mockClient)

	ctx := context.Background()

	mockClient.On("Exec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", []any{1, "John"}).
		Return(nil, fmt.Errorf("database connection error"))

	_, err := q.Exec(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection error")
}

func TestQueryBuilderInto(t *testing.T) {
	mockClient := mocks.NewClient(t)

	// For this test, we'll use Into directly with WithClient
	q := sqlc.Into("users").
		WithClient(mockClient).
		Columns("id", "name").
		Values(1, "John")

	ctx := context.Background()

	mockClient.On("Exec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", []any{1, "John"}).
		Return(nil, nil)

	_, err := q.Exec(ctx)

	assert.NoError(t, err)
}

func TestInsertMixedValuesAndValuesRows(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John").
		ValuesRows(
			[]any{2, "Jane"},
			[]any{3, "Bob"},
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?), (?, ?)", sql)
	assert.Equal(t, []any{1, "John", 2, "Jane", 3, "Bob"}, params)
}

func TestInsertRecordWithExplicitColumns(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := User{ID: 1, Name: "John", Email: "john@example.com"}
	q := sqlc.Into("users").
		Columns("id", "name", "email").
		Records(user)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// ToSql extracts values and uses positional parameters
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com"}, params)
}

func TestInsertPartialStructFields(t *testing.T) {
	type User struct {
		ID     int    `db:"id"`
		Name   string `db:"name"`
		Email  string `db:"email"`
		NoTag  string // No db tag
		Status string `db:"status"`
	}

	user := User{
		ID:     1,
		Name:   "John",
		Email:  "john@example.com",
		NoTag:  "ignored",
		Status: "active",
	}
	q := sqlc.Into("users").Records(user)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Should only include fields with db tags; ToSql uses positional parameters
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`, `status`) VALUES (?, ?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", "active"}, params)
}

func TestInsertWithClientImmutability(t *testing.T) {
	mock1 := mocks.NewClient(t)
	mock2 := mocks.NewClient(t)

	base := sqlc.Into("users").Columns("id", "name")
	q1 := base.WithClient(mock1).Values(1, "John")
	q2 := base.WithClient(mock2).Values(2, "Jane")

	ctx := context.Background()

	mock1.On("Exec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", []any{1, "John"}).Return(nil, nil).Once()
	mock2.On("Exec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", []any{2, "Jane"}).Return(nil, nil).Once()

	_, err1 := q1.Exec(ctx)
	_, err2 := q2.Exec(ctx)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestInsertDifferentDataTypes(t *testing.T) {
	type Product struct {
		ID       int     `db:"id"`
		Name     string  `db:"name"`
		Price    float64 `db:"price"`
		InStock  bool    `db:"in_stock"`
		Quantity int64   `db:"quantity"`
	}

	product := Product{
		ID:       1,
		Name:     "Widget",
		Price:    19.99,
		InStock:  true,
		Quantity: 100,
	}

	q := sqlc.Into("products").Records(product)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// ToSql uses positional parameters and extracts values
	assert.Equal(t, "INSERT INTO `products` (`id`, `name`, `price`, `in_stock`, `quantity`) VALUES (?, ?, ?, ?, ?)", sql)
	assert.Equal(t, []any{1, "Widget", 19.99, true, int64(100)}, params)
}

func TestInsertColumnQuoting(t *testing.T) {
	q := sqlc.Into("users").
		Columns("user.id", "user.name", "metadata->'$.email'").
		Values(1, "John", "john@example.com")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Table-qualified and JSON columns should be quoted correctly
	assert.Equal(t, "INSERT INTO `users` (`user`.`id`, `user`.`name`, `metadata`->'$.email') VALUES (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com"}, params)
}

func TestInsertToNamedSqlSingleRecord(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := User{ID: 1, Name: "John", Email: "john@example.com"}
	q := sqlc.Into("users").Records(user)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// ToNamedSql returns named parameters and untouched records
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, user, params[0])
}

func TestInsertToNamedSqlMultipleRecords(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user1 := User{ID: 1, Name: "John", Email: "john@example.com"}
	user2 := User{ID: 2, Name: "Jane", Email: "jane@example.com"}
	user3 := User{ID: 3, Name: "Bob", Email: "bob@example.com"}
	q := sqlc.Into("users").Records(user1, user2, user3)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// ToNamedSql returns named parameters and untouched records
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)", sql)
	assert.Len(t, params, 3)
	assert.Equal(t, user1, params[0])
	assert.Equal(t, user2, params[1])
	assert.Equal(t, user3, params[2])
}

func TestInsertToNamedSqlWithPointers(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user1 := &User{ID: 1, Name: "John", Email: "john@example.com"}
	user2 := &User{ID: 2, Name: "Jane", Email: "jane@example.com"}
	q := sqlc.Into("users").Records(user1, user2)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// ToNamedSql returns named parameters and untouched record pointers
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, user1, params[0])
	assert.Equal(t, user2, params[1])
}

func TestInsertValuesMapsToNamedSql(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "John", "email": "john@example.com"}
	map2 := map[string]any{"id": 2, "name": "Jane", "email": "jane@example.com"}

	q := sqlc.Into("users").ValuesMaps(map1, map2)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// Columns are inferred from map keys in alphabetical order
	// Named parameters use the map keys
	assert.Equal(t, "INSERT INTO `users` (`email`, `id`, `name`) VALUES (:email, :id, :name)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, map1, params[0])
	assert.Equal(t, map2, params[1])
}

func TestInsertValuesMapsWithExplicitColumns(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "John", "email": "john@example.com"}
	map2 := map[string]any{"id": 2, "name": "Jane", "email": "jane@example.com"}

	q := sqlc.Into("users").
		Columns("id", "name", "email").
		ValuesMaps(map1, map2)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// Named parameters use the explicit column names
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (:id, :name, :email)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, map1, params[0])
	assert.Equal(t, map2, params[1])
}

func TestInsertValuesMapsEmpty(t *testing.T) {
	q := sqlc.Into("users").ValuesMaps()

	sql, params, err := q.ToNamedSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expects at least one map")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertValuesMapsMissingColumn(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "email").
		ValuesMaps(
			map[string]any{"id": 1, "name": "John"}, // Missing email
		)

	// Error is caught during ValuesMaps call
	sql, params, err := q.ToNamedSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column: email")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertValuesMapsExtraKeys(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		ValuesMaps(
			map[string]any{"id": 1, "name": "John", "email": "john@example.com"}, // Extra key
		)

	// Error is caught during ValuesMaps call
	sql, params, err := q.ToNamedSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "has 3 keys but 2 columns are defined")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestInsertValuesMapsMultipleCalls(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "John"}
	map2 := map[string]any{"id": 2, "name": "Jane"}

	q := sqlc.Into("users").
		ValuesMaps(map1).
		ValuesMaps(map2)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, map1, params[0])
	assert.Equal(t, map2, params[1])
}

func TestInsertValuesMapsWithDifferentTypes(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "Widget", "price": 19.99, "in_stock": true, "quantity": int64(100)}
	map2 := map[string]any{"id": 2, "name": "Gadget", "price": 29.99, "in_stock": false, "quantity": int64(50)}

	q := sqlc.Into("products").ValuesMaps(map1, map2)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// Columns are inferred from map keys in alphabetical order
	assert.Equal(t, "INSERT INTO `products` (`id`, `in_stock`, `name`, `price`, `quantity`) VALUES (:id, :in_stock, :name, :price, :quantity)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, map1, params[0])
	assert.Equal(t, map2, params[1])
}

func TestInsertValuesMapsImmutability(t *testing.T) {
	base := sqlc.Into("users")

	map1 := map[string]any{"id": 1, "name": "John"}
	map2 := map[string]any{"id": 2, "name": "Jane"}

	q1 := base.ValuesMaps(map1)
	q2 := base.ValuesMaps(map2)

	sql1, params1, err1 := q1.ToNamedSql()
	require.NoError(t, err1)

	sql2, params2, err2 := q2.ToNamedSql()
	require.NoError(t, err2)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)", sql1)
	assert.Len(t, params1, 1)
	assert.Equal(t, map1, params1[0])

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)", sql2)
	assert.Len(t, params2, 1)
	assert.Equal(t, map2, params2[0])
}

func TestInsertValuesMapsWithClient(t *testing.T) {
	mockClient := mocks.NewClient(t)

	m := map[string]any{"id": 1, "name": "John"}
	q := sqlc.Into("users").
		ValuesMaps(m).
		WithClient(mockClient)

	ctx := context.Background()

	mockClient.On("NamedExec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)", m).Return(nil, nil)

	_, err := q.Exec(ctx)

	assert.NoError(t, err)
}

func TestInsertValuesMapsWithClientBatch(t *testing.T) {
	mockClient := mocks.NewClient(t)

	maps := []map[string]any{
		{"id": 1, "name": "John"},
		{"id": 2, "name": "Jane"},
	}

	q := sqlc.Into("users").
		ValuesMaps(maps[0], maps[1]).
		WithClient(mockClient)

	ctx := context.Background()

	// NamedExec with batch receives slice of maps
	expectedMaps := []any{maps[0], maps[1]}
	mockClient.On("NamedExec", ctx, "INSERT INTO `users` (`id`, `name`) VALUES (:id, :name)", expectedMaps).Return(nil, nil)

	_, err := q.Exec(ctx)

	assert.NoError(t, err)
}

func TestInsertValuesMapsColumnOrderConsistency(t *testing.T) {
	// Even though maps are unordered, the query builder should produce consistent column order
	map1 := map[string]any{"name": "John", "id": 1, "email": "john@example.com"}
	map2 := map[string]any{"email": "jane@example.com", "id": 2, "name": "Jane"}

	q := sqlc.Into("users").ValuesMaps(map1, map2)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	// Columns should be sorted alphabetically
	assert.Equal(t, "INSERT INTO `users` (`email`, `id`, `name`) VALUES (:email, :id, :name)", sql)
	assert.Len(t, params, 2)
}

func TestInsertValuesMapsToSql(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "John", "email": "john@example.com"}
	map2 := map[string]any{"id": 2, "name": "Jane", "email": "jane@example.com"}

	q := sqlc.Into("users").ValuesMaps(map1, map2)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Columns are inferred from map keys in alphabetical order
	// Values extracted in column order
	assert.Equal(t, "INSERT INTO `users` (`email`, `id`, `name`) VALUES (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{"john@example.com", 1, "John", "jane@example.com", 2, "Jane"}, params)
}

func TestInsertValuesMapsToSqlWithExplicitColumns(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "John", "email": "john@example.com"}
	map2 := map[string]any{"id": 2, "name": "Jane", "email": "jane@example.com"}

	q := sqlc.Into("users").
		Columns("id", "name", "email").
		ValuesMaps(map1, map2)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Values extracted in explicit column order
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)", sql)
	assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com"}, params)
}

func TestInsertValuesMapsToSqlSingleMap(t *testing.T) {
	m := map[string]any{"id": 1, "name": "John"}

	q := sqlc.Into("users").ValuesMaps(m)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestInsertValuesMapsToSqlWithDifferentTypes(t *testing.T) {
	map1 := map[string]any{"id": 1, "name": "Widget", "price": 19.99, "in_stock": true}
	map2 := map[string]any{"id": 2, "name": "Gadget", "price": 29.99, "in_stock": false}

	q := sqlc.Into("products").ValuesMaps(map1, map2)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Columns are sorted alphabetically
	assert.Equal(t, "INSERT INTO `products` (`id`, `in_stock`, `name`, `price`) VALUES (?, ?, ?, ?), (?, ?, ?, ?)", sql)
	assert.Equal(t, []any{1, true, "Widget", 19.99, 2, false, "Gadget", 29.99}, params)
}

func TestInsertMixedValuesMapsAndValuesWithToSql(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John").
		ValuesMaps(map[string]any{"id": 2, "name": "Jane"})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?)", sql)
	assert.Equal(t, []any{1, "John", 2, "Jane"}, params)
}

func TestInsertMode(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestReplaceMode(t *testing.T) {
	q := sqlc.Into("users").
		Replace().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "REPLACE INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestReplaceModeWithMultipleRows(t *testing.T) {
	q := sqlc.Into("users").
		Replace().
		Columns("id", "name").
		Values(1, "John").
		Values(2, "Jane")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "REPLACE INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?)", sql)
	assert.Equal(t, []any{1, "John", 2, "Jane"}, params)
}

func TestReplaceModeWithRecords(t *testing.T) {
	type User struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	user := User{ID: 1, Name: "John"}

	q := sqlc.Into("users").Replace().Records(user)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	assert.Equal(t, "REPLACE INTO `users` (`id`, `name`) VALUES (:id, :name)", sql)
	assert.Len(t, params, 1)
}

func TestReplaceModeWithValuesMaps(t *testing.T) {
	q := sqlc.Into("users").
		Replace().
		ValuesMaps(
			map[string]any{"id": 1, "name": "John"},
			map[string]any{"id": 2, "name": "Jane"},
		)

	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	assert.Equal(t, "REPLACE INTO `users` (`id`, `name`) VALUES (:id, :name)", sql)
	assert.Len(t, params, 2)
}

func TestInsertToReplaceToggle(t *testing.T) {
	// Start with INSERT (default)
	q1 := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John")

	sql1, _, err := q1.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql1, "INSERT INTO")

	// Toggle to REPLACE
	q2 := q1.Replace()

	sql2, _, err := q2.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql2, "REPLACE INTO")

	// Toggle back to INSERT
	q3 := q2.Insert()

	sql3, _, err := q3.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql3, "INSERT INTO")

	// Verify immutability - original query should still be INSERT
	sql1Again, _, err := q1.ToSql()
	require.NoError(t, err)
	assert.Contains(t, sql1Again, "INSERT INTO")
}

func TestReplaceModeImmutability(t *testing.T) {
	base := sqlc.Into("users").Columns("id", "name")

	insert := base.Insert().Values(1, "John")
	replace := base.Replace().Values(2, "Jane")

	insertSql, _, err := insert.ToSql()
	require.NoError(t, err)
	assert.Contains(t, insertSql, "INSERT INTO")

	replaceSql, _, err := replace.ToSql()
	require.NoError(t, err)
	assert.Contains(t, replaceSql, "REPLACE INTO")
}

func TestReplaceModeWithClient(t *testing.T) {
	mockClient := mocks.NewClient(t)
	ctx := context.Background()

	mockClient.On("Exec", ctx, "REPLACE INTO `users` (`id`, `name`) VALUES (?, ?)", []any{1, "John"}).
		Return(nil, nil)

	q := sqlc.Into("users").
		Replace().
		Columns("id", "name").
		Values(1, "John").
		WithClient(mockClient)

	_, err := q.Exec(ctx)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

// Modifier tests

func TestIgnoreModifier(t *testing.T) {
	q := sqlc.Into("users").
		Ignore().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestLowPriorityModifier(t *testing.T) {
	q := sqlc.Into("users").
		LowPriority().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT LOW_PRIORITY INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestHighPriorityModifier(t *testing.T) {
	q := sqlc.Into("users").
		HighPriority().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT HIGH_PRIORITY INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestDelayedModifier(t *testing.T) {
	q := sqlc.Into("users").
		Delayed().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT DELAYED INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestIgnoreWithLowPriority(t *testing.T) {
	q := sqlc.Into("users").
		LowPriority().
		Ignore().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT LOW_PRIORITY IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestIgnoreWithHighPriority(t *testing.T) {
	q := sqlc.Into("users").
		HighPriority().
		Ignore().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT HIGH_PRIORITY IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestReplaceWithIgnore(t *testing.T) {
	q := sqlc.Into("users").
		Replace().
		Ignore().
		Columns("id", "name").
		Values(1, "John")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "REPLACE IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
	assert.Equal(t, []any{1, "John"}, params)
}

func TestModifierImmutability(t *testing.T) {
	base := sqlc.Into("users").
		Columns("id", "name").
		Values(1, "John")

	// Create multiple variants from base
	ignore := base.Ignore()
	lowPriority := base.LowPriority()
	highPriority := base.HighPriority()

	// Verify base is unmodified
	baseSql, _, err := base.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", baseSql)

	// Verify ignore variant
	ignoreSql, _, err := ignore.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT IGNORE INTO `users` (`id`, `name`) VALUES (?, ?)", ignoreSql)

	// Verify low priority variant
	lowPrioritySql, _, err := lowPriority.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT LOW_PRIORITY INTO `users` (`id`, `name`) VALUES (?, ?)", lowPrioritySql)

	// Verify high priority variant
	highPrioritySql, _, err := highPriority.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT HIGH_PRIORITY INTO `users` (`id`, `name`) VALUES (?, ?)", highPrioritySql)
}

// ON DUPLICATE KEY UPDATE tests

func TestOnDuplicateKeyUpdateWithValue(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "count").
		Values(1, "John", 5).
		OnDuplicateKeyUpdate(
			sqlc.Assign("count", 10),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = ?", sql)
	assert.Equal(t, []any{1, "John", 5, 10}, params)
}

func TestOnDuplicateKeyUpdateWithExpression(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "count").
		Values(1, "John", 5).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + VALUES(count)"),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)", sql)
	assert.Equal(t, []any{1, "John", 5}, params)
}

func TestOnDuplicateKeyUpdateMultipleAssignments(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "count", "updated_at").
		Values(1, "John", 5, "2024-01-01").
		OnDuplicateKeyUpdate(
			sqlc.Assign("name", "UpdatedJohn"),
			sqlc.AssignExpr("count", "count + VALUES(count)"),
			sqlc.AssignExpr("updated_at", "NOW()"),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`, `updated_at`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `count` = count + VALUES(count), `updated_at` = NOW()", sql)
	assert.Equal(t, []any{1, "John", 5, "2024-01-01", "UpdatedJohn"}, params)
}

func TestOnDuplicateKeyUpdateWithMultipleRows(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "count").
		Values(1, "John", 5).
		Values(2, "Jane", 3).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + VALUES(count)"),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?), (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)", sql)
	assert.Equal(t, []any{1, "John", 5, 2, "Jane", 3}, params)
}

func TestOnDuplicateKeyUpdateWithRecords(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Count int    `db:"count"`
	}

	user := User{ID: 1, Name: "John", Count: 5}

	q := sqlc.Into("users").
		Records(user).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + VALUES(count)"),
		)

	// Test ToNamedSql
	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (:id, :name, :count) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)", sql)
	assert.Equal(t, []any{user}, params)

	// Test ToSql
	sqlPositional, paramsPositional, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)", sqlPositional)
	assert.Equal(t, []any{1, "John", 5}, paramsPositional)
}

func TestOnDuplicateKeyUpdateWithValuesMaps(t *testing.T) {
	q := sqlc.Into("users").
		ValuesMaps(
			map[string]any{"id": 1, "name": "John", "count": 5},
		).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + VALUES(count)"),
		)

	// Test ToNamedSql
	sql, params, err := q.ToNamedSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`count`, `id`, `name`) VALUES (:count, :id, :name) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)", sql)
	assert.Len(t, params, 1)

	// Test ToSql
	sqlPositional, paramsPositional, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`count`, `id`, `name`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)", sqlPositional)
	assert.Equal(t, []any{5, 1, "John"}, paramsPositional)
}

func TestOnDuplicateKeyUpdateWithIgnore(t *testing.T) {
	q := sqlc.Into("users").
		Ignore().
		Columns("id", "name", "count").
		Values(1, "John", 5).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + 1"),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT IGNORE INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + 1", sql)
	assert.Equal(t, []any{1, "John", 5}, params)
}

func TestOnDuplicateKeyUpdateWithReplace(t *testing.T) {
	q := sqlc.Into("users").
		Replace().
		Columns("id", "name").
		Values(1, "John").
		OnDuplicateKeyUpdate(
			sqlc.Assign("name", "Jane"),
		)

	// REPLACE cannot be used with ON DUPLICATE KEY UPDATE
	_, _, err := q.ToSql()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REPLACE cannot be used with ON DUPLICATE KEY UPDATE")
}

func TestOnDuplicateKeyUpdateImmutability(t *testing.T) {
	base := sqlc.Into("users").
		Columns("id", "name", "count").
		Values(1, "John", 5)

	// Create variant with ON DUPLICATE KEY UPDATE
	withDuplicate := base.OnDuplicateKeyUpdate(
		sqlc.AssignExpr("count", "count + 1"),
	)

	// Verify base is unmodified
	baseSql, _, err := base.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?)", baseSql)

	// Verify withDuplicate has the clause
	duplicateSql, _, err := withDuplicate.ToSql()
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + 1", duplicateSql)
}

func TestOnDuplicateKeyUpdateMultipleCalls(t *testing.T) {
	q := sqlc.Into("users").
		Columns("id", "name", "count").
		Values(1, "John", 5).
		OnDuplicateKeyUpdate(
			sqlc.Assign("name", "UpdatedJohn"),
		).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + 1"),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?, `count` = count + 1", sql)
	assert.Equal(t, []any{1, "John", 5, "UpdatedJohn"}, params)
}

func TestOnDuplicateKeyUpdateWithClientExec(t *testing.T) {
	mockClient := mocks.NewClient(t)
	ctx := context.Background()

	expectedSQL := "INSERT INTO `users` (`id`, `name`, `count`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)"
	mockClient.On("Exec", ctx, expectedSQL, []any{1, "John", 5}).
		Return(nil, nil)

	q := sqlc.Into("users").
		Columns("id", "name", "count").
		Values(1, "John", 5).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + VALUES(count)"),
		).
		WithClient(mockClient)

	_, err := q.Exec(ctx)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestOnDuplicateKeyUpdateWithNamedExec(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Count int    `db:"count"`
	}

	user := User{ID: 1, Name: "John", Count: 5}

	mockClient := mocks.NewClient(t)
	ctx := context.Background()

	expectedSQL := "INSERT INTO `users` (`id`, `name`, `count`) VALUES (:id, :name, :count) ON DUPLICATE KEY UPDATE `count` = count + VALUES(count)"
	mockClient.On("NamedExec", ctx, expectedSQL, user).
		Return(nil, nil)

	q := sqlc.Into("users").
		Records(user).
		OnDuplicateKeyUpdate(
			sqlc.AssignExpr("count", "count + VALUES(count)"),
		).
		WithClient(mockClient)

	_, err := q.Exec(ctx)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}
