package sqlc_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/exec"
	logmocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockExecutor is a simple executor that just runs the function
type mockExecutor struct {
	executeFunc func(ctx context.Context, f exec.Executable) (any, error)
}

func (m *mockExecutor) Execute(ctx context.Context, f exec.Executable, notifier ...exec.Notify) (any, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, f)
	}

	return f(ctx)
}

func newMockExecutor() *mockExecutor {
	return &mockExecutor{}
}

// setupClient creates a client with mocked logger, db, and executor
func setupClient(t *testing.T) (sqlc.Client, sqlmock.Sqlmock, *mockExecutor) {
	t.Helper()

	// Create mock logger
	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)

	// Create mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	// Wrap in sqlx.DB
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	// Create mock executor
	executor := newMockExecutor()

	// Create client with interfaces
	client := sqlc.NewClientWithInterfaces(logger, sqlxDB, executor)

	t.Cleanup(func() {
		// Don't close client here since each test manages its own expectations
		// Just verify all expectations were met
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	return client, mock, executor
}

func TestClientGet(t *testing.T) {
	client, mock, _ := setupClient(t)
	ctx := context.Background()

	t.Run("successful get", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John")

		mock.ExpectQuery("SELECT (.+) FROM users WHERE id = ?").
			WithArgs(1).
			WillReturnRows(rows)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var user User
		err := client.Get(ctx, &user, "SELECT id, name FROM users WHERE id = ?", 1)

		require.NoError(t, err)
		assert.Equal(t, 1, user.ID)
		assert.Equal(t, "John", user.Name)
	})

	t.Run("no rows found", func(t *testing.T) {
		mock.ExpectQuery("SELECT (.+) FROM users WHERE id = ?").
			WithArgs(999).
			WillReturnError(sql.ErrNoRows)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var user User
		err := client.Get(ctx, &user, "SELECT id, name FROM users WHERE id = ?", 999)

		assert.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("database error", func(t *testing.T) {
		dbErr := errors.New("database connection failed")
		mock.ExpectQuery("SELECT (.+) FROM users").
			WillReturnError(dbErr)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var user User
		err := client.Get(ctx, &user, "SELECT id, name FROM users", nil)

		assert.ErrorIs(t, err, dbErr)
	})
}

func TestClientExec(t *testing.T) {
	client, mock, _ := setupClient(t)
	ctx := context.Background()

	t.Run("successful insert", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO users").
			WithArgs("John", "john@example.com").
			WillReturnResult(sqlmock.NewResult(1, 1))

		result, err := client.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")

		require.NoError(t, err)
		lastID, err := result.LastInsertId()
		require.NoError(t, err)
		assert.Equal(t, int64(1), lastID)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)
	})

	t.Run("successful update", func(t *testing.T) {
		mock.ExpectExec("UPDATE users SET name = ?").
			WithArgs("Jane", 1).
			WillReturnResult(sqlmock.NewResult(0, 1))

		result, err := client.Exec(ctx, "UPDATE users SET name = ? WHERE id = ?", "Jane", 1)

		require.NoError(t, err)
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)
	})

	t.Run("successful delete", func(t *testing.T) {
		mock.ExpectExec("DELETE FROM users WHERE id = ?").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(0, 1))

		result, err := client.Exec(ctx, "DELETE FROM users WHERE id = ?", 1)

		require.NoError(t, err)
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)
	})

	t.Run("database error", func(t *testing.T) {
		dbErr := errors.New("constraint violation")
		mock.ExpectExec("INSERT INTO users").
			WithArgs("John", "john@example.com").
			WillReturnError(dbErr)

		result, err := client.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")

		assert.ErrorIs(t, err, dbErr)
		assert.Nil(t, result)
	})
}

func TestClientNamedExec(t *testing.T) {
	client, mock, _ := setupClient(t)
	ctx := context.Background()

	t.Run("successful insert with struct", func(t *testing.T) {
		type User struct {
			Name  string `db:"name"`
			Email string `db:"email"`
		}

		user := User{Name: "John", Email: "john@example.com"}

		mock.ExpectExec("INSERT INTO users").
			WithArgs("John", "john@example.com").
			WillReturnResult(sqlmock.NewResult(1, 1))

		result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", user)

		require.NoError(t, err)
		lastID, err := result.LastInsertId()
		require.NoError(t, err)
		assert.Equal(t, int64(1), lastID)
	})

	t.Run("successful insert with map", func(t *testing.T) {
		params := map[string]any{
			"name":  "Jane",
			"email": "jane@example.com",
		}

		mock.ExpectExec("INSERT INTO users").
			WithArgs("Jane", "jane@example.com").
			WillReturnResult(sqlmock.NewResult(2, 1))

		result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", params)

		require.NoError(t, err)
		lastID, err := result.LastInsertId()
		require.NoError(t, err)
		assert.Equal(t, int64(2), lastID)
	})

	t.Run("database error", func(t *testing.T) {
		type User struct {
			Name  string `db:"name"`
			Email string `db:"email"`
		}

		user := User{Name: "John", Email: "john@example.com"}
		dbErr := errors.New("unique constraint violation")

		mock.ExpectExec("INSERT INTO users").
			WithArgs("John", "john@example.com").
			WillReturnError(dbErr)

		result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", user)

		assert.ErrorIs(t, err, dbErr)
		assert.Nil(t, result)
	})
}

func TestClientSelect(t *testing.T) {
	client, mock, _ := setupClient(t)
	ctx := context.Background()

	t.Run("successful select multiple rows", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John").
			AddRow(2, "Jane").
			AddRow(3, "Bob")

		mock.ExpectQuery("SELECT (.+) FROM users").
			WillReturnRows(rows)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var users []User
		err := client.Select(ctx, &users, "SELECT id, name FROM users")

		require.NoError(t, err)
		assert.Len(t, users, 3)
		assert.Equal(t, "John", users[0].Name)
		assert.Equal(t, "Jane", users[1].Name)
		assert.Equal(t, "Bob", users[2].Name)
	})

	t.Run("select empty result", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"})

		mock.ExpectQuery("SELECT (.+) FROM users WHERE id > ?").
			WithArgs(1000).
			WillReturnRows(rows)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var users []User
		err := client.Select(ctx, &users, "SELECT id, name FROM users WHERE id > ?", 1000)

		require.NoError(t, err)
		assert.Empty(t, users)
	})

	t.Run("database error", func(t *testing.T) {
		dbErr := errors.New("connection timeout")
		mock.ExpectQuery("SELECT (.+) FROM users").
			WillReturnError(dbErr)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var users []User
		err := client.Select(ctx, &users, "SELECT id, name FROM users")

		assert.ErrorIs(t, err, dbErr)
	})
}

func TestClientQuery(t *testing.T) {
	client, mock, _ := setupClient(t)
	ctx := context.Background()

	t.Run("successful query with iteration", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John").
			AddRow(2, "Jane")

		mock.ExpectQuery("SELECT (.+) FROM users").
			WillReturnRows(rows)

		resultRows, err := client.Query(ctx, "SELECT id, name FROM users")
		require.NoError(t, err)
		require.NotNil(t, resultRows)
		defer func() {
			assert.NoError(t, resultRows.Close())
		}()

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var users []User
		for resultRows.Next() {
			var user User
			err := resultRows.StructScan(&user)
			require.NoError(t, err)
			users = append(users, user)
		}

		require.NoError(t, resultRows.Err())
		assert.Len(t, users, 2)
		assert.Equal(t, "John", users[0].Name)
		assert.Equal(t, "Jane", users[1].Name)
	})

	t.Run("database error", func(t *testing.T) {
		dbErr := errors.New("query execution failed")
		mock.ExpectQuery("SELECT (.+) FROM users").
			WillReturnError(dbErr)

		resultRows, err := client.Query(ctx, "SELECT id, name FROM users")

		assert.ErrorIs(t, err, dbErr)
		assert.Nil(t, resultRows)
	})
}

func TestClientQb(t *testing.T) {
	client, _, _ := setupClient(t)

	t.Run("returns query builder", func(t *testing.T) {
		qb := client.Q()

		assert.NotNil(t, qb)
	})
}

func TestClientClose(t *testing.T) {
	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	executor := newMockExecutor()

	client := sqlc.NewClientWithInterfaces(logger, sqlxDB, executor)

	mock.ExpectClose()

	err = client.Close()
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClientWithExecutorRetry(t *testing.T) {
	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)

	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	t.Run("executor is called for operations", func(t *testing.T) {
		executorCalled := false
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, f exec.Executable) (any, error) {
				executorCalled = true

				return f(ctx)
			},
		}

		client := sqlc.NewClientWithInterfaces(logger, sqlxDB, executor)

		// Note: We're not using mock expectations here since this test is focused on executor behavior
		// The actual DB call will fail, but that's okay - we're testing the executor wrapping

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var user User
		// This will actually fail because we're not setting up mock expectations,
		// but the executor will still be called - we're just checking it was invoked
		err := client.Get(context.Background(), &user, "SELECT id, name FROM users WHERE id = ?", 1)
		assert.Error(t, err) // We expect an error since no mock expectations are set

		assert.True(t, executorCalled, "executor should have been called")
	})

	t.Run("executor error is propagated", func(t *testing.T) {
		retryErr := errors.New("persistent error")
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, f exec.Executable) (any, error) {
				return nil, retryErr
			},
		}

		client := sqlc.NewClientWithInterfaces(logger, sqlxDB, executor)

		type User struct {
			ID   int    `db:"id"`
			Name string `db:"name"`
		}

		var user User
		err := client.Get(context.Background(), &user, "SELECT id, name FROM users WHERE id = ?", 1)

		assert.ErrorIs(t, err, retryErr)
	})
}

func TestClientIntegrationWithQueryBuilder(t *testing.T) {
	client, mock, _ := setupClient(t)
	ctx := context.Background()

	t.Run("query builder with client", func(t *testing.T) {
		mock.ExpectExec("INSERT INTO `users`").
			WithArgs("John", "john@example.com").
			WillReturnResult(sqlmock.NewResult(1, 1))

		result, err := client.Q().
			Into("users").
			Columns("name", "email").
			Values("John", "john@example.com").
			WithClient(client).
			Exec(ctx)

		require.NoError(t, err)
		lastID, err := result.LastInsertId()
		require.NoError(t, err)
		assert.Equal(t, int64(1), lastID)
	})
}
