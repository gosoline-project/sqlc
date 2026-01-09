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
	"github.com/stretchr/testify/suite"
)

// User is a common test struct used across multiple tests
type User struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

// ClientTestSuite is the base test suite for client tests
type ClientTestSuite struct {
	suite.Suite
	client sqlc.Client
	mock   sqlmock.Sqlmock
	ctx    context.Context
}

// SetupTest runs before each test in the suite
func (s *ClientTestSuite) SetupTest() {
	s.ctx = context.Background()

	// Create mock logger
	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(s.T()), logmocks.WithMockAll)

	// Create mock database
	mockDB, mock, err := sqlmock.New()
	s.Require().NoError(err)

	// Wrap in sqlx.DB
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	s.mock = mock

	// Create default config
	qbConfig := sqlc.DefaultConfig()

	// Create client with default executor
	s.client = sqlc.NewClientWithInterfaces(logger, sqlxDB, exec.NewDefaultExecutor(), qbConfig)
}

// TearDownTest runs after each test in the suite
func (s *ClientTestSuite) TearDownTest() {
	s.Assert().NoError(s.mock.ExpectationsWereMet())
}

// TestClientSuite runs the client test suite
func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

// -----------------------------------------------------------------------------
// Get Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestGet_Success() {
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "John")

	s.mock.ExpectQuery("SELECT (.+) FROM users WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)

	var user User
	err := s.client.Get(s.ctx, &user, "SELECT id, name FROM users WHERE id = ?", 1)

	s.Require().NoError(err)
	s.Assert().Equal(1, user.ID)
	s.Assert().Equal("John", user.Name)
}

func (s *ClientTestSuite) TestGet_NoRowsFound() {
	s.mock.ExpectQuery("SELECT (.+) FROM users WHERE id = ?").
		WithArgs(999).
		WillReturnError(sql.ErrNoRows)

	var user User
	err := s.client.Get(s.ctx, &user, "SELECT id, name FROM users WHERE id = ?", 999)

	s.Assert().ErrorIs(err, sql.ErrNoRows)
}

func (s *ClientTestSuite) TestGet_DatabaseError() {
	dbErr := errors.New("database connection failed")
	s.mock.ExpectQuery("SELECT (.+) FROM users").
		WillReturnError(dbErr)

	var user User
	err := s.client.Get(s.ctx, &user, "SELECT id, name FROM users", nil)

	s.Assert().ErrorIs(err, dbErr)
}

// -----------------------------------------------------------------------------
// Exec Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestExec_Insert() {
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := s.client.Exec(s.ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")

	s.Require().NoError(err)
	lastID, err := result.LastInsertId()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), lastID)

	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), rowsAffected)
}

func (s *ClientTestSuite) TestExec_Update() {
	s.mock.ExpectExec("UPDATE users SET name = ?").
		WithArgs("Jane", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := s.client.Exec(s.ctx, "UPDATE users SET name = ? WHERE id = ?", "Jane", 1)

	s.Require().NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), rowsAffected)
}

func (s *ClientTestSuite) TestExec_Delete() {
	s.mock.ExpectExec("DELETE FROM users WHERE id = ?").
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := s.client.Exec(s.ctx, "DELETE FROM users WHERE id = ?", 1)

	s.Require().NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), rowsAffected)
}

func (s *ClientTestSuite) TestExec_DatabaseError() {
	dbErr := errors.New("constraint violation")
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnError(dbErr)

	result, err := s.client.Exec(s.ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")

	s.Assert().ErrorIs(err, dbErr)
	s.Assert().Nil(result)
}

// -----------------------------------------------------------------------------
// NamedExec Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestNamedExec_WithStruct() {
	user := User{Name: "John", Email: "john@example.com"}

	// sqlx binds named params in the order they appear in the SQL: :name, :email
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := s.client.NamedExec(s.ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", user)

	s.Require().NoError(err)
	lastID, err := result.LastInsertId()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), lastID)
}

func (s *ClientTestSuite) TestNamedExec_WithMap() {
	params := map[string]any{
		"name":  "Jane",
		"email": "jane@example.com",
	}

	// sqlx binds named params in the order they appear in the SQL: :name, :email
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("Jane", "jane@example.com").
		WillReturnResult(sqlmock.NewResult(2, 1))

	result, err := s.client.NamedExec(s.ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", params)

	s.Require().NoError(err)
	lastID, err := result.LastInsertId()
	s.Require().NoError(err)
	s.Assert().Equal(int64(2), lastID)
}

func (s *ClientTestSuite) TestNamedExec_DatabaseError() {
	user := User{Name: "John", Email: "john@example.com"}
	dbErr := errors.New("unique constraint violation")

	// sqlx binds named params in the order they appear in the SQL: :name, :email
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnError(dbErr)

	result, err := s.client.NamedExec(s.ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", user)

	s.Assert().ErrorIs(err, dbErr)
	s.Assert().Nil(result)
}

// -----------------------------------------------------------------------------
// Select Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestSelect_MultipleRows() {
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "John").
		AddRow(2, "Jane").
		AddRow(3, "Bob")

	s.mock.ExpectQuery("SELECT (.+) FROM users").
		WillReturnRows(rows)

	var users []User
	err := s.client.Select(s.ctx, &users, "SELECT id, name FROM users")

	s.Require().NoError(err)
	s.Assert().Len(users, 3)
	s.Assert().Equal("John", users[0].Name)
	s.Assert().Equal("Jane", users[1].Name)
	s.Assert().Equal("Bob", users[2].Name)
}

func (s *ClientTestSuite) TestSelect_EmptyResult() {
	rows := sqlmock.NewRows([]string{"id", "name"})

	s.mock.ExpectQuery("SELECT (.+) FROM users WHERE id > ?").
		WithArgs(1000).
		WillReturnRows(rows)

	var users []User
	err := s.client.Select(s.ctx, &users, "SELECT id, name FROM users WHERE id > ?", 1000)

	s.Require().NoError(err)
	s.Assert().Empty(users)
}

func (s *ClientTestSuite) TestSelect_DatabaseError() {
	dbErr := errors.New("connection timeout")
	s.mock.ExpectQuery("SELECT (.+) FROM users").
		WillReturnError(dbErr)

	var users []User
	err := s.client.Select(s.ctx, &users, "SELECT id, name FROM users")

	s.Assert().ErrorIs(err, dbErr)
}

// -----------------------------------------------------------------------------
// Query Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestQuery_WithIteration() {
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "John").
		AddRow(2, "Jane")

	s.mock.ExpectQuery("SELECT (.+) FROM users").
		WillReturnRows(rows)

	resultRows, err := s.client.Query(s.ctx, "SELECT id, name FROM users")
	s.Require().NoError(err)
	s.Require().NotNil(resultRows)
	defer func() {
		s.Assert().NoError(resultRows.Close())
	}()

	var users []User
	for resultRows.Next() {
		var user User
		err := resultRows.StructScan(&user)
		s.Require().NoError(err)
		users = append(users, user)
	}

	s.Require().NoError(resultRows.Err())
	s.Assert().Len(users, 2)
	s.Assert().Equal("John", users[0].Name)
	s.Assert().Equal("Jane", users[1].Name)
}

func (s *ClientTestSuite) TestQuery_DatabaseError() {
	dbErr := errors.New("query execution failed")
	s.mock.ExpectQuery("SELECT (.+) FROM users").
		WillReturnError(dbErr)

	resultRows, err := s.client.Query(s.ctx, "SELECT id, name FROM users")

	s.Assert().ErrorIs(err, dbErr)
	s.Assert().Nil(resultRows)
}

// -----------------------------------------------------------------------------
// QueryBuilder Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestQ_ReturnsQueryBuilder() {
	qb := s.client.Q()
	s.Assert().NotNil(qb)
}

func (s *ClientTestSuite) TestQ_IntegrationWithClient() {
	s.mock.ExpectExec("INSERT INTO `users`").
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := s.client.Q().
		Into("users").
		Columns("name", "email").
		Values("John", "john@example.com").
		WithClient(s.client).
		Exec(s.ctx)

	s.Require().NoError(err)
	lastID, err := result.LastInsertId()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), lastID)
}

// -----------------------------------------------------------------------------
// WithTx Tests
// -----------------------------------------------------------------------------

func (s *ClientTestSuite) TestWithTx_SuccessfulCommit() {
	s.mock.ExpectBegin()
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mock.ExpectCommit()

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		_, err := tx.Exec(s.ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")
		return err
	})

	s.Require().NoError(err)
}

func (s *ClientTestSuite) TestWithTx_FunctionErrorRollsBack() {
	fnErr := errors.New("function failed")

	s.mock.ExpectBegin()
	s.mock.ExpectRollback()

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		return fnErr
	})

	s.Require().ErrorIs(err, fnErr)
}

func (s *ClientTestSuite) TestWithTx_DatabaseErrorRollsBack() {
	dbErr := errors.New("database constraint violation")

	s.mock.ExpectBegin()
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnError(dbErr)
	s.mock.ExpectRollback()

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		_, err := tx.Exec(s.ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")
		return err
	})

	s.Require().ErrorIs(err, dbErr)
}

func (s *ClientTestSuite) TestWithTx_BeginFailure() {
	beginErr := errors.New("failed to begin transaction")
	s.mock.ExpectBegin().WillReturnError(beginErr)

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		return nil
	})

	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "failed to begin transaction")
}

func (s *ClientTestSuite) TestWithTx_CommitFailure() {
	s.mock.ExpectBegin()
	s.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		return nil
	})

	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "transaction commit failed")
}

func (s *ClientTestSuite) TestWithTx_RollbackFailureIncludesOriginalError() {
	fnErr := errors.New("function error")

	s.mock.ExpectBegin()
	s.mock.ExpectRollback().WillReturnError(errors.New("rollback failed"))

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		return fnErr
	})

	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "transaction rollback failed")
	s.Assert().Contains(err.Error(), "function error")
}

func (s *ClientTestSuite) TestWithTx_MultipleOperations() {
	s.mock.ExpectBegin()
	s.mock.ExpectExec("INSERT INTO users").
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mock.ExpectExec("INSERT INTO profiles").
		WithArgs(1, "Developer").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mock.ExpectCommit()

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		result, err := tx.Exec(s.ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")
		if err != nil {
			return err
		}

		userID, err := result.LastInsertId()
		if err != nil {
			return err
		}

		_, err = tx.Exec(s.ctx, "INSERT INTO profiles (user_id, role) VALUES (?, ?)", userID, "Developer")
		return err
	})

	s.Require().NoError(err)
}

func (s *ClientTestSuite) TestWithTx_WithCustomOptions() {
	s.mock.ExpectBegin()
	s.mock.ExpectCommit()

	opts := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	}

	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		return nil
	}, opts)

	s.Require().NoError(err)
}

func (s *ClientTestSuite) TestWithTx_UsesTransactionForQueries() {
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John")

	s.mock.ExpectBegin()
	s.mock.ExpectQuery("SELECT (.+) FROM users WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)
	s.mock.ExpectCommit()

	var user User
	err := s.client.WithTx(s.ctx, func(tx sqlc.Tx) error {
		return tx.Get(s.ctx, &user, "SELECT id, name FROM users WHERE id = ?", 1)
	})

	s.Require().NoError(err)
	s.Assert().Equal(1, user.ID)
	s.Assert().Equal("John", user.Name)
}

// -----------------------------------------------------------------------------
// Close Test (standalone - needs separate setup)
// -----------------------------------------------------------------------------

func TestClientClose(t *testing.T) {
	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	qbConfig := sqlc.DefaultConfig()

	client := sqlc.NewClientWithInterfaces(logger, sqlxDB, exec.NewDefaultExecutor(), qbConfig)

	mock.ExpectClose()

	err = client.Close()
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -----------------------------------------------------------------------------
// Executor Tests (standalone - needs custom executor setup)
// -----------------------------------------------------------------------------

// mockExecutor is used to test executor behavior
type mockExecutor struct {
	executeFunc func(ctx context.Context, f exec.Executable) (any, error)
}

func (m *mockExecutor) Execute(ctx context.Context, f exec.Executable, _ ...exec.Notify) (any, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, f)
	}

	return f(ctx)
}

func TestClientWithExecutorRetry(t *testing.T) {
	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)

	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	qbConfig := sqlc.DefaultConfig()

	t.Run("executor is called for operations", func(t *testing.T) {
		executorCalled := false
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, f exec.Executable) (any, error) {
				executorCalled = true

				return f(ctx)
			},
		}

		client := sqlc.NewClientWithInterfaces(logger, sqlxDB, executor, qbConfig)

		var user User
		// This will fail because no mock expectations are set,
		// but the executor will still be called
		err := client.Get(context.Background(), &user, "SELECT id, name FROM users WHERE id = ?", 1)
		assert.Error(t, err)

		assert.True(t, executorCalled, "executor should have been called")
	})

	t.Run("executor error is propagated", func(t *testing.T) {
		retryErr := errors.New("persistent error")
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, f exec.Executable) (any, error) {
				return nil, retryErr
			},
		}

		client := sqlc.NewClientWithInterfaces(logger, sqlxDB, executor, qbConfig)

		var user User
		err := client.Get(context.Background(), &user, "SELECT id, name FROM users WHERE id = ?", 1)

		assert.ErrorIs(t, err, retryErr)
	})
}
