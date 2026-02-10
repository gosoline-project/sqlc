package sqlc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	mocks "github.com/gosoline-project/sqlc/mocks"
	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/exec"
	logmocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// -----------------------------------------------------------------------------
// PreparedTestSuite — integration tests using sqlmock
// -----------------------------------------------------------------------------

// PreparedTestSuite tests prepared statement functionality using sqlmock.
type PreparedTestSuite struct {
	suite.Suite
	client sqlc.Client
	mock   sqlmock.Sqlmock
	ctx    context.Context
}

// SetupTest runs before each test in the suite.
func (s *PreparedTestSuite) SetupTest() {
	s.ctx = context.Background()

	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(s.T()), logmocks.WithMockAll)

	mockDB, mock, err := sqlmock.New()
	s.Require().NoError(err)

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	s.mock = mock

	qbConfig := sqlc.DefaultConfig()
	s.client = sqlc.NewClientWithInterfaces(logger, sqlxDB, exec.NewDefaultExecutor(), qbConfig)
}

// TearDownTest runs after each test in the suite.
func (s *PreparedTestSuite) TearDownTest() {
	s.Assert().NoError(s.mock.ExpectationsWereMet())
}

// TestPreparedSuite runs the prepared statement test suite.
func TestPreparedSuite(t *testing.T) {
	suite.Run(t, new(PreparedTestSuite))
}

// -----------------------------------------------------------------------------
// PreparedSelect — Get
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelect_Get() {
	ep := s.mock.ExpectPrepare("SELECT `id`, `name` FROM `users` WHERE status = \\?")
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	prepared, err := sqlc.From("users").
		WithClient(s.client).
		Columns("id", "name").
		Where("status = ?", "active").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	var user User
	err = prepared.Get(s.ctx, &user, "active")
	s.Require().NoError(err)
	s.Assert().Equal(1, user.ID)
	s.Assert().Equal("John", user.Name)
}

// -----------------------------------------------------------------------------
// PreparedSelect — Select
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelect_Select() {
	ep := s.mock.ExpectPrepare("SELECT `id`, `name` FROM `users` WHERE status = \\?")
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "name"}).
				AddRow(1, "John").
				AddRow(2, "Jane"),
		)

	prepared, err := sqlc.From("users").
		WithClient(s.client).
		Columns("id", "name").
		Where("status = ?", "active").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	var users []User
	err = prepared.Select(s.ctx, &users, "active")
	s.Require().NoError(err)
	s.Assert().Len(users, 2)
	s.Assert().Equal("John", users[0].Name)
	s.Assert().Equal("Jane", users[1].Name)
}

// -----------------------------------------------------------------------------
// PreparedSelect — Query (row iteration)
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelect_Query() {
	ep := s.mock.ExpectPrepare("SELECT `id`, `name` FROM `users` WHERE status = \\?")
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "name"}).
				AddRow(1, "John").
				AddRow(2, "Jane"),
		)

	prepared, err := sqlc.From("users").
		WithClient(s.client).
		Columns("id", "name").
		Where("status = ?", "active").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	rows, err := prepared.Query(s.ctx, "active")
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(rows.Close())
	}()

	var users []User
	for rows.Next() {
		var user User
		err := rows.StructScan(&user)
		s.Require().NoError(err)
		users = append(users, user)
	}

	s.Require().NoError(rows.Err())
	s.Assert().Len(users, 2)
	s.Assert().Equal("John", users[0].Name)
	s.Assert().Equal("Jane", users[1].Name)
}

// -----------------------------------------------------------------------------
// PreparedSelect — Batch execution (reuse prepared statement)
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelect_BatchExecution() {
	ep := s.mock.ExpectPrepare("SELECT `id`, `name` FROM `users` WHERE status = \\?")

	// First execution
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	// Second execution with different args
	ep.ExpectQuery().
		WithArgs("inactive").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(2, "Jane"))

	prepared, err := sqlc.From("users").
		WithClient(s.client).
		Columns("id", "name").
		Where("status = ?", "active").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	// First call
	var activeUsers []User
	err = prepared.Select(s.ctx, &activeUsers, "active")
	s.Require().NoError(err)
	s.Assert().Len(activeUsers, 1)
	s.Assert().Equal("John", activeUsers[0].Name)

	// Second call with different args
	var inactiveUsers []User
	err = prepared.Select(s.ctx, &inactiveUsers, "inactive")
	s.Require().NoError(err)
	s.Assert().Len(inactiveUsers, 1)
	s.Assert().Equal("Jane", inactiveUsers[0].Name)
}

// -----------------------------------------------------------------------------
// PreparedExec — INSERT
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedExec_Insert() {
	ep := s.mock.ExpectPrepare("INSERT INTO `users` \\(`name`, `email`\\) VALUES \\(\\?, \\?\\)")
	ep.ExpectExec().
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	prepared, err := sqlc.Into("users").
		WithClient(s.client).
		Columns("name", "email").
		Values("John", "john@example.com").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	result, err := prepared.Exec(s.ctx, "John", "john@example.com")
	s.Require().NoError(err)

	lastID, err := result.LastInsertId()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), lastID)
}

// -----------------------------------------------------------------------------
// PreparedExec — INSERT batch (reuse prepared statement)
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedExec_InsertBatch() {
	ep := s.mock.ExpectPrepare("INSERT INTO `users` \\(`name`, `email`\\) VALUES \\(\\?, \\?\\)")
	ep.ExpectExec().
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))
	ep.ExpectExec().
		WithArgs("Jane", "jane@example.com").
		WillReturnResult(sqlmock.NewResult(2, 1))
	ep.ExpectExec().
		WithArgs("Bob", "bob@example.com").
		WillReturnResult(sqlmock.NewResult(3, 1))

	prepared, err := sqlc.Into("users").
		WithClient(s.client).
		Columns("name", "email").
		Values("placeholder", "placeholder").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	users := []struct {
		name  string
		email string
	}{
		{"John", "john@example.com"},
		{"Jane", "jane@example.com"},
		{"Bob", "bob@example.com"},
	}

	for i, u := range users {
		result, err := prepared.Exec(s.ctx, u.name, u.email)
		s.Require().NoError(err)

		lastID, err := result.LastInsertId()
		s.Require().NoError(err)
		s.Assert().Equal(int64(i+1), lastID)
	}
}

// -----------------------------------------------------------------------------
// PreparedExec — UPDATE
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedExec_Update() {
	ep := s.mock.ExpectPrepare("UPDATE `users` SET `name` = \\? WHERE id = \\?")
	ep.ExpectExec().
		WithArgs("Jane", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	prepared, err := sqlc.Update("users").
		WithClient(s.client).
		Set("name", "placeholder").
		Where("id = ?", 0).
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	result, err := prepared.Exec(s.ctx, "Jane", 1)
	s.Require().NoError(err)

	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), rowsAffected)
}

// -----------------------------------------------------------------------------
// PreparedExec — DELETE
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedExec_Delete() {
	ep := s.mock.ExpectPrepare("DELETE FROM `users` WHERE status = \\?")
	ep.ExpectExec().
		WithArgs("inactive").
		WillReturnResult(sqlmock.NewResult(0, 5))

	prepared, err := sqlc.Delete("users").
		WithClient(s.client).
		Where("status = ?", "inactive").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	result, err := prepared.Exec(s.ctx, "inactive")
	s.Require().NoError(err)

	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(5), rowsAffected)
}

// -----------------------------------------------------------------------------
// Generic PreparedSelectG — Get
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelectG_Get() {
	ep := s.mock.ExpectPrepare("SELECT \\* FROM `users` WHERE id = \\?")
	ep.ExpectQuery().
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	prepared, err := sqlc.FromG[User]("users").
		WithClient(s.client).
		Where("id = ?", 1).
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	user, err := prepared.Get(s.ctx, 1)
	s.Require().NoError(err)
	s.Assert().Equal(1, user.ID)
	s.Assert().Equal("John", user.Name)
	s.Assert().Equal("john@example.com", user.Email)
}

// -----------------------------------------------------------------------------
// Generic PreparedSelectG — Select
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelectG_Select() {
	ep := s.mock.ExpectPrepare("SELECT \\* FROM `users` WHERE status = \\?")
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "name", "email"}).
				AddRow(1, "John", "john@example.com").
				AddRow(2, "Jane", "jane@example.com"),
		)

	prepared, err := sqlc.FromG[User]("users").
		WithClient(s.client).
		Where("status = ?", "active").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	users, err := prepared.Select(s.ctx, "active")
	s.Require().NoError(err)
	s.Assert().Len(users, 2)
	s.Assert().Equal("John", users[0].Name)
	s.Assert().Equal("Jane", users[1].Name)
}

// -----------------------------------------------------------------------------
// Generic PreparedSelectG — Query (row iteration)
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedSelectG_Query() {
	ep := s.mock.ExpectPrepare("SELECT \\* FROM `users` WHERE status = \\?")
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "name", "email"}).
				AddRow(1, "John", "john@example.com").
				AddRow(2, "Jane", "jane@example.com"),
		)

	prepared, err := sqlc.FromG[User]("users").
		WithClient(s.client).
		Where("status = ?", "active").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	rows, err := prepared.Query(s.ctx, "active")
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(rows.Close())
	}()

	var users []User
	for rows.Next() {
		var user User
		err := rows.StructScan(&user)
		s.Require().NoError(err)
		users = append(users, user)
	}

	s.Require().NoError(rows.Err())
	s.Assert().Len(users, 2)
}

// -----------------------------------------------------------------------------
// Generic builders returning PreparedExec
// -----------------------------------------------------------------------------

func (s *PreparedTestSuite) TestPreparedExecG_Insert() {
	ep := s.mock.ExpectPrepare("INSERT INTO `users` \\(`name`, `email`\\) VALUES \\(\\?, \\?\\)")
	ep.ExpectExec().
		WithArgs("John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	prepared, err := sqlc.IntoG[User]("users").
		WithClient(s.client).
		Columns("name", "email").
		Values("placeholder", "placeholder").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	result, err := prepared.Exec(s.ctx, "John", "john@example.com")
	s.Require().NoError(err)

	lastID, err := result.LastInsertId()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), lastID)
}

func (s *PreparedTestSuite) TestPreparedExecG_Update() {
	ep := s.mock.ExpectPrepare("UPDATE `users` SET `name` = \\? WHERE id = \\?")
	ep.ExpectExec().
		WithArgs("Jane", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	prepared, err := sqlc.UpdateG[User]("users").
		WithClient(s.client).
		Set("name", "placeholder").
		Where("id = ?", 0).
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	result, err := prepared.Exec(s.ctx, "Jane", 1)
	s.Require().NoError(err)

	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), rowsAffected)
}

func (s *PreparedTestSuite) TestPreparedExecG_Delete() {
	ep := s.mock.ExpectPrepare("DELETE FROM `users` WHERE status = \\?")
	ep.ExpectExec().
		WithArgs("inactive").
		WillReturnResult(sqlmock.NewResult(0, 3))

	prepared, err := sqlc.DeleteG[User]("users").
		WithClient(s.client).
		Where("status = ?", "inactive").
		Prepare(s.ctx)
	s.Require().NoError(err)
	defer func() {
		s.Assert().NoError(prepared.Close())
	}()

	result, err := prepared.Exec(s.ctx, "inactive")
	s.Require().NoError(err)

	rowsAffected, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Assert().Equal(int64(3), rowsAffected)
}

// -----------------------------------------------------------------------------
// Error cases — standalone tests using mock Querier
// -----------------------------------------------------------------------------

func TestPrepareSelect_NoClient(t *testing.T) {
	ctx := context.Background()

	// No WithClient call — client is nil
	prepared, err := sqlc.From("users").
		Columns("id", "name").
		Where("status = ?", "active").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareExec_NoClient_Insert(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.Into("users").
		Columns("name", "email").
		Values("John", "john@example.com").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareExec_NoClient_Update(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareExec_NoClient_Delete(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.Delete("users").
		Where("status = ?", "inactive").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareSelect_ToSqlError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewQuerier(t)

	// From("") with no table should produce a ToSql error
	prepared, err := sqlc.From("").
		WithClient(mockClient).
		Columns("id").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not build sql for prepare")
}

func TestPrepareExec_PrepareFailure(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewQuerier(t)

	prepErr := errors.New("database unavailable")

	mockClient.EXPECT().
		Prepare(ctx, "DELETE FROM `users` WHERE status = ?").
		Return(nil, prepErr)

	prepared, err := sqlc.Delete("users").
		WithClient(mockClient).
		Where("status = ?", "inactive").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.ErrorIs(t, err, prepErr)
	assert.Contains(t, err.Error(), "could not prepare statement")
}

func TestPrepareSelect_PrepareFailure(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewQuerier(t)

	prepErr := errors.New("database unavailable")

	mockClient.EXPECT().
		Prepare(ctx, "SELECT `id`, `name` FROM `users` WHERE status = ?").
		Return(nil, prepErr)

	prepared, err := sqlc.From("users").
		WithClient(mockClient).
		Columns("id", "name").
		Where("status = ?", "active").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.ErrorIs(t, err, prepErr)
	assert.Contains(t, err.Error(), "could not prepare statement")
}

// -----------------------------------------------------------------------------
// Generic error cases
// -----------------------------------------------------------------------------

func TestPrepareSelectG_NoClient(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.FromG[User]("users").
		Where("status = ?", "active").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareExecG_NoClient_Insert(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.IntoG[User]("users").
		Columns("name", "email").
		Values("John", "john@example.com").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareExecG_NoClient_Update(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.UpdateG[User]("users").
		Set("name", "John").
		Where("id = ?", 1).
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareExecG_NoClient_Delete(t *testing.T) {
	ctx := context.Background()

	prepared, err := sqlc.DeleteG[User]("users").
		Where("status = ?", "inactive").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no client set for query preparation")
}

func TestPrepareSelectG_PrepareFailure(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewQuerier(t)

	prepErr := errors.New("database unavailable")

	mockClient.EXPECT().
		Prepare(ctx, "SELECT * FROM `users` WHERE status = ?").
		Return(nil, prepErr)

	prepared, err := sqlc.FromG[User]("users").
		WithClient(mockClient).
		Where("status = ?", "active").
		Prepare(ctx)

	assert.Nil(t, prepared)
	require.Error(t, err)
	assert.ErrorIs(t, err, prepErr)
}
