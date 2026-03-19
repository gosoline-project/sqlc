package sqlc_test

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/exec"
	logmocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type taggedNamedUser struct {
	DisplayName string `db:"name"`
	Mail        string `db:"email"`
}

type userWithoutEmail struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type nestedPlace struct {
	ID int `db:"id"`
}

type nestedNamedUser struct {
	Place nestedPlace `db:"place"`
	Name  string      `db:"name"`
}

type nullableInsert struct {
	ID    int     `db:"id"`
	Value *string `db:"value"`
}

type wrongTypes struct {
	ID   string `db:"id"`
	Name int    `db:"name"`
}

func newCharacterizationClient(t *testing.T, driverName string) (context.Context, sqlc.Client, sqlmock.Sqlmock) {
	t.Helper()

	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
	})

	sqlxDB := sqlx.NewDb(mockDB, driverName)

	return context.Background(), sqlc.NewClientWithInterfaces(logger, sqlxDB, exec.NewDefaultExecutor(), sqlc.DefaultConfig()), mock
}

func TestSQLXCharacterizationNamedExecStructTagsAndRepeatedPlaceholder(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	arg := taggedNamedUser{DisplayName: "John", Mail: "john@example.com"}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (name, email, audit_email) VALUES (?, ?, ?)")).
		WithArgs("John", "john@example.com", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := client.NamedExec(ctx, "INSERT INTO users (name, email, audit_email) VALUES (:name, :email, :email)", arg)

	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

func TestSQLXCharacterizationNamedExecBatchStructSlice(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	users := []User{
		{Name: "John", Email: "john@example.com"},
		{Name: "Jane", Email: "jane@example.com"},
	}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (name, email) VALUES (?, ?),(?, ?)")).
		WithArgs("John", "john@example.com", "Jane", "jane@example.com").
		WillReturnResult(sqlmock.NewResult(2, 2))

	result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", users)

	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(2), rowsAffected)
}

func TestSQLXCharacterizationNamedExecBatchMapSlice(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	users := []map[string]any{
		{"name": "John", "email": "john@example.com"},
		{"name": "Jane", "email": "jane@example.com"},
	}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (name, email) VALUES (?, ?),(?, ?)")).
		WithArgs("John", "john@example.com", "Jane", "jane@example.com").
		WillReturnResult(sqlmock.NewResult(2, 2))

	result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", users)

	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(2), rowsAffected)
}

func TestSQLXCharacterizationNamedExecMissingMapKeyReturnsBindingError(t *testing.T) {
	ctx, client, _ := newCharacterizationClient(t, "sqlmock")

	result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", map[string]any{"name": "John"})

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not find name email")
}

func TestSQLXCharacterizationNamedExecMapFollowsPlaceholderOrder(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (email, name) VALUES (?, ?)")).
		WithArgs("john@example.com", "John").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (email, name) VALUES (:email, :name)", map[string]any{
		"name":  "John",
		"email": "john@example.com",
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecSupportsNamedParamsAtEndAndNumericSuffixes(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("UPDATE users SET first_name = ? WHERE last_name = ?")).
		WithArgs("John", "Doe").
		WillReturnResult(sqlmock.NewResult(0, 1))

	_, err := client.NamedExec(ctx, "UPDATE users SET first_name = :name1 WHERE last_name = :name2", map[string]any{
		"name1": "John",
		"name2": "Doe",
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecRewritesEscapedDoubleColons(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO audit (literal, decorated, first_name, last_name) VALUES ('a:b:c', '::ABC:_:', ?, ?)")).
		WithArgs("John", "Doe").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO audit (literal, decorated, first_name, last_name) VALUES ('a::b::c', '::::ABC::_::', :first_name, :last_name)", map[string]any{
		"first_name": "John",
		"last_name":  "Doe",
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecPreservesAtAssignmentSyntax(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO audit (marker, age, first_name, last_name) VALUES (@name := \"name\", ?, ?, ?)")).
		WithArgs(30, "John", "Doe").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO audit (marker, age, first_name, last_name) VALUES (@name := \"name\", :age, :first, :last)", map[string]any{
		"age":   30,
		"first": "John",
		"last":  "Doe",
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecSupportsNestedFieldPaths(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO place_users (place_id, name) VALUES (?, ?)")).
		WithArgs(7, "John").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO place_users (place_id, name) VALUES (:place.id, :name)", nestedNamedUser{
		Place: nestedPlace{ID: 7},
		Name:  "John",
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecBindsNilPointerFieldsAsNil(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nullable_values (id, value) VALUES (?, ?)")).
		WithArgs(2, nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO nullable_values (id, value) VALUES (:id, :value)", nullableInsert{ID: 2})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecWithinTransactionUsesSameBinding(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (email, name, email_copy) VALUES (?, ?, ?)")).
		WithArgs("john@example.com", "John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := client.WithTx(ctx, func(tx sqlc.Tx) error {
		_, err := tx.NamedExec(ctx, "INSERT INTO users (email, name, email_copy) VALUES (:email, :name, :email)", taggedNamedUser{
			DisplayName: "John",
			Mail:        "john@example.com",
		})

		return err
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationNamedExecRebindsForPostgres(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "postgres")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (name, email, audit_email) VALUES ($1, $2, $3)")).
		WithArgs("John", "john@example.com", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (name, email, audit_email) VALUES (:name, :email, :email)", taggedNamedUser{
		DisplayName: "John",
		Mail:        "john@example.com",
	})

	require.NoError(t, err)
}

func TestSQLXCharacterizationGetIntoPrimitive(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John"))

	var name string
	err := client.Get(ctx, &name, "SELECT name FROM users WHERE id = ?", 1)

	require.NoError(t, err)
	assert.Equal(t, "John", name)
}

func TestSQLXCharacterizationGetFailsForMissingDestinationFields(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name, email FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	var user userWithoutEmail
	err := client.Get(ctx, &user, "SELECT id, name, email FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing destination name email")
}

func TestSQLXCharacterizationGetIntoMapReturnsScannableError(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var result map[string]any
	err := client.Get(ctx, &result, "SELECT id, name FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scannable dest type map with >1 columns")
}

func TestSQLXCharacterizationGetRejectsNilDestinationPointer(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var user *User
	err := client.Get(ctx, user, "SELECT id, name FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil pointer passed to StructScan destination")
}

func TestSQLXCharacterizationGetReportsScanTypeMismatch(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var user wrongTypes
	err := client.Get(ctx, &user, "SELECT id, name FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting driver.Value type")
}

func TestSQLXCharacterizationGetPassesThroughPostgresPlaceholders(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "postgres")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT name FROM users WHERE id = $1")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John"))

	var name string
	err := client.Get(ctx, &name, "SELECT name FROM users WHERE id = $1", 1)

	require.NoError(t, err)
	assert.Equal(t, "John", name)
}

func TestSQLXCharacterizationSelectIntoPrimitiveSlice(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John").AddRow("Jane"))

	var names []string
	err := client.Select(ctx, &names, "SELECT name FROM users ORDER BY id")

	require.NoError(t, err)
	assert.Equal(t, []string{"John", "Jane"}, names)
}

func TestSQLXCharacterizationSelectSupportsNullStringSlices(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT city FROM places ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"city"}).AddRow("Berlin").AddRow(nil))

	var cities []sql.NullString
	err := client.Select(ctx, &cities, "SELECT city FROM places ORDER BY id")

	require.NoError(t, err)
	require.Len(t, cities, 2)
	assert.True(t, cities[0].Valid)
	assert.Equal(t, "Berlin", cities[0].String)
	assert.False(t, cities[1].Valid)
}

func TestSQLXCharacterizationSelectSupportsTimeSlices(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")
	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT created_at FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(now).AddRow(now.Add(time.Hour)))

	var createdAt []time.Time
	err := client.Select(ctx, &createdAt, "SELECT created_at FROM users ORDER BY id")

	require.NoError(t, err)
	assert.Equal(t, []time.Time{now, now.Add(time.Hour)}, createdAt)
}

func TestSQLXCharacterizationSelectSupportsPointerStructSlices(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))

	var users []*User
	err := client.Select(ctx, &users, "SELECT id, name FROM users ORDER BY id")

	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "John", users[0].Name)
	assert.Equal(t, "Jane", users[1].Name)
}

func TestSQLXCharacterizationSelectFailsForMissingDestinationFields(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name, email FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	var users []userWithoutEmail
	err := client.Select(ctx, &users, "SELECT id, name, email FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing destination name email")
}

func TestSQLXCharacterizationSelectIntoMapSliceReturnsNonStructError(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))

	var result []map[string]any
	err := client.Select(ctx, &result, "SELECT id, name FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-struct dest type map with >1 columns")
}

func TestSQLXCharacterizationSelectRejectsNilDestinationPointer(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var users *[]User
	err := client.Select(ctx, users, "SELECT id, name FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil pointer passed to StructScan destination")
}

func TestSQLXCharacterizationSelectReportsScanTypeMismatch(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var users []wrongTypes
	err := client.Select(ctx, &users, "SELECT id, name FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting driver.Value type")
}

func TestSQLXCharacterizationSelectWithinTransactionUsesSameScanning(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))
	mock.ExpectCommit()

	var users []User
	err := client.WithTx(ctx, func(tx sqlc.Tx) error {
		return tx.Select(ctx, &users, "SELECT id, name FROM users ORDER BY id")
	})

	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "John", users[0].Name)
	assert.Equal(t, "Jane", users[1].Name)
}

func TestSQLXCharacterizationQueryStructScanMissingDestinationField(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name, email FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	rows, err := client.Query(ctx, "SELECT id, name, email FROM users ORDER BY id")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rows.Close())
	}()

	require.True(t, rows.Next())

	var user userWithoutEmail
	err = rows.StructScan(&user)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing destination name email")
}

func TestSQLXCharacterizationQueryStructScanReportsTypeMismatch(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	rows, err := client.Query(ctx, "SELECT id, name FROM users ORDER BY id")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rows.Close())
	}()

	require.True(t, rows.Next())

	var user wrongTypes
	err = rows.StructScan(&user)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting driver.Value type")
}

func TestSQLXCharacterizationQueryWithinTransactionUsesSameRowIteration(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))
	mock.ExpectCommit()

	var users []User
	err := client.WithTx(ctx, func(tx sqlc.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id, name FROM users ORDER BY id")
		if err != nil {
			return err
		}
		defer func() {
			assert.NoError(t, rows.Close())
		}()

		for rows.Next() {
			var user User
			if err := rows.StructScan(&user); err != nil {
				return err
			}

			users = append(users, user)
		}

		return rows.Err()
	})

	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "John", users[0].Name)
	assert.Equal(t, "Jane", users[1].Name)
}

func TestSQLXCharacterizationClientPrepareReusesStatementAfterNoRows(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	ep := mock.ExpectPrepare(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?"))
	ep.ExpectQuery().WithArgs(1).WillReturnError(sql.ErrNoRows)
	ep.ExpectQuery().WithArgs(2).WillReturnError(sql.ErrNoRows)

	stmt, err := client.Prepare(ctx, "SELECT id, name FROM users WHERE id = ?")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, stmt.Close())
	}()

	var user User
	err = stmt.GetContext(ctx, &user, 1)
	assert.ErrorIs(t, err, sql.ErrNoRows)

	err = stmt.GetContext(ctx, &user, 2)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestSQLXCharacterizationClientPrepareSupportsSelectAndQuery(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	ep := mock.ExpectPrepare(regexp.QuoteMeta("SELECT id, name FROM users WHERE status = ?"))
	ep.ExpectQuery().WithArgs("active").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))
	ep.ExpectQuery().WithArgs("active").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))

	stmt, err := client.Prepare(ctx, "SELECT id, name FROM users WHERE status = ?")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, stmt.Close())
	}()

	var users []User
	err = stmt.SelectContext(ctx, &users, "active")
	require.NoError(t, err)
	require.Len(t, users, 2)

	rows, err := stmt.QueryxContext(ctx, "active")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rows.Close())
	}()

	count := 0
	for rows.Next() {
		var user User
		require.NoError(t, rows.StructScan(&user))
		count++
	}

	require.NoError(t, rows.Err())
	assert.Equal(t, 2, count)
}

func TestSQLXCharacterizationTxPrepareSupportsPreparedQueries(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	mock.ExpectBegin()
	ep := mock.ExpectPrepare(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?"))
	ep.ExpectQuery().WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))
	mock.ExpectCommit()

	var user User
	err := client.WithTx(ctx, func(tx sqlc.Tx) error {
		stmt, err := tx.Prepare(ctx, "SELECT id, name FROM users WHERE id = ?")
		if err != nil {
			return err
		}
		defer func() {
			assert.NoError(t, stmt.Close())
		}()

		return stmt.GetContext(ctx, &user, 1)
	})

	require.NoError(t, err)
	assert.Equal(t, "John", user.Name)
}

func TestSQLXCharacterizationPreparedQueryStructScanMissingDestinationField(t *testing.T) {
	ctx, client, mock := newCharacterizationClient(t, "sqlmock")

	preparedSQL := regexp.QuoteMeta("SELECT `id`, `name`, `email` FROM `users` WHERE status = ?")
	ep := mock.ExpectPrepare(preparedSQL)
	ep.ExpectQuery().
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	prepared, err := sqlc.From("users").
		WithClient(client).
		Columns("id", "name", "email").
		Where("status = ?", "active").
		Prepare(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, prepared.Close())
	}()

	rows, err := prepared.Query(ctx, "active")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rows.Close())
	}()

	require.True(t, rows.Next())

	var user userWithoutEmail
	err = rows.StructScan(&user)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing destination name email")
}
