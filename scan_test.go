package sqlc_test

import (
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGet_ScansPrimitiveValue(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John"))

	var name string
	err := client.Get(ctx, &name, "SELECT name FROM users WHERE id = ?", 1)

	require.NoError(t, err)
	assert.Equal(t, "John", name)
}

func TestGet_FailsForMissingDestinationFields(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name, email FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	var user userWithoutEmail
	err := client.Get(ctx, &user, "SELECT id, name, email FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing destination name email")
}

func TestGet_RejectsMapDestinationWithMultipleColumns(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var result map[string]any
	err := client.Get(ctx, &result, "SELECT id, name FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scannable dest type map with >1 columns")
}

func TestGet_RejectsNilDestinationPointer(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var user *User
	err := client.Get(ctx, user, "SELECT id, name FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil pointer passed to StructScan destination")
}

func TestGet_ReportsScanTypeMismatch(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var user wrongTypes
	err := client.Get(ctx, &user, "SELECT id, name FROM users WHERE id = ?", 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting driver.Value type")
}

func TestGet_PreservesPostgresPlaceholders(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "postgres")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT name FROM users WHERE id = $1")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John"))

	var name string
	err := client.Get(ctx, &name, "SELECT name FROM users WHERE id = $1", 1)

	require.NoError(t, err)
	assert.Equal(t, "John", name)
}

func TestGet_AllocatesNilNestedPointerFields(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT place_id, name FROM users WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"place.id", "name"}).AddRow(7, "John"))

	var user pointerNestedNamedUser
	err := client.Get(ctx, &user, "SELECT place_id, name FROM users WHERE id = ?", 1)

	require.NoError(t, err)
	require.NotNil(t, user.Place)
	assert.Equal(t, 7, user.Place.ID)
	assert.Equal(t, "John", user.Name)
}

func TestSelect_ScansPrimitiveSlice(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John").AddRow("Jane"))

	var names []string
	err := client.Select(ctx, &names, "SELECT name FROM users ORDER BY id")

	require.NoError(t, err)
	assert.Equal(t, []string{"John", "Jane"}, names)
}

func TestSelect_ScansNullStringSlice(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestSelect_ScansTimeSlice(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")
	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT created_at FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(now).AddRow(now.Add(time.Hour)))

	var createdAt []time.Time
	err := client.Select(ctx, &createdAt, "SELECT created_at FROM users ORDER BY id")

	require.NoError(t, err)
	assert.Equal(t, []time.Time{now, now.Add(time.Hour)}, createdAt)
}

func TestSelect_ScansPointerStructSlice(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))

	var users []*User
	err := client.Select(ctx, &users, "SELECT id, name FROM users ORDER BY id")

	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "John", users[0].Name)
	assert.Equal(t, "Jane", users[1].Name)
}

func TestSelect_FailsForMissingDestinationFields(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name, email FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "John", "john@example.com"))

	var users []userWithoutEmail
	err := client.Select(ctx, &users, "SELECT id, name, email FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing destination name email")
}

func TestSelect_RejectsMapSliceDestinationWithMultipleColumns(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John").AddRow(2, "Jane"))

	var result []map[string]any
	err := client.Select(ctx, &result, "SELECT id, name FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-struct dest type map with >1 columns")
}

func TestSelect_RejectsNilDestinationPointer(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var users *[]User
	err := client.Select(ctx, users, "SELECT id, name FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil pointer passed to StructScan destination")
}

func TestSelect_ReportsScanTypeMismatch(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name FROM users ORDER BY id")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

	var users []wrongTypes
	err := client.Select(ctx, &users, "SELECT id, name FROM users ORDER BY id")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting driver.Value type")
}

func TestSelect_UsesSameScanningWithinTransaction(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestQuery_StructScanFailsForMissingDestinationFields(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestQuery_StructScanReportsTypeMismatch(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestQuery_UsesSameRowIterationWithinTransaction(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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
