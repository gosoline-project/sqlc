package sqlc_test

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamedExec_BindsStructTagsAndRepeatedPlaceholders(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestNamedExec_SupportsBatchStructSlices(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestNamedExec_SupportsBatchMapSlices(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestNamedExec_ReturnsBindingErrorForMissingMapKey(t *testing.T) {
	ctx, client, _ := newTestClientWithDriver(t, "sqlmock")

	result, err := client.NamedExec(ctx, "INSERT INTO users (name, email) VALUES (:name, :email)", map[string]any{"name": "John"})

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not find name email")
}

func TestNamedExec_FollowsPlaceholderOrderForMaps(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (email, name) VALUES (?, ?)")).
		WithArgs("john@example.com", "John").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (email, name) VALUES (:email, :name)", map[string]any{
		"name":  "John",
		"email": "john@example.com",
	})

	require.NoError(t, err)
}

func TestNamedExec_SupportsNumericSuffixPlaceholders(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("UPDATE users SET first_name = ? WHERE last_name = ?")).
		WithArgs("John", "Doe").
		WillReturnResult(sqlmock.NewResult(0, 1))

	_, err := client.NamedExec(ctx, "UPDATE users SET first_name = :name1 WHERE last_name = :name2", map[string]any{
		"name1": "John",
		"name2": "Doe",
	})

	require.NoError(t, err)
}

func TestNamedExec_RewritesEscapedDoubleColons(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO audit (literal, decorated, first_name, last_name) VALUES ('a:b:c', '::ABC:_:', ?, ?)")).
		WithArgs("John", "Doe").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO audit (literal, decorated, first_name, last_name) VALUES ('a::b::c', '::::ABC::_::', :first_name, :last_name)", map[string]any{
		"first_name": "John",
		"last_name":  "Doe",
	})

	require.NoError(t, err)
}

func TestNamedExec_PreservesAtAssignmentSyntax(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestNamedExec_SupportsNestedFieldPaths(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO place_users (place_id, name) VALUES (?, ?)")).
		WithArgs(7, "John").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO place_users (place_id, name) VALUES (:place.id, :name)", nestedNamedUser{
		Place: nestedPlace{ID: 7},
		Name:  "John",
	})

	require.NoError(t, err)
}

func TestNamedExec_FlattensAnonymousEmbeddedStructs(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (id, name, email) VALUES (?, ?, ?)")).
		WithArgs(7, "John", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)", embeddedNamedUser{
		embeddedNamedFields: embeddedNamedFields{ID: 7, Name: "John"},
		Email:               "john@example.com",
	})

	require.NoError(t, err)
}

func TestNamedExec_SupportsTaggedEmbeddedStructPaths(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (id, email) VALUES (?, ?)")).
		WithArgs(7, "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (id, email) VALUES (:user.id, :email)", taggedEmbeddedNamedUser{
		embeddedNamedFields: embeddedNamedFields{ID: 7, Name: "John"},
		Email:               "john@example.com",
	})

	require.NoError(t, err)
}

func TestNamedExec_PrefersDirectFieldOverEmbeddedField(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (name) VALUES (?)")).
		WithArgs("direct").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (name) VALUES (:name)", shadowedEmbeddedNamedUser{
		embeddedNamedFields: embeddedNamedFields{Name: "embedded"},
		Name:                "direct",
	})

	require.NoError(t, err)
}

func TestNamedExec_BindsNilPointerFieldsAsNil(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nullable_values (id, value) VALUES (?, ?)")).
		WithArgs(2, nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO nullable_values (id, value) VALUES (:id, :value)", nullableInsert{ID: 2})

	require.NoError(t, err)
}

func TestNamedExec_UsesSameBindingWithinTransaction(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "sqlmock")

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

func TestNamedExec_RebindsPlaceholdersForPostgres(t *testing.T) {
	ctx, client, mock := newTestClientWithDriver(t, "postgres")

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO users (name, email, audit_email) VALUES ($1, $2, $3)")).
		WithArgs("John", "john@example.com", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := client.NamedExec(ctx, "INSERT INTO users (name, email, audit_email) VALUES (:name, :email, :email)", taggedNamedUser{
		DisplayName: "John",
		Mail:        "john@example.com",
	})

	require.NoError(t, err)
}
