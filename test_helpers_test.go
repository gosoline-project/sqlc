package sqlc_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/exec"
	logmocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/stretchr/testify/require"
)

type User struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

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

type embeddedNamedFields struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type embeddedNamedUser struct {
	embeddedNamedFields
	Email string `db:"email"`
}

type taggedEmbeddedNamedUser struct {
	embeddedNamedFields `db:"user"`
	Email               string `db:"email"`
}

type shadowedEmbeddedNamedUser struct {
	embeddedNamedFields
	Name string `db:"name"`
}

type pointerNestedNamedUser struct {
	Place *nestedPlace `db:"place"`
	Name  string       `db:"name"`
}

type nullableInsert struct {
	ID    int     `db:"id"`
	Value *string `db:"value"`
}

type wrongTypes struct {
	ID   string `db:"id"`
	Name int    `db:"name"`
}

func newTestClientWithDriver(t *testing.T, driverName string) (context.Context, sqlc.Client, sqlmock.Sqlmock) {
	t.Helper()

	logger := logmocks.NewLoggerMock(logmocks.WithTestingT(t), logmocks.WithMockAll)
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
	})

	return context.Background(), sqlc.NewClientWithDB(logger, sqlc.WrapDB(mockDB, driverName), exec.NewDefaultExecutor(), sqlc.DefaultConfig()), mock
}
