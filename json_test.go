package sqlc_test

import (
	"encoding/json"
	"testing"

	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/require"
)

type (
	JSONTestCase[BehaviourFrom, BehaviourTo sqlc.Nullable | sqlc.NonNullable] struct {
		description
		fromBehaviour BehaviourFrom
	}

	description string

	Runnable interface {
		Run(t *testing.T)
	}
	Describable interface {
		Desc() string
	}

	StandardTestCase[BehaviourFrom, BehaviourTo sqlc.Nullable | sqlc.NonNullable]    JSONTestCase[BehaviourFrom, BehaviourTo]
	NilTestCase[BehaviourFrom, BehaviourTo sqlc.Nullable | sqlc.NonNullable]         JSONTestCase[BehaviourFrom, BehaviourTo]
	EmptyStringTestCase[BehaviourFrom, BehaviourTo sqlc.Nullable | sqlc.NonNullable] JSONTestCase[BehaviourFrom, BehaviourTo]
	NilMapTestCase[BehaviourFrom, BehaviourTo sqlc.Nullable | sqlc.NonNullable]      JSONTestCase[BehaviourFrom, BehaviourTo]
	NilSliceTestCase[BehaviourFrom, BehaviourTo sqlc.Nullable | sqlc.NonNullable]    JSONTestCase[BehaviourFrom, BehaviourTo]
)

func (testCase StandardTestCase[BehaviourFrom, BehaviourTo]) Run(t *testing.T) {
	t.Parallel()

	type T struct {
		Foo string
	}
	v := T{
		Foo: "bar",
	}

	jsonType := sqlc.NewJSON(v, testCase.fromBehaviour)

	require.Equal(t, v, jsonType.Get())

	value, err := jsonType.Value()
	require.NoError(t, err)

	b, err := json.Marshal(v)
	require.NoError(t, err)

	require.Equal(t, b, value)
	parsed := &sqlc.JSON[T, BehaviourTo]{}
	require.NoError(t, parsed.Scan(value))

	require.Equal(t, jsonType.Get(), parsed.Get())
}

func (testCase NilTestCase[BehaviourFrom, BehaviourTo]) Run(t *testing.T) {
	t.Parallel()

	type T struct {
		Foo string
	}
	var v *T

	jsonType := sqlc.NewJSON(v, testCase.fromBehaviour)

	require.Equal(t, v, jsonType.Get())

	value, err := jsonType.Value()
	require.NoError(t, err)

	if sqlc.IsNullable(testCase.fromBehaviour).IsNullable() {
		require.Nil(t, value)
	} else {
		require.Equal(t, []byte("null"), value)
	}

	parsed := &sqlc.JSON[*T, BehaviourTo]{}
	require.NoError(t, parsed.Scan(value))

	require.Equal(t, jsonType.Get(), parsed.Get())
}

func (testCase EmptyStringTestCase[BehaviourFrom, BehaviourTo]) Run(t *testing.T) {
	t.Parallel()

	var v string

	jsonType := sqlc.NewJSON(v, testCase.fromBehaviour)

	require.Equal(t, v, jsonType.Get())

	value, err := jsonType.Value()
	require.NoError(t, err)

	require.Equal(t, "\"\"", string(value.([]byte)))

	parsed := &sqlc.JSON[string, BehaviourTo]{}
	require.NoError(t, parsed.Scan(value))

	require.Equal(t, jsonType.Get(), parsed.Get())
}

func (testCase NilSliceTestCase[BehaviourFrom, BehaviourTo]) Run(t *testing.T) {
	t.Parallel()

	type T struct {
		Foo string
	}
	var v []T

	jsonType := sqlc.NewJSON(v, testCase.fromBehaviour)

	require.Equal(t, v, jsonType.Get())

	value, err := jsonType.Value()
	require.NoError(t, err)

	if sqlc.IsNullable(testCase.fromBehaviour).IsNullable() {
		require.Nil(t, value)
	} else {
		require.Equal(t, []byte("null"), value)
	}

	parsed := &sqlc.JSON[*T, BehaviourTo]{}
	require.NoError(t, parsed.Scan(value))

	require.Equal(t, jsonType.Get(), parsed.Get())
}

func (testCase NilMapTestCase[BehaviourFrom, BehaviourTo]) Run(t *testing.T) {
	t.Parallel()

	type T struct {
		Foo string
	}
	var v map[T]T

	jsonType := sqlc.NewJSON(v, testCase.fromBehaviour)

	require.Equal(t, v, jsonType.Get())

	value, err := jsonType.Value()
	require.NoError(t, err)

	if sqlc.IsNullable(testCase.fromBehaviour).IsNullable() {
		require.Nil(t, value)
	} else {
		require.Equal(t, []byte("null"), value)
	}

	parsed := &sqlc.JSON[*T, BehaviourTo]{}
	require.NoError(t, parsed.Scan(value))

	require.Equal(t, jsonType.Get(), parsed.Get())
}

func (d description) Desc() string {
	return string(d)
}

func TestJSON(t *testing.T) {
	testCases := []interface {
		Runnable
		Describable
	}{
		StandardTestCase[sqlc.NonNullable, sqlc.NonNullable]{
			description: "NonNullable To NonNullable",
		},
		StandardTestCase[sqlc.Nullable, sqlc.NonNullable]{
			description: "Nullable To NonNullable",
		},
		StandardTestCase[sqlc.NonNullable, sqlc.Nullable]{
			description: "NonNullable To Nullable",
		},
		StandardTestCase[sqlc.Nullable, sqlc.Nullable]{
			description: "Nullable To Nullable",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Desc(), tC.Run)
	}
}

func TestJSONNil(t *testing.T) {
	testCases := []interface {
		Runnable
		Describable
	}{
		NilTestCase[sqlc.NonNullable, sqlc.NonNullable]{
			description: "NonNullable To NonNullable",
		},
		NilTestCase[sqlc.Nullable, sqlc.NonNullable]{
			description: "Nullable To NonNullable",
		},
		NilTestCase[sqlc.NonNullable, sqlc.Nullable]{
			description: "NonNullable To Nullable",
		},
		NilTestCase[sqlc.Nullable, sqlc.Nullable]{
			description: "Nullable To Nullable",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Desc(), tC.Run)
	}
}

func TestJSONEmptyString(t *testing.T) {
	testCases := []interface {
		Runnable
		Describable
	}{
		EmptyStringTestCase[sqlc.NonNullable, sqlc.NonNullable]{
			description: "NonNullable To NonNullable",
		},
		EmptyStringTestCase[sqlc.Nullable, sqlc.NonNullable]{
			description: "Nullable To NonNullable",
		},
		EmptyStringTestCase[sqlc.NonNullable, sqlc.Nullable]{
			description: "NonNullable To Nullable",
		},
		EmptyStringTestCase[sqlc.Nullable, sqlc.Nullable]{
			description: "Nullable To Nullable",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Desc(), tC.Run)
	}
}

func TestJSONNilMap(t *testing.T) {
	testCases := []interface {
		Runnable
		Describable
	}{
		EmptyStringTestCase[sqlc.NonNullable, sqlc.NonNullable]{
			description: "NonNullable To NonNullable",
		},
		EmptyStringTestCase[sqlc.Nullable, sqlc.NonNullable]{
			description: "Nullable To NonNullable",
		},
		EmptyStringTestCase[sqlc.NonNullable, sqlc.Nullable]{
			description: "NonNullable To Nullable",
		},
		EmptyStringTestCase[sqlc.Nullable, sqlc.Nullable]{
			description: "Nullable To Nullable",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Desc(), tC.Run)
	}
}

func TestJSONNilSlice(t *testing.T) {
	testCases := []interface {
		Runnable
		Describable
	}{
		EmptyStringTestCase[sqlc.NonNullable, sqlc.NonNullable]{
			description: "NonNullable To NonNullable",
		},
		EmptyStringTestCase[sqlc.Nullable, sqlc.NonNullable]{
			description: "Nullable To NonNullable",
		},
		EmptyStringTestCase[sqlc.NonNullable, sqlc.Nullable]{
			description: "NonNullable To Nullable",
		},
		EmptyStringTestCase[sqlc.Nullable, sqlc.Nullable]{
			description: "Nullable To Nullable",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Desc(), tC.Run)
	}
}

func TestJSONTypeEmptyStringParseNonNullable(t *testing.T) {
	t.Parallel()

	parsed := &sqlc.JSON[string, sqlc.NonNullable]{}
	require.Error(t, parsed.Scan([]byte("")))
}

func TestJSONTypeEmptyStringParseNullable(t *testing.T) {
	t.Parallel()

	parsed := &sqlc.JSON[string, sqlc.Nullable]{}
	require.Error(t, parsed.Scan([]byte("")))
}

func TestInvalidType(t *testing.T) {
	t.Parallel()

	parsed := &sqlc.JSON[string, sqlc.Nullable]{}
	require.Error(t, parsed.Scan(""), sqlc.ErrJSONInvalidType)
}
