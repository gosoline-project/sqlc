package sqlc_test

import (
	"encoding/json"
	"testing"

	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonOrder_ToExpression(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		expected string
	}{
		{"default ascending", `{"column":"name"}`, "SELECT * FROM `users` ORDER BY `name` ASC"},
		{"ascending", `{"column":"name","direction":"asc"}`, "SELECT * FROM `users` ORDER BY `name` ASC"},
		{"descending", `{"column":"users.created_at","direction":"desc"}`, "SELECT * FROM `users` ORDER BY `users`.`created_at` DESC"},
		{"mixed case", `{"column":"name","direction":"DeSc"}`, "SELECT * FROM `users` ORDER BY `name` DESC"},
		{"uppercase", `{"column":"name","direction":"ASC"}`, "SELECT * FROM `users` ORDER BY `name` ASC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order sqlc.JsonOrder
			require.NoError(t, json.Unmarshal([]byte(tc.body), &order))
			expression, err := order.ToExpression()
			require.NoError(t, err)
			query, params, err := sqlc.From("users").OrderBy(expression).ToSql()
			require.NoError(t, err)
			assert.Equal(t, tc.expected, query)
			assert.Empty(t, params)
		})
	}
}

func TestJsonOrder_RejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order sqlc.JsonOrder
	}{
		{"empty column", sqlc.JsonOrder{}},
		{"empty qualifier", sqlc.JsonOrder{Column: ".name"}},
		{"empty trailing identifier", sqlc.JsonOrder{Column: "users."}},
		{"wildcard", sqlc.JsonOrder{Column: "users.*"}},
		{"expression", sqlc.JsonOrder{Column: "LOWER(name)"}},
		{"embedded direction", sqlc.JsonOrder{Column: "name DESC"}},
		{"quoted identifier", sqlc.JsonOrder{Column: "`name`"}},
		{"statement", sqlc.JsonOrder{Column: "name; DROP TABLE users"}},
		{"invalid direction", sqlc.JsonOrder{Column: "name", Direction: "sideways"}},
		{"direction expression", sqlc.JsonOrder{Column: "name", Direction: "ASC, id DESC"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expression, err := tc.order.ToExpression()
			require.Error(t, err)
			assert.Nil(t, expression)
		})
	}
}

func TestJsonOrder_JSONRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		order sqlc.JsonOrder
		json  string
	}{
		{sqlc.JsonOrder{Column: "name"}, `{"column":"name"}`},
		{sqlc.JsonOrder{Column: "users.id", Direction: "desc"}, `{"column":"users.id","direction":"desc"}`},
	} {
		payload, err := json.Marshal(tc.order)
		require.NoError(t, err)
		assert.JSONEq(t, tc.json, string(payload))
		var decoded sqlc.JsonOrder
		require.NoError(t, json.Unmarshal(payload, &decoded))
		assert.Equal(t, tc.order, decoded)
	}
}
