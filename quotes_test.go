package sqlc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		quote    string
		expected string
	}{
		{
			name:     "simple column",
			input:    "id",
			quote:    "`",
			expected: "`id`",
		},
		{
			name:     "wildcard",
			input:    "*",
			quote:    "`",
			expected: "*",
		},
		{
			name:     "already quoted",
			input:    "`id`",
			quote:    "`",
			expected: "`id`",
		},
		{
			name:     "table qualified column",
			input:    "users.id",
			quote:    "`",
			expected: "`users`.`id`",
		},
		{
			name:     "table qualified wildcard",
			input:    "users.*",
			quote:    "`",
			expected: "`users`.*",
		},
		{
			name:     "JSON operator",
			input:    "data->'$.field'",
			quote:    "`",
			expected: "`data`->'$.field'",
		},
		{
			name:     "table qualified JSON operator",
			input:    "u.metadata->'$.email'",
			quote:    "`",
			expected: "`u`.`metadata`->'$.email'",
		},
		{
			name:     "PostgreSQL double quotes",
			input:    "users.id",
			quote:    `"`,
			expected: `"users"."id"`,
		},
		{
			name:     "empty quote uses default",
			input:    "id",
			quote:    "",
			expected: "`id`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteIdentifier(tt.input, tt.quote)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQuoteOrderByClause(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		quote    string
		expected string
	}{
		{
			name:     "column with DESC",
			input:    "created_at DESC",
			quote:    "`",
			expected: "`created_at` DESC",
		},
		{
			name:     "column with ASC",
			input:    "name ASC",
			quote:    "`",
			expected: "`name` ASC",
		},
		{
			name:     "column only",
			input:    "id",
			quote:    "`",
			expected: "`id`",
		},
		{
			name:     "table qualified with DESC",
			input:    "users.name DESC",
			quote:    "`",
			expected: "`users`.`name` DESC",
		},
		{
			name:     "empty clause",
			input:    "",
			quote:    "`",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteOrderByClause(tt.input, tt.quote)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQuoteConditionIdentifiers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		quote    string
		expected string
	}{
		{
			name:     "simple equality with alias-qualified columns",
			input:    "u.id = o.user_id",
			quote:    "`",
			expected: "`u`.`id` = `o`.`user_id`",
		},
		{
			name:     "simple equality with full table names",
			input:    "users.id = orders.user_id",
			quote:    "`",
			expected: "`users`.`id` = `orders`.`user_id`",
		},
		{
			name:     "compound condition with AND and placeholder",
			input:    "u.id = o.user_id AND o.status = ?",
			quote:    "`",
			expected: "`u`.`id` = `o`.`user_id` AND `o`.`status` = ?",
		},
		{
			name:     "single column with placeholder",
			input:    "status = ?",
			quote:    "`",
			expected: "`status` = ?",
		},
		{
			name:     "not-equal operator",
			input:    "a.id != b.id",
			quote:    "`",
			expected: "`a`.`id` != `b`.`id`",
		},
		{
			name:     "less-than-or-equal operator",
			input:    "a.score <= b.score",
			quote:    "`",
			expected: "`a`.`score` <= `b`.`score`",
		},
		{
			name:     "greater-than operator",
			input:    "a.value > b.value",
			quote:    "`",
			expected: "`a`.`value` > `b`.`value`",
		},
		{
			name:     "diamond operator",
			input:    "a.id <> b.id",
			quote:    "`",
			expected: "`a`.`id` <> `b`.`id`",
		},
		{
			name:     "OR keyword",
			input:    "u.id = o.user_id OR u.id = o.alt_id",
			quote:    "`",
			expected: "`u`.`id` = `o`.`user_id` OR `u`.`id` = `o`.`alt_id`",
		},
		{
			name:     "IS NULL",
			input:    "o.deleted_at IS NULL",
			quote:    "`",
			expected: "`o`.`deleted_at` IS NULL",
		},
		{
			name:     "IS NOT NULL",
			input:    "o.deleted_at IS NOT NULL",
			quote:    "`",
			expected: "`o`.`deleted_at` IS NOT NULL",
		},
		{
			name:     "BETWEEN with placeholders",
			input:    "o.amount BETWEEN ? AND ?",
			quote:    "`",
			expected: "`o`.`amount` BETWEEN ? AND ?",
		},
		{
			name:     "numeric literal",
			input:    "o.status = 1",
			quote:    "`",
			expected: "`o`.`status` = 1",
		},
		{
			name:     "negative numeric literal",
			input:    "o.balance > -100",
			quote:    "`",
			expected: "`o`.`balance` > -100",
		},
		{
			name:     "decimal numeric literal",
			input:    "o.rate > 3.14",
			quote:    "`",
			expected: "`o`.`rate` > 3.14",
		},
		{
			name:     "string literal",
			input:    "o.status = 'active'",
			quote:    "`",
			expected: "`o`.`status` = 'active'",
		},
		{
			name:     "already quoted identifiers",
			input:    "`u`.`id` = `o`.`user_id`",
			quote:    "`",
			expected: "`u`.`id` = `o`.`user_id`",
		},
		{
			name:     "PostgreSQL double quotes",
			input:    "u.id = o.user_id AND o.status = ?",
			quote:    `"`,
			expected: `"u"."id" = "o"."user_id" AND "o"."status" = ?`,
		},
		{
			name:     "empty condition",
			input:    "",
			quote:    "`",
			expected: "",
		},
		{
			name:     "empty quote uses default",
			input:    "u.id = o.user_id",
			quote:    "",
			expected: "`u`.`id` = `o`.`user_id`",
		},
		{
			name:     "function call left as-is",
			input:    "COALESCE(u.id) = o.user_id",
			quote:    "`",
			expected: "COALESCE(u.id) = `o`.`user_id`",
		},
		{
			name:     "self join with different aliases",
			input:    "e.manager_id = m.id",
			quote:    "`",
			expected: "`e`.`manager_id` = `m`.`id`",
		},
		{
			name:     "multiple AND conditions",
			input:    "u.id = o.user_id AND o.type = ? AND o.active = ?",
			quote:    "`",
			expected: "`u`.`id` = `o`.`user_id` AND `o`.`type` = ? AND `o`.`active` = ?",
		},
		{
			name:     "LIKE keyword preserved",
			input:    "u.name LIKE ?",
			quote:    "`",
			expected: "`u`.`name` LIKE ?",
		},
		{
			name:     "IN keyword preserved",
			input:    "u.status IN ?",
			quote:    "`",
			expected: "`u`.`status` IN ?",
		},
		{
			name:     "TRUE and FALSE preserved",
			input:    "u.active = TRUE",
			quote:    "`",
			expected: "`u`.`active` = TRUE",
		},
		{
			name:     "EXISTS keyword preserved",
			input:    "EXISTS ?",
			quote:    "`",
			expected: "EXISTS ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteConditionIdentifiers(tt.input, tt.quote)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNumericLiteral(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"123", true},
		{"0", true},
		{"3.14", true},
		{"-42", true},
		{"-3.14", true},
		{"", false},
		{"abc", false},
		{"12abc", false},
		{"1.2.3", false},
		{"?", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := isNumericLiteral(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
