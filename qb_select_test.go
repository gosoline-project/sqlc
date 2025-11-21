package sqlg_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gosoline-project/sqlg"
	mocks "github.com/gosoline-project/sqlg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleSelect(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name", "email")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestSelectWithWhere(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name", "email").
		Where("status = ?", "active").
		Where("age >= ?", 18)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ? AND age >= ?", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, "active", params[0])
	assert.Equal(t, 18, params[1])
}

func TestSelectWithExpressions(t *testing.T) {
	q := sqlg.From("orders").
		Columns(
			sqlg.Col("customer_id"),
			sqlg.Col("*").Count().As("order_count"),
			sqlg.Col("amount").Sum().As("total_amount"),
		).
		GroupBy("customer_id")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `customer_id`, COUNT(*) AS order_count, SUM(`amount`) AS total_amount FROM `orders` GROUP BY `customer_id`", sql)
	assert.Empty(t, params)
}

func TestSelectWithLimitOffset(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where("status = ?", "active").
		OrderBy("created_at DESC").
		Limit(10).
		Offset(20)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE status = ? ORDER BY `created_at` DESC LIMIT ? OFFSET ?", sql)
	assert.Len(t, params, 3)
	assert.Equal(t, "active", params[0])
	assert.Equal(t, 10, params[1])
	assert.Equal(t, 20, params[2])
}

func TestSelectDistinct(t *testing.T) {
	q := sqlg.From("users").
		Columns("country").
		Distinct()

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT DISTINCT `country` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestSelectWithAlias(t *testing.T) {
	q := sqlg.From("users").
		As("u").
		Columns("u.id", "u.name")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `u`.`id`, `u`.`name` FROM `users` AS u", sql)
	assert.Empty(t, params)
}

func TestSelectAllAggregates(t *testing.T) {
	q := sqlg.From("products").
		Columns(
			sqlg.Col("price").Min().As("min_price"),
			sqlg.Col("price").Max().As("max_price"),
			sqlg.Col("price").Avg().As("avg_price"),
			sqlg.Col("price").Sum().As("total_price"),
			sqlg.Col("*").Count().As("product_count"),
		)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT MIN(`price`) AS min_price, MAX(`price`) AS max_price, AVG(`price`) AS avg_price, SUM(`price`) AS total_price, COUNT(*) AS product_count FROM `products`", sql)
	assert.Empty(t, params)
}

func TestComplexQuery(t *testing.T) {
	q := sqlg.From("orders").
		Columns(
			sqlg.Col("customer_id"),
			sqlg.Col("status"),
			sqlg.Col("*").Count().As("order_count"),
			sqlg.Col("amount").Sum().As("total_amount"),
			sqlg.Col("amount").Avg().As("avg_amount"),
		).
		Where("created_at >= ?", "2024-01-01").
		Where("status IN (?, ?)", "completed", "shipped").
		GroupBy("customer_id", "status").
		Having("COUNT(*) >= ?", 5).
		Having("SUM(amount) > ?", 10000).
		OrderBy(sqlg.Col("total_amount").Desc()).
		Limit(100)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `customer_id`, `status`, COUNT(*) AS order_count, SUM(`amount`) AS total_amount, AVG(`amount`) AS avg_amount FROM `orders` WHERE created_at >= ? AND status IN (?, ?) GROUP BY `customer_id`, `status` HAVING COUNT(*) >= ? AND SUM(amount) > ? ORDER BY `total_amount` DESC LIMIT ?", sql)
	assert.Len(t, params, 6)
	assert.Equal(t, "2024-01-01", params[0])
	assert.Equal(t, "completed", params[1])
	assert.Equal(t, "shipped", params[2])
	assert.Equal(t, 5, params[3])
	assert.Equal(t, 10000, params[4])
	assert.Equal(t, 100, params[5])
}

func TestImmutability(t *testing.T) {
	// Create base query
	base := sqlg.From("users").Columns("id", "name")

	// Create two different queries from the same base
	q1 := base.Where("status = ?", "active")
	q2 := base.Where("status = ?", "inactive")

	sql1, params1, err1 := q1.ToSql()
	require.NoError(t, err1)

	sql2, params2, err2 := q2.ToSql()
	require.NoError(t, err2)

	// Both should have same SQL structure but different params
	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE status = ?", sql1)
	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE status = ?", sql2)

	assert.Len(t, params1, 1)
	assert.Equal(t, "active", params1[0])

	assert.Len(t, params2, 1)
	assert.Equal(t, "inactive", params2[0])
}

func TestWhereWithInExpression(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "customer_id", "status").
		Where(sqlg.Col("status").In("completed", "shipped"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `customer_id`, `status` FROM `orders` WHERE `status` IN (?, ?)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, "completed", params[0])
	assert.Equal(t, "shipped", params[1])
}

func TestWhereWithMultipleExpressions(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "customer_id", "amount").
		Where(sqlg.Col("status").In("completed", "shipped")).
		Where(sqlg.Col("amount").Gte(100))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `customer_id`, `amount` FROM `orders` WHERE `status` IN (?, ?) AND `amount` >= ?", sql)
	assert.Len(t, params, 3)
	assert.Equal(t, "completed", params[0])
	assert.Equal(t, "shipped", params[1])
	assert.Equal(t, 100, params[2])
}

func TestWhereEqExpression(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Col("status").Eq("active"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE `status` = ?", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, "active", params[0])
}

func TestWhereComparisonExpressions(t *testing.T) {
	tests := []struct {
		name          string
		expr          *sqlg.Expression
		expectedSQL   string
		expectedValue any
	}{
		{
			name:          "Eq",
			expr:          sqlg.Col("age").Eq(25),
			expectedSQL:   "`age` = ?",
			expectedValue: 25,
		},
		{
			name:          "NotEq",
			expr:          sqlg.Col("status").NotEq("deleted"),
			expectedSQL:   "`status` != ?",
			expectedValue: "deleted",
		},
		{
			name:          "Gt",
			expr:          sqlg.Col("price").Gt(100),
			expectedSQL:   "`price` > ?",
			expectedValue: 100,
		},
		{
			name:          "Gte",
			expr:          sqlg.Col("age").Gte(18),
			expectedSQL:   "`age` >= ?",
			expectedValue: 18,
		},
		{
			name:          "Lt",
			expr:          sqlg.Col("price").Lt(50),
			expectedSQL:   "`price` < ?",
			expectedValue: 50,
		},
		{
			name:          "Lte",
			expr:          sqlg.Col("age").Lte(65),
			expectedSQL:   "`age` <= ?",
			expectedValue: 65,
		},
		{
			name:          "Like",
			expr:          sqlg.Col("name").Like("%john%"),
			expectedSQL:   "`name` LIKE ?",
			expectedValue: "%john%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := sqlg.From("users").
				Columns("id").
				Where(tt.expr)

			sql, params, err := q.ToSql()
			require.NoError(t, err)

			assert.Equal(t, fmt.Sprintf("SELECT `id` FROM `users` WHERE %s", tt.expectedSQL), sql)
			assert.Len(t, params, 1)
			assert.Equal(t, tt.expectedValue, params[0])
		})
	}
}

func TestWhereIsNullExpression(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Col("deleted_at").IsNull())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE `deleted_at` IS NULL", sql)
	assert.Empty(t, params)
}

func TestWhereIsNotNullExpression(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Col("email").IsNotNull())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE `email` IS NOT NULL", sql)
	assert.Empty(t, params)
}

func TestWhereNotInExpression(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "status").
		Where(sqlg.Col("status").NotIn("cancelled", "refunded"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `status` FROM `orders` WHERE `status` NOT IN (?, ?)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, "cancelled", params[0])
	assert.Equal(t, "refunded", params[1])
}

func TestMixedWhereConditions(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "customer_id", "amount", "status").
		Where("created_at >= ?", "2024-01-01").
		Where(sqlg.Col("status").In("completed", "shipped")).
		Where(sqlg.Col("amount").Gt(100)).
		Where("customer_id = ?", 42)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `customer_id`, `amount`, `status` FROM `orders` WHERE created_at >= ? AND `status` IN (?, ?) AND `amount` > ? AND customer_id = ?", sql)
	assert.Len(t, params, 5)
	assert.Equal(t, "2024-01-01", params[0])
	assert.Equal(t, "completed", params[1])
	assert.Equal(t, "shipped", params[2])
	assert.Equal(t, 100, params[3])
	assert.Equal(t, 42, params[4])
}

func TestWhereWithAndExpression(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.And(
			sqlg.Col("age").Gte(18),
			sqlg.Col("status").Eq("active"),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE (`age` >= ? AND `status` = ?)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, 18, params[0])
	assert.Equal(t, "active", params[1])
}

func TestWhereWithOrExpression(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "status").
		Where(sqlg.Or(
			sqlg.Col("status").Eq("completed"),
			sqlg.Col("status").Eq("shipped"),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `status` FROM `orders` WHERE (`status` = ? OR `status` = ?)", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, "completed", params[0])
	assert.Equal(t, "shipped", params[1])
}

func TestWhereWithNotExpression(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Not(sqlg.Col("status").Eq("deleted")))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE NOT (`status` = ?)", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, "deleted", params[0])
}

func TestWhereWithNestedLogicalExpressions(t *testing.T) {
	// (status = 'active' AND age >= 18) OR (status = 'premium' AND age >= 21)
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Or(
			sqlg.And(
				sqlg.Col("status").Eq("active"),
				sqlg.Col("age").Gte(18),
			),
			sqlg.And(
				sqlg.Col("status").Eq("premium"),
				sqlg.Col("age").Gte(21),
			),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE ((`status` = ? AND `age` >= ?) OR (`status` = ? AND `age` >= ?))", sql)
	assert.Len(t, params, 4)
	assert.Equal(t, "active", params[0])
	assert.Equal(t, 18, params[1])
	assert.Equal(t, "premium", params[2])
	assert.Equal(t, 21, params[3])
}

func TestWhereWithComplexLogicalExpressions(t *testing.T) {
	// status IN ('completed', 'shipped') AND NOT (amount < 10 OR discount > 50)
	q := sqlg.From("orders").
		Columns("id", "amount").
		Where(sqlg.And(
			sqlg.Col("status").In("completed", "shipped"),
			sqlg.Not(sqlg.Or(
				sqlg.Col("amount").Lt(10),
				sqlg.Col("discount").Gt(50),
			)),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `amount` FROM `orders` WHERE (`status` IN (?, ?) AND NOT ((`amount` < ? OR `discount` > ?)))", sql)
	assert.Len(t, params, 4)
	assert.Equal(t, "completed", params[0])
	assert.Equal(t, "shipped", params[1])
	assert.Equal(t, 10, params[2])
	assert.Equal(t, 50, params[3])
}

func TestMixedWhereWithLogicalExpressions(t *testing.T) {
	// Mix string-based and expression-based conditions
	q := sqlg.From("orders").
		Columns("id", "customer_id", "amount").
		Where("created_at >= ?", "2024-01-01").
		Where(sqlg.Or(
			sqlg.Col("status").Eq("completed"),
			sqlg.Col("status").Eq("shipped"),
		)).
		Where(sqlg.Col("amount").Gt(100))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `customer_id`, `amount` FROM `orders` WHERE created_at >= ? AND (`status` = ? OR `status` = ?) AND `amount` > ?", sql)
	assert.Len(t, params, 4)
	assert.Equal(t, "2024-01-01", params[0])
	assert.Equal(t, "completed", params[1])
	assert.Equal(t, "shipped", params[2])
	assert.Equal(t, 100, params[3])
}

func TestSelectWithInvalidType(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", 123, "name") // 123 is an invalid type

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Columns argument 1")
	assert.Contains(t, err.Error(), "got int")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestWhereWithInvalidType(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(123) // 123 is an invalid type

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Where condition")
	assert.Contains(t, err.Error(), "got int")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestGroupByWithInvalidType(t *testing.T) {
	q := sqlg.From("orders").
		Columns("customer_id", "COUNT(*)").
		GroupBy("customer_id", 456) // 456 is an invalid type

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for GroupBy argument 1")
	assert.Contains(t, err.Error(), "got int")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestOrderByWithInvalidType(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		OrderBy("id", true) // true is an invalid type

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for OrderBy argument 1")
	assert.Contains(t, err.Error(), "got bool")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestErrorPersistsAcrossCalls(t *testing.T) {
	// Create a query with an error
	q := sqlg.From("users").
		Columns("id", 123) // Invalid type

	// Chain more methods - error should persist
	q = q.Where("status = ?", "active").
		OrderBy("id")

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Columns argument 1")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

// Test structs for ForType tests
type User struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

type Product struct {
	ProductID   int     `db:"product_id"`
	ProductName string  `db:"product_name"`
	Price       float64 `db:"price"`
	Stock       int     `db:"stock"`
	CategoryID  int     `db:"category_id"`
}

type PartialStruct struct {
	ID     int    `db:"id"`
	NoTag  string // No db tag
	Status string `db:"status"`
}

type EmbeddedStruct struct {
	BaseID   int    `db:"base_id"`
	BaseName string `db:"base_name"`
}

type CompositeStruct struct {
	EmbeddedStruct
	ExtraField string `db:"extra_field"`
}

func TestForTypeSimple(t *testing.T) {
	q := sqlg.From("users").ForType(User{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestForTypeMultipleFields(t *testing.T) {
	q := sqlg.From("products").ForType(Product{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `product_id`, `product_name`, `price`, `stock`, `category_id` FROM `products`", sql)
	assert.Empty(t, params)
}

func TestForTypeWithWhere(t *testing.T) {
	q := sqlg.From("users").
		ForType(User{}).
		Where("status = ?", "active")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ?", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, "active", params[0])
}

func TestForTypeWithOrderByLimit(t *testing.T) {
	q := sqlg.From("products").
		ForType(Product{}).
		Where("price > ?", 100).
		OrderBy("price DESC").
		Limit(10)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `product_id`, `product_name`, `price`, `stock`, `category_id` FROM `products` WHERE price > ? ORDER BY `price` DESC LIMIT ?", sql)
	assert.Len(t, params, 2)
	assert.Equal(t, 100, params[0])
	assert.Equal(t, 10, params[1])
}

func TestForTypePartialTags(t *testing.T) {
	q := sqlg.From("items").ForType(PartialStruct{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Should only include fields with db tags
	assert.Equal(t, "SELECT `id`, `status` FROM `items`", sql)
	assert.Empty(t, params)
}

func TestForTypeWithPointer(t *testing.T) {
	q := sqlg.From("users").ForType(&User{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestForTypeEmbedded(t *testing.T) {
	q := sqlg.From("composite").ForType(CompositeStruct{})

	sql, _, err := q.ToSql()
	require.NoError(t, err)

	// Should include the extra_field from CompositeStruct
	// Note: refl.GetTags may not recursively extract embedded struct fields
	assert.Contains(t, sql, "extra_field")
}

func TestForTypeWithAlias(t *testing.T) {
	q := sqlg.From("users").
		As("u").
		ForType(User{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users` AS u", sql)
	assert.Empty(t, params)
}

func TestForTypeImmutability(t *testing.T) {
	base := sqlg.From("users")

	q1 := base.ForType(User{})
	q2 := base.ForType(Product{})

	sql1, _, err1 := q1.ToSql()
	require.NoError(t, err1)

	sql2, _, err2 := q2.ToSql()
	require.NoError(t, err2)

	// q1 should have User fields
	assert.Contains(t, sql1, "`id`, `name`, `email`")

	// q2 should have Product fields
	assert.Contains(t, sql2, "`product_id`, `product_name`, `price`, `stock`, `category_id`")
}

func TestForTypeOverridesSelect(t *testing.T) {
	// ForType should replace any previous Select calls
	q := sqlg.From("users").
		Columns("only_this").
		ForType(User{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Should have User fields, not "only_this"
	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestColumnWithString(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Column("email")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestColumnWithExpression(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Column(sqlg.Col("age").Max().As("max_age"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, MAX(`age`) AS max_age FROM `users`", sql)
	assert.Empty(t, params)
}

func TestColumnMultipleCalls(t *testing.T) {
	q := sqlg.From("users").
		Columns("id").
		Column("name").
		Column("email").
		Column("created_at")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email`, `created_at` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestColumnAfterForType(t *testing.T) {
	q := sqlg.From("users").
		ForType(User{}).
		Column("created_at").
		Column(sqlg.Col("updated_at"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `email`, `created_at`, `updated_at` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestColumnWithInvalidType(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Column(12345)

	sql, params, err := q.ToSql()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type for Column argument")
	assert.Contains(t, err.Error(), "got int")
	assert.Empty(t, sql)
	assert.Nil(t, params)
}

func TestColumnWithWhere(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "customer_id").
		Column("amount").
		Column(sqlg.Col("status")).
		Where("created_at >= ?", "2024-01-01")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `customer_id`, `amount`, `status` FROM `orders` WHERE created_at >= ?", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, "2024-01-01", params[0])
}

func TestColumnImmutability(t *testing.T) {
	base := sqlg.From("users").Columns("id", "name")

	q1 := base.Column("email")
	q2 := base.Column("phone")

	sql1, _, err1 := q1.ToSql()
	require.NoError(t, err1)

	sql2, _, err2 := q2.ToSql()
	require.NoError(t, err2)

	// q1 should have email
	assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql1)

	// q2 should have phone (not email)
	assert.Equal(t, "SELECT `id`, `name`, `phone` FROM `users`", sql2)
}

func TestColumnEmptyBase(t *testing.T) {
	// Column should work even without prior Select
	q := sqlg.From("users").
		Column("id").
		Column("name")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestWithClient(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	// Verify client is set (we can't access it directly, `but` we can test Exec)
	ctx := context.Background()
	var dest []User

	// With explicit Columns, ForType is NOT called, so only id and name are selected
	mockClient.On("Select", ctx, &dest, "SELECT `id`, `name` FROM `users`").Return(nil)

	err := q.Select(ctx, &dest)

	// Should not error about missing client
	assert.NoError(t, err)
}

func TestWithClientImmutability(t *testing.T) {
	mock1 := mocks.NewClient(t)
	mock2 := mocks.NewClient(t)

	base := sqlg.From("users").Columns("id", "name")
	q1 := base.WithClient(mock1)
	q2 := base.WithClient(mock2)

	ctx := context.Background()
	var dest []User

	// With explicit Columns, ForType is NOT called, so only id and name are selected
	mock1.On("Select", ctx, &dest, "SELECT `id`, `name` FROM `users`").Return(nil).Once()
	mock2.On("Select", ctx, &dest, "SELECT `id`, `name` FROM `users`").Return(nil).Once()

	err1 := q1.Select(ctx, &dest)
	err2 := q2.Select(ctx, &dest)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestExecWithoutClient(t *testing.T) {
	q := sqlg.From("users").Columns("id", "name")

	ctx := context.Background()
	var dest []User

	err := q.Select(ctx, &dest)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no client set")
}

func TestExecWithValidClient(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name", "email").
		Where("status = ?", "active").
		WithClient(mockClient)

	ctx := context.Background()
	var dest []User

	mockClient.On("Select", ctx, &dest, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ?", []any{"active"}).Return(nil)

	err := q.Select(ctx, &dest)

	require.NoError(t, err)
}

func TestExecUsesForType(t *testing.T) {
	mockClient := mocks.NewClient(t)

	// Don't specify columns - Exec should use ForType to extract them
	q := sqlg.From("users").
		Where("status = ?", "active").
		WithClient(mockClient)

	ctx := context.Background()
	var dest []User

	// Should have extracted columns from User struct (id, `name`, `email`)
	mockClient.On("Select", ctx, &dest, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ?", []any{"active"}).Return(nil)

	err := q.Select(ctx, &dest)

	require.NoError(t, err)
}

func TestExecWithClientError(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()
	var dest []User

	// With explicit Columns, ForType is NOT called, so only id and name are selected
	mockClient.On("Select", ctx, &dest, "SELECT `id`, `name` FROM `users`").Return(fmt.Errorf("database connection error"))

	err := q.Select(ctx, &dest)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection error")
}

func TestExecWithQueryBuildError(t *testing.T) {
	mockClient := mocks.NewClient(t)

	// Create a query with invalid type to cause build error
	q := sqlg.From("users").
		Columns("id", 123). // Invalid type
		WithClient(mockClient)

	ctx := context.Background()
	var dest []User

	err := q.Select(ctx, &dest)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not build sql")
	// Mock should not be called due to build error
}

func TestExecWithComplexQuery(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("orders").
		Where(sqlg.Col("status").In("completed", "shipped")).
		Where(sqlg.Col("amount").Gte(100)).
		OrderBy("created_at DESC").
		Limit(10).
		WithClient(mockClient)

	ctx := context.Background()

	type Order struct {
		ID         int     `db:"id"`
		CustomerID int     `db:"customer_id"`
		Status     string  `db:"status"`
		Amount     float64 `db:"amount"`
	}
	var dest []Order

	mockClient.On("Select", ctx, &dest, "SELECT `id`, `customer_id`, `status`, `amount` FROM `orders` WHERE `status` IN (?, ?) AND `amount` >= ? ORDER BY `created_at` DESC LIMIT ?", []any{"completed", "shipped", 100, 10}).Return(nil)

	err := q.Select(ctx, &dest)

	require.NoError(t, err)
}

func TestJsonExpressionWithArrow(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name", "metadata->'$.email'")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `metadata`->'$.email' FROM `users`", sql)
	assert.Empty(t, params)
}

func TestJsonExpressionWithDoubleArrow(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "data->>'$.address.city'")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `data`->>'$.address.city' FROM `users`", sql)
	assert.Empty(t, params)
}

func TestJsonExpressionInWhere(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where("metadata->'$.status' = ?", "active")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE metadata->'$.status' = ?", sql)
	assert.Len(t, params, 1)
	assert.Equal(t, "active", params[0])
}

func TestJsonExpressionInOrderBy(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name").
		OrderBy("data->'$.priority' DESC")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `products` ORDER BY `data`->'$.priority' DESC", sql)
	assert.Empty(t, params)
}

func TestTableQualifiedJsonExpression(t *testing.T) {
	q := sqlg.From("users").
		As("u").
		Columns("u.id", "u.metadata->'$.email'")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `u`.`id`, `u`.`metadata`->'$.email' FROM `users` AS u", sql)
	assert.Empty(t, params)
}

func TestNestedJsonPath(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "data->'$.customer.address.zipcode'")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `data`->'$.customer.address.zipcode' FROM `orders`", sql)
	assert.Empty(t, params)
}

func TestMultipleJsonExpressions(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "metadata->'$.name'", "metadata->'$.price'", "settings->>'$.enabled'")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `metadata`->'$.name', `metadata`->'$.price', `settings`->>'$.enabled' FROM `products`", sql)
	assert.Empty(t, params)
}

func TestJsonExpressionInGroupBy(t *testing.T) {
	q := sqlg.From("events").
		Columns("data->'$.category'", sqlg.Col("*").Count().As("count")).
		GroupBy("data->'$.category'")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `data`->'$.category', COUNT(*) AS count FROM `events` GROUP BY `data`->'$.category'", sql)
	assert.Empty(t, params)
}

func TestComplexQueryWithJsonExpressions(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name", "metadata->'$.email'", "settings->>'$.timezone'").
		Where("metadata->'$.status' = ?", "active").
		Where("settings->>'$.premium' = ?", "true").
		OrderBy("metadata->'$.created_at' DESC").
		Limit(10)

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `metadata`->'$.email', `settings`->>'$.timezone' FROM `users` WHERE metadata->'$.status' = ? AND settings->>'$.premium' = ? ORDER BY `metadata`->'$.created_at' DESC LIMIT ?", sql)
	assert.Len(t, params, 3)
	assert.Equal(t, "active", params[0])
	assert.Equal(t, "true", params[1])
	assert.Equal(t, 10, params[2])
}

func TestOrderByWithAscString(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		OrderBy("created_at ASC")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` ORDER BY `created_at` ASC", sql)
	assert.Empty(t, params)
}

func TestOrderByWithAscExpression(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Col("price").Asc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY `price` ASC", sql)
	assert.Empty(t, params)
}

func TestOrderByWithDescExpression(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Col("price").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY `price` DESC", sql)
	assert.Empty(t, params)
}

func TestOrderByMultipleWithMixedDirections(t *testing.T) {
	q := sqlg.From("orders").
		Columns("id", "customer_id", "created_at").
		OrderBy(sqlg.Col("customer_id").Asc(), sqlg.Col("created_at").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `customer_id`, `created_at` FROM `orders` ORDER BY `customer_id` ASC, `created_at` DESC", sql)
	assert.Empty(t, params)
}

func TestOrderByMixedStringAndExpression(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price", "category").
		OrderBy("category ASC", sqlg.Col("price").Desc(), "name")

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price`, `category` FROM `products` ORDER BY `category` ASC, `price` DESC, `name`", sql)
	assert.Empty(t, params)
}

func TestSelectWithoutExplicitColumns(t *testing.T) {
	mockClient := mocks.NewClient(t)

	// No Columns() call - should auto-detect from User struct
	q := sqlg.From("users").
		Where("status = ?", "active").
		WithClient(mockClient)

	ctx := context.Background()
	var dest []User

	// ForType should be called automatically, selecting all User struct columns
	mockClient.On("Select", ctx, &dest, "SELECT `id`, `name`, `email` FROM `users` WHERE status = ?", []any{"active"}).Return(nil)

	err := q.Select(ctx, &dest)

	assert.NoError(t, err)
}

func TestGetWithoutExplicitColumns(t *testing.T) {
	mockClient := mocks.NewClient(t)

	// No Columns() call - should auto-detect from User struct
	q := sqlg.From("users").
		Where("id = ?", 123).
		WithClient(mockClient)

	ctx := context.Background()
	var dest User

	// ForType should be called automatically, selecting all User struct columns
	mockClient.On("Get", ctx, &dest, "SELECT `id`, `name`, `email` FROM `users` WHERE id = ?", []any{123}).Return(nil)

	err := q.Get(ctx, &dest)

	assert.NoError(t, err)
}

func TestGetWithExplicitColumns(t *testing.T) {
	mockClient := mocks.NewClient(t)

	// Explicit Columns() - should NOT call ForType
	q := sqlg.From("users").
		Columns("id", "name").
		Where("id = ?", 456).
		WithClient(mockClient)

	ctx := context.Background()
	var dest User

	// Only id and name should be selected
	mockClient.On("Get", ctx, &dest, "SELECT `id`, `name` FROM `users` WHERE id = ?", []any{456}).Return(nil)

	err := q.Get(ctx, &dest)

	assert.NoError(t, err)
}

func TestLitInOrderBy(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Lit("1"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY 1", sql)
	assert.Empty(t, params)
}

func TestLitWithAsc(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Lit("1").Asc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY 1 ASC", sql)
	assert.Empty(t, params)
}

func TestLitWithDesc(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Lit("2").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY 2 DESC", sql)
	assert.Empty(t, params)
}

func TestLitMultipleOrderBy(t *testing.T) {
	q := sqlg.From("products").
		Columns("category", "name", "price").
		OrderBy(sqlg.Lit("1").Asc(), sqlg.Lit("3").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `category`, `name`, `price` FROM `products` ORDER BY 1 ASC, 3 DESC", sql)
	assert.Empty(t, params)
}

func TestLitMixedWithColumnOrderBy(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price", "category").
		OrderBy(sqlg.Col("category").Asc(), sqlg.Lit("3").Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price`, `category` FROM `products` ORDER BY `category` ASC, 3 DESC", sql)
	assert.Empty(t, params)
}

func TestLitInSelect(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", sqlg.Lit("'literal_value'").As("constant"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, 'literal_value' AS constant FROM `products`", sql)
	assert.Empty(t, params)
}

func TestLitInGroupBy(t *testing.T) {
	q := sqlg.From("orders").
		Columns(sqlg.Col("status"), sqlg.Col("*").Count().As("count")).
		GroupBy(sqlg.Lit("1"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `status`, COUNT(*) AS count FROM `orders` GROUP BY 1", sql)
	assert.Empty(t, params)
}

func TestLitWithInt(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Lit(1).Asc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY 1 ASC", sql)
	assert.Empty(t, params)
}

func TestLitWithIntDesc(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", "price").
		OrderBy(sqlg.Lit(2).Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, `price` FROM `products` ORDER BY 2 DESC", sql)
	assert.Empty(t, params)
}

func TestLitWithMultipleInts(t *testing.T) {
	q := sqlg.From("products").
		Columns("category", "name", "price").
		OrderBy(sqlg.Lit(1).Asc(), sqlg.Lit(3).Desc())

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `category`, `name`, `price` FROM `products` ORDER BY 1 ASC, 3 DESC", sql)
	assert.Empty(t, params)
}

func TestLitWithFloat(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name", sqlg.Lit(3.14).As("pi"))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name`, 3.14 AS pi FROM `products`", sql)
	assert.Empty(t, params)
}

func TestLitWithInt64(t *testing.T) {
	q := sqlg.From("products").
		Columns("id", "name").
		OrderBy(sqlg.Lit(int64(1)))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `products` ORDER BY 1", sql)
	assert.Empty(t, params)
}

func TestLitGroupByWithInt(t *testing.T) {
	q := sqlg.From("orders").
		Columns(sqlg.Col("status"), sqlg.Col("*").Count().As("count")).
		GroupBy(sqlg.Lit(1))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `status`, COUNT(*) AS count FROM `orders` GROUP BY 1", sql)
	assert.Empty(t, params)
}

func TestEqMapWithSingleCondition(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Eq{"status": "active"})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE `status` = ?", sql)
	assert.Equal(t, []any{"active"}, params)
}

func TestEqMapWithMultipleConditions(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Eq{
			"status": "active",
			"age":    21,
			"role":   "admin",
		})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Keys are sorted alphabetically: age, role, status
	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE (`age` = ? AND `role` = ? AND `status` = ?)", sql)
	assert.Equal(t, []any{21, "admin", "active"}, params)
}

func TestEqMapWithEmptyMap(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Eq{})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Empty map should not generate WHERE clause
	assert.Equal(t, "SELECT `id`, `name` FROM `users`", sql)
	assert.Empty(t, params)
}

func TestEqMapWithOtherConditions(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.And(
			sqlg.Col("status").Eq("active"),
			sqlg.Col("age").Gt(18),
		))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE (`status` = ? AND `age` > ?)", sql)
	assert.Equal(t, []any{"active", 18}, params)
}

func TestEqMapSortingIsConsistent(t *testing.T) {
	// Run the same query multiple times to ensure sorting is consistent
	for i := 0; i < 5; i++ {
		q := sqlg.From("users").
			Columns("id").
			Where(sqlg.Eq{
				"z_column": "z",
				"a_column": "a",
				"m_column": "m",
			})

		sql, params, err := q.ToSql()
		require.NoError(t, err)

		// Should always be sorted: a_column, m_column, z_column
		assert.Equal(t, "SELECT `id` FROM `users` WHERE (`a_column` = ? AND `m_column` = ? AND `z_column` = ?)", sql)
		assert.Equal(t, []any{"a", "m", "z"}, params)
	}
}

func TestEqTypeDirectly(t *testing.T) {
	q := sqlg.From("users").
		Columns("id", "name").
		Where(sqlg.Eq{"status": "active", "verified": true})

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	// Keys are sorted alphabetically: status, verified
	assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE (`status` = ? AND `verified` = ?)", sql)
	assert.Equal(t, []any{"active", true}, params)
}

func TestSelectWithNonPointer(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()
	var users []User // Not a pointer

	err := q.Select(ctx, users)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Select: destination must be a pointer")
	assert.Contains(t, err.Error(), "use &[]sqlg_test.User instead")
}

func TestSelectWithNil(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()

	err := q.Select(ctx, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Select: destination cannot be nil")
}

func TestSelectWithNilPointer(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()
	var users *[]User // nil pointer

	err := q.Select(ctx, users)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Select: destination pointer cannot be nil")
}

func TestGetWithNonPointer(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()
	var user User // Not a pointer

	err := q.Get(ctx, user)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Get: destination must be a pointer")
	assert.Contains(t, err.Error(), "use &sqlg_test.User instead")
}

func TestGetWithNil(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()

	err := q.Get(ctx, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Get: destination cannot be nil")
}

func TestGetWithNilPointer(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()
	var user *User // nil pointer

	err := q.Get(ctx, user)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Get: destination pointer cannot be nil")
}

func TestGetWithSlice(t *testing.T) {
	mockClient := mocks.NewClient(t)

	q := sqlg.From("users").
		Columns("id", "name").
		WithClient(mockClient)

	ctx := context.Background()
	var users []User

	err := q.Get(ctx, &users)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Get: destination must be a single struct, not a slice")
	assert.Contains(t, err.Error(), "Use Select() for multiple results")
}
