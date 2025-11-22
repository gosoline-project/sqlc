package main

import (
	"fmt"

	"github.com/gosoline-project/sqlc"
)

// ExampleNumericFunctions demonstrates using numeric SQL functions
func ExampleNumericFunctions() {
	fmt.Println("=== Numeric Functions Examples ===")
	fmt.Println()

	// Example 1: Basic math functions
	qb1 := sqlc.From("products").
		Columns(
			sqlc.Col("name"),
			sqlc.Col("price").Round().As("price_rounded"),
			sqlc.Col("price").RoundN(2).As("price_exact"),
			sqlc.Col("discount").Abs().As("abs_discount"),
		)
	sql1, _, _ := qb1.ToSql()
	fmt.Printf("Example 1 - Basic Math:\n%s\n\n", sql1)

	// Example 2: Advanced functions with arguments
	qb2 := sqlc.From("calculations").
		Columns(
			sqlc.Col("value"),
			sqlc.Col("value").Pow(2).As("squared"),
			sqlc.Col("value").Pow(3).As("cubed"),
			sqlc.Col("value").Sqrt().As("square_root"),
			sqlc.Col("id").Mod(100).As("last_two_digits"),
		)
	sql2, _, _ := qb2.ToSql()
	fmt.Printf("Example 2 - Power & Modulo:\n%s\n\n", sql2)

	// Example 3: Rounding and truncation
	qb3 := sqlc.From("financials").
		Columns(
			sqlc.Col("amount"),
			sqlc.Col("amount").Round().As("rounded"),
			sqlc.Col("amount").RoundN(2).As("rounded_cents"),
			sqlc.Col("amount").Truncate(2).As("truncated_cents"),
			sqlc.Col("amount").Ceil().As("ceiling"),
			sqlc.Col("amount").Floor().As("floor"),
		)
	sql3, _, _ := qb3.ToSql()
	fmt.Printf("Example 3 - Rounding & Truncation:\n%s\n\n", sql3)

	// Example 4: Aggregate functions
	qb4 := sqlc.From("sales").
		Columns(
			sqlc.Col("product_id"),
			sqlc.Col("amount").Sum().As("total_sales"),
			sqlc.Col("amount").Avg().As("avg_sales"),
			sqlc.Col("amount").Min().As("min_sale"),
			sqlc.Col("amount").Max().As("max_sale"),
			sqlc.Col("amount").StdDev().As("std_dev"),
			sqlc.Col("amount").Variance().As("variance"),
		).
		GroupBy(sqlc.Col("product_id"))
	sql4, _, _ := qb4.ToSql()
	fmt.Printf("Example 4 - Aggregate Functions:\n%s\n\n", sql4)

	// Example 5: Special functions
	qb5 := sqlc.From("data").
		Columns(
			sqlc.Col("value").Sign().As("sign"),
			sqlc.Col("tags").GroupConcat().As("all_tags"),
			sqlc.Rand().As("random_value"),
		)
	sql5, _, _ := qb5.ToSql()
	fmt.Printf("Example 5 - Special Functions:\n%s\n\n", sql5)

	// Example 6: Combining with WHERE and ORDER BY
	qb6 := sqlc.From("orders").
		Columns(
			sqlc.Col("id"),
			sqlc.Col("total").RoundN(2).As("total_rounded"),
			sqlc.Col("discount").Abs().As("abs_discount"),
		).
		Where(sqlc.Col("total").Gt(100)).
		OrderBy(sqlc.Col("total").RoundN(2).Desc())
	sql6, params6, _ := qb6.ToSql()
	fmt.Printf("Example 6 - Combined with WHERE/ORDER BY:\n%s\nParams: %v\n\n", sql6, params6)
}
