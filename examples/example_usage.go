package main

import (
	"fmt"

	"github.com/gosoline-project/sqlg"
)

// ExampleUsage demonstrates how to use the query builder
func ExampleUsage() {
	// Simple select with string-based WHERE
	q1 := sqlg.From("users").
		Columns("id", "name", "email").
		Where("status = ?", "active").
		OrderBy("created_at DESC").
		Limit(10)

	sql1, params1, err := q1.ToSql()
	if err != nil {
		fmt.Printf("Error building query 1: %v\n", err)

		return
	}
	fmt.Println("Query 1 (string-based WHERE):")
	fmt.Println(sql1)
	fmt.Println("Params:", params1)
	fmt.Println()

	// Expression-based WHERE with In()
	q2 := sqlg.From("orders").
		Columns("id", "customer_id", "status", "amount").
		Where(sqlg.Col("status").In("completed", "shipped")).
		Where(sqlg.Col("amount").Gte(100)).
		OrderBy("created_at DESC").
		Limit(50)

	sql2, params2, err := q2.ToSql()
	if err != nil {
		fmt.Printf("Error building query 2: %v\n", err)

		return
	}
	fmt.Println("Query 2 (expression-based WHERE with In):")
	fmt.Println(sql2)
	fmt.Println("Params:", params2)
	fmt.Println()

	// Using expressions with aggregations (like Flink's $("col").count().as("alias"))
	q3 := sqlg.From("orders").
		Columns(
			sqlg.Col("customer_id"),
			sqlg.Col("*").Count().As("order_count"),
			sqlg.Col("amount").Sum().As("total_amount"),
		).
		Where(sqlg.Col("status").Eq("completed")).
		GroupBy("customer_id").
		Having("SUM(amount) > ?", 1000).
		OrderBy(sqlg.Col("total_amount").Desc()).
		Limit(100)

	sql3, params3, err := q3.ToSql()
	if err != nil {
		fmt.Printf("Error building query 3: %v\n", err)

		return
	}
	fmt.Println("Query 3 (with aggregations and Eq):")
	fmt.Println(sql3)
	fmt.Println("Params:", params3)
	fmt.Println()

	// All comparison operators
	q4 := sqlg.From("products").
		Columns("id", "name", "price", "category").
		Where(sqlg.Col("price").Gt(10)).
		Where(sqlg.Col("price").Lte(1000)).
		Where(sqlg.Col("category").NotEq("discontinued")).
		Where(sqlg.Col("name").Like("%phone%"))

	sql4, params4, err := q4.ToSql()
	if err != nil {
		fmt.Printf("Error building query 4: %v\n", err)

		return
	}
	fmt.Println("Query 4 (various comparison operators):")
	fmt.Println(sql4)
	fmt.Println("Params:", params4)
	fmt.Println()

	// IS NULL / IS NOT NULL
	q5 := sqlg.From("users").
		Columns("id", "name", "email").
		Where(sqlg.Col("deleted_at").IsNull()).
		Where(sqlg.Col("email").IsNotNull())

	sql5, params5, err := q5.ToSql()
	if err != nil {
		fmt.Printf("Error building query 5: %v\n", err)

		return
	}
	fmt.Println("Query 5 (IS NULL / IS NOT NULL):")
	fmt.Println(sql5)
	fmt.Println("Params:", params5)
	fmt.Println()

	// Mixed: string-based and expression-based WHERE
	q6 := sqlg.From("sales").
		As("s").
		Columns(
			"s.region",
			sqlg.Col("s.revenue").Sum().As("total_revenue"),
			sqlg.Col("*").Count().As("sale_count"),
		).
		Where("s.sale_date >= ?", "2024-01-01").
		Where(sqlg.Col("s.status").In("completed", "verified")).
		Where(sqlg.Col("s.revenue").Gt(0)).
		GroupBy("s.region").
		Having("COUNT(*) >= ?", 10).
		OrderBy(sqlg.Col("total_revenue").Desc()).
		Limit(10)

	sql6, params6, err := q6.ToSql()
	if err != nil {
		fmt.Printf("Error building query 6: %v\n", err)

		return
	}
	fmt.Println("Query 6 (mixed string and expression WHERE):")
	fmt.Println(sql6)
	fmt.Println("Params:", params6)
}

func main() {
	ExampleUsage()
}
