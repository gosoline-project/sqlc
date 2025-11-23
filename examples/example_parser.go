package main

import (
	"fmt"

	"github.com/gosoline-project/sqlc"
)

// ExampleParseWhere demonstrates using the SQL WHERE clause parser
func ExampleParseWhere() {
	fmt.Println("=== ParseWhere Examples ===")

	// Example 1: Simple equality condition
	expr1, _ := sqlc.ParseWhere("id = 1 AND name = 'foo'")
	sql1, params1, _ := sqlc.From("users").Where(expr1).ToSql()
	fmt.Printf("Example 1: %s\nParams: %v\n\n", sql1, params1)

	// Example 2: IN clause
	expr2, _ := sqlc.ParseWhere("status IN ('active', 'pending', 'approved')")
	sql2, params2, _ := sqlc.From("orders").Where(expr2).ToSql()
	fmt.Printf("Example 2: %s\nParams: %v\n\n", sql2, params2)

	// Example 3: Complex condition with parentheses
	expr3, _ := sqlc.ParseWhere("(age >= 18 AND age <= 65) OR status = 'vip'")
	sql3, params3, _ := sqlc.From("customers").Where(expr3).ToSql()
	fmt.Printf("Example 3: %s\nParams: %v\n\n", sql3, params3)

	// Example 4: NULL checks
	expr4, _ := sqlc.ParseWhere("deleted_at IS NULL AND email IS NOT NULL")
	sql4, params4, _ := sqlc.From("accounts").Where(expr4).ToSql()
	fmt.Printf("Example 4: %s\nParams: %v\n\n", sql4, params4)

	// Example 5: LIKE pattern matching
	expr5, _ := sqlc.ParseWhere("name LIKE '%john%' AND email LIKE '%@example.com'")
	sql5, params5, _ := sqlc.From("users").Where(expr5).ToSql()
	fmt.Printf("Example 5: %s\nParams: %v\n\n", sql5, params5)

	// Example 6: NOT operator
	expr6, _ := sqlc.ParseWhere("NOT (deleted = 1 OR banned = 1) AND verified = 1")
	sql6, params6, _ := sqlc.From("members").Where(expr6).ToSql()
	fmt.Printf("Example 6: %s\nParams: %v\n\n", sql6, params6)

	// Example 7: Comparison with programmatic approach
	fmt.Println("Comparison: Parser vs Programmatic")
	fmt.Println("-----------------------------------")

	// Using parser
	exprParsed, _ := sqlc.ParseWhere("age > 18 AND status = 'active'")
	sqlParsed, paramsParsed, _ := sqlc.From("users").Where(exprParsed).ToSql()
	fmt.Printf("Parser:        %s\n", sqlParsed)
	fmt.Printf("               Params: %v\n", paramsParsed)

	// Using programmatic API
	exprProg := sqlc.And(
		sqlc.Col("age").Gt(18),
		sqlc.Col("status").Eq("active"),
	)
	sqlProg, paramsProg, _ := sqlc.From("users").Where(exprProg).ToSql()
	fmt.Printf("Programmatic:  %s\n", sqlProg)
	fmt.Printf("               Params: %v\n", paramsProg)
}
