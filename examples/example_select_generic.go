package main

import (
	"fmt"

	"github.com/gosoline-project/sqlc"
)

type User struct {
	ID     int    `db:"id"`
	Name   string `db:"name"`
	Email  string `db:"email"`
	Status string `db:"status"`
}

type Order struct {
	ID         int     `db:"id"`
	CustomerID int     `db:"customer_id"`
	Status     string  `db:"status"`
	Amount     float64 `db:"amount"`
}

// ExampleSelectGeneric demonstrates how to use the generic query builder
func ExampleSelectGeneric() {
	// Generic query builder - type-safe return values
	// No need to pass dest parameter - the result type comes from the generic parameter

	// Simple select with automatic column detection based on struct tags
	q1 := sqlc.FromG[User]("users").
		Where("status = ?", "active").
		OrderBy("name ASC").
		Limit(10)

	sql1, params1, _ := q1.ToSql()
	fmt.Println("Generic Query 1 (automatic columns from struct tags):")
	fmt.Println(sql1)
	fmt.Println("Params:", params1)
	fmt.Println()

	// With explicit columns
	q2 := sqlc.FromG[User]("users").
		Columns("id", "name"). // Explicit columns override ForType()
		Where(sqlc.Col("status").Eq("active")).
		OrderBy("created_at DESC").
		Limit(10)

	sql2, params2, _ := q2.ToSql()
	fmt.Println("Generic Query 2 (explicit columns):")
	fmt.Println(sql2)
	fmt.Println("Params:", params2)
	fmt.Println()

	// Complex query with expressions
	q3 := sqlc.FromG[Order]("orders").
		Where(sqlc.Col("status").In("completed", "shipped")).
		Where(sqlc.Col("amount").Gte(100)).
		OrderBy(sqlc.Col("amount").Desc()).
		Limit(50)

	sql3, params3, _ := q3.ToSql()
	fmt.Println("Generic Query 3 (with expressions):")
	fmt.Println(sql3)
	fmt.Println("Params:", params3)
	fmt.Println()

	// Usage in real code would look like this:
	fmt.Println("\n--- How to use with a client ---")
	fmt.Println("// Get a single user (returns User directly, not *User):")
	fmt.Println(`user, err := sqlc.FromG[User]("users").`)
	fmt.Println(`    WithClient(client).`)
	fmt.Println(`    Where("id = ?", 123).`)
	fmt.Println(`    Get(ctx)`)
	fmt.Println(`if err != nil {`)
	fmt.Println(`    return err`)
	fmt.Println(`}`)
	fmt.Println(`fmt.Println(user.Name) // user is User, not *User`)
	fmt.Println()

	fmt.Println("// Select multiple users (returns []User directly, not []*User):")
	fmt.Println(`users, err := sqlc.FromG[User]("users").`)
	fmt.Println(`    WithClient(client).`)
	fmt.Println(`    Where("status = ?", "active").`)
	fmt.Println(`    Select(ctx)`)
	fmt.Println(`if err != nil {`)
	fmt.Println(`    return err`)
	fmt.Println(`}`)
	fmt.Println(`for _, user := range users {`)
	fmt.Println(`    fmt.Println(user.Name) // users is []User, not []*User`)
	fmt.Println(`}`)
}
