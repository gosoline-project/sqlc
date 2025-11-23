package main

import (
	"fmt"

	"github.com/gosoline-project/sqlc"
)

func ExampleInsert() {
	// INSERT with explicit columns and values
	i1 := sqlc.Into("users").
		Columns("id", "name", "email").
		Values(1, "John Doe", "john@example.com")

	sqlI1, paramsI1, _ := i1.ToSql()
	fmt.Println("Insert 1 (single row with explicit columns):")
	fmt.Println(sqlI1)
	fmt.Println("Params:", paramsI1)
	fmt.Println()

	// INSERT multiple rows
	i2 := sqlc.Into("users").
		Columns("id", "name", "email").
		Values(1, "John Doe", "john@example.com").
		Values(2, "Jane Smith", "jane@example.com").
		Values(3, "Bob Wilson", "bob@example.com")

	sqlI2, paramsI2, _ := i2.ToSql()
	fmt.Println("Insert 2 (multiple rows):")
	fmt.Println(sqlI2)
	fmt.Println("Params:", paramsI2)
	fmt.Println()

	// INSERT using ValuesRows for bulk insert
	i3 := sqlc.Into("products").
		Columns("id", "name", "price").
		ValuesRows(
			[]any{1, "Widget", 19.99},
			[]any{2, "Gadget", 29.99},
			[]any{3, "Doohickey", 39.99},
		)

	sqlI3, paramsI3, _ := i3.ToSql()
	fmt.Println("Insert 3 (bulk insert with ValuesRows):")
	fmt.Println(sqlI3)
	fmt.Println("Params:", paramsI3)
	fmt.Println()

	// INSERT with struct (Records) - uses named parameters for single record
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	user := User{ID: 42, Name: "Alice", Email: "alice@example.com"}
	i4 := sqlc.Into("users").Records(user)

	sqlI4, paramsI4, _ := i4.ToSql()
	fmt.Println("Insert 4 (single struct with Records - uses named parameters):")
	fmt.Println(sqlI4)
	fmt.Println("Params:", paramsI4, "(nil for named parameters, struct passed to NamedExec)")
	fmt.Println()

	// INSERT multiple structs (Records variadic)
	user10 := User{ID: 10, Name: "User10", Email: "user10@example.com"}
	user11 := User{ID: 11, Name: "User11", Email: "user11@example.com"}
	user12 := User{ID: 12, Name: "User12", Email: "user12@example.com"}
	i5 := sqlc.Into("users").Records(user10, user11, user12)

	sqlI5, paramsI5, _ := i5.ToSql()
	fmt.Println("Insert 5 (multiple structs with Records variadic):")
	fmt.Println(sqlI5)
	fmt.Println("Params:", paramsI5)
}
