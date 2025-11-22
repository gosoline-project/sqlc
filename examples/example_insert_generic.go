package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
)

// ExampleInsertGeneric demonstrates the usage of the generic InsertQueryBuilder.
func ExampleInsertGeneric() {
	// Define a User struct with db tags
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	// Assume we have a client
	var client sqlc.Querier

	ctx := context.Background()

	// Example 1: Insert a single user using Records
	user := User{ID: 1, Name: "John Doe", Email: "john@example.com"}

	result, err := sqlc.IntoG[User]("users").
		WithClient(client).
		Records(user).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error inserting user: %v\n", err)
		return
	}

	lastID, _ := result.LastInsertId()
	fmt.Printf("Inserted user with ID: %d\n", lastID)

	// Example 2: Insert multiple users in a batch
	users := []User{
		{ID: 2, Name: "Jane Smith", Email: "jane@example.com"},
		{ID: 3, Name: "Bob Wilson", Email: "bob@example.com"},
	}

	result, err = sqlc.IntoG[User]("users").
		WithClient(client).
		Records(users).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error batch inserting users: %v\n", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Inserted %d users\n", rowsAffected)

	// Example 3: Insert with ON DUPLICATE KEY UPDATE
	result, err = sqlc.IntoG[User]("users").
		WithClient(client).
		Records(user).
		OnDuplicateKeyUpdate(
			sqlc.Assign("name", "John Doe Updated"),
			sqlc.AssignExpr("email", "CONCAT(email, '_updated')"),
		).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error upserting user: %v\n", err)
		return
	}

	// Example 4: Using REPLACE mode
	result, err = sqlc.IntoG[User]("users").
		WithClient(client).
		Replace().
		Records(user).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error replacing user: %v\n", err)
		return
	}

	// Example 5: Using IGNORE modifier
	result, err = sqlc.IntoG[User]("users").
		WithClient(client).
		Ignore().
		Records(user).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error inserting user with ignore: %v\n", err)
		return
	}
}
