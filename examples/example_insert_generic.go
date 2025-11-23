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

	result, _ := sqlc.IntoG[User]("users").
		WithClient(client).
		Records(user).
		Exec(ctx)

	lastID, _ := result.LastInsertId()
	fmt.Printf("Inserted user with ID: %d\n", lastID)

	// Example 2: Insert multiple users in a batch
	users := []User{
		{ID: 2, Name: "Jane Smith", Email: "jane@example.com"},
		{ID: 3, Name: "Bob Wilson", Email: "bob@example.com"},
	}

	result, _ = sqlc.IntoG[User]("users").
		WithClient(client).
		Records(users).
		Exec(ctx)

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Inserted %d users\n", rowsAffected)

	// Example 3: Insert with ON DUPLICATE KEY UPDATE
	result, _ = sqlc.IntoG[User]("users").
		WithClient(client).
		Records(user).
		OnDuplicateKeyUpdate(
			sqlc.Assign("name", "John Doe Updated"),
			sqlc.AssignExpr("email", "CONCAT(email, '_updated')"),
		).
		Exec(ctx)

	// Example 4: Using REPLACE mode
	result, _ = sqlc.IntoG[User]("users").
		WithClient(client).
		Replace().
		Records(user).
		Exec(ctx)

	// Example 5: Using IGNORE modifier
	result, _ = sqlc.IntoG[User]("users").
		WithClient(client).
		Ignore().
		Records(user).
		Exec(ctx)
}
