package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
)

// ExampleUpdateGeneric demonstrates the usage of the generic UpdateQueryBuilder.
func ExampleUpdateGeneric() {
	// Define a User struct with db tags
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	// Assume we have a client
	var client sqlc.Querier

	ctx := context.Background()

	// Example 1: Update using a struct record
	user := User{Name: "John Doe Updated", Email: "john.updated@example.com"}

	result, _ := sqlc.UpdateG[User]("users").
		WithClient(client).
		SetRecord(user).
		Where("id = ?", 1).
		Exec(ctx)

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Updated %d user(s)\n", rowsAffected)

	// Example 2: Update using individual Set calls
	result, _ = sqlc.UpdateG[User]("users").
		WithClient(client).
		Set("name", "Jane Smith").
		Set("email", "jane@example.com").
		Where("id = ?", 2).
		Exec(ctx)

	// Example 3: Update using a map
	updates := map[string]any{
		"name":  "Bob Wilson",
		"email": "bob@example.com",
	}

	result, _ = sqlc.UpdateG[User]("users").
		WithClient(client).
		SetMap(updates).
		Where("id = ?", 3).
		Exec(ctx)

	// Example 4: Update with expression
	result, _ = sqlc.UpdateG[User]("users").
		WithClient(client).
		SetExpr("name", "UPPER(name)").
		SetExpr("updated_at", "NOW()").
		Where("id = ?", 4).
		Exec(ctx)

	// Example 5: Update with multiple WHERE conditions
	result, _ = sqlc.UpdateG[User]("users").
		WithClient(client).
		Set("status", "active").
		Where("created_at < ?", "2023-01-01").
		Where("status = ?", "pending").
		Exec(ctx)

	// Example 6: Update with ORDER BY and LIMIT
	result, _ = sqlc.UpdateG[User]("users").
		WithClient(client).
		Set("priority", "high").
		Where("status = ?", "pending").
		OrderBy("created_at ASC").
		Limit(5).
		Exec(ctx)
}
