package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
)

// ExampleDeleteGeneric demonstrates the usage of the generic DeleteQueryBuilder.
func ExampleDeleteGeneric() {
	// Define a User struct for type consistency
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email"`
	}

	// Assume we have a client
	var client sqlc.Querier

	ctx := context.Background()

	// Example 1: Simple delete with WHERE condition
	result, _ := sqlc.DeleteG[User]("users").
		WithClient(client).
		Where("id = ?", 1).
		Exec(ctx)

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Deleted %d user(s)\n", rowsAffected)

	// Example 2: Delete with multiple WHERE conditions
	result, _ = sqlc.DeleteG[User]("users").
		WithClient(client).
		Where("status = ?", "inactive").
		Where("last_login < ?", "2020-01-01").
		Exec(ctx)

	// Example 3: Delete using Expression
	result, _ = sqlc.DeleteG[User]("users").
		WithClient(client).
		Where(sqlc.Col("created_at").Lt("2020-01-01")).
		Exec(ctx)

	// Example 4: Delete with Eq map
	result, _ = sqlc.DeleteG[User]("users").
		WithClient(client).
		Where(sqlc.Eq{"status": "deleted", "verified": false}).
		Exec(ctx)

	// Example 5: Delete with ORDER BY and LIMIT (MySQL-specific)
	// This is useful for batch deletion
	result, _ = sqlc.DeleteG[User]("logs").
		WithClient(client).
		Where("level = ?", "debug").
		OrderBy("created_at ASC").
		Limit(1000).
		Exec(ctx)

	// Example 6: Generate SQL without executing
	sql, args, _ := sqlc.DeleteG[User]("users").
		Where("status = ?", "inactive").
		OrderBy("created_at ASC").
		Limit(100).
		ToSql()

	fmt.Printf("Generated SQL: %s\n", sql)
	fmt.Printf("Arguments: %v\n", args)
}
