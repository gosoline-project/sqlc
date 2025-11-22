package main

import (
	"context"
	"fmt"

	"github.com/gosoline-project/sqlc"
)

// ExampleQueryBuilderG demonstrates the usage of the generic QueryBuilderG factory
// which provides a convenient way to create all types of query builders with a
// pre-attached database client.
func ExampleQueryBuilderG() {
	// Define a User struct with db tags
	type User struct {
		ID     int    `db:"id"`
		Name   string `db:"name"`
		Email  string `db:"email"`
		Status string `db:"status"`
	}

	// Assume we have a client
	var client sqlc.Querier

	ctx := context.Background()

	// Create a generic query builder factory with the client pre-attached
	qb := sqlc.QG[User](client)

	// Example 1: SELECT query
	// No need to call WithClient() - it's already attached
	users, err := qb.From("users").
		Where("status = ?", "active").
		OrderBy("created_at DESC").
		Limit(10).
		Select(ctx)
	if err != nil {
		fmt.Printf("Error selecting users: %v\n", err)
		return
	}
	fmt.Printf("Found %d active users\n", len(users))

	// Example 2: Get a single user
	user, err := qb.From("users").
		Where("id = ?", 1).
		Get(ctx)
	if err != nil {
		fmt.Printf("Error getting user: %v\n", err)
		return
	}
	fmt.Printf("User: %s (%s)\n", user.Name, user.Email)

	// Example 3: INSERT query
	newUser := User{
		ID:     100,
		Name:   "John Doe",
		Email:  "john@example.com",
		Status: "active",
	}

	result, err := qb.Into("users").
		Records(newUser).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error inserting user: %v\n", err)
		return
	}

	lastID, _ := result.LastInsertId()
	fmt.Printf("Inserted user with ID: %d\n", lastID)

	// Example 4: Batch INSERT
	batchUsers := []User{
		{ID: 101, Name: "Jane Smith", Email: "jane@example.com", Status: "active"},
		{ID: 102, Name: "Bob Wilson", Email: "bob@example.com", Status: "active"},
	}

	result, err = qb.Into("users").
		Records(batchUsers).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error batch inserting users: %v\n", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Inserted %d users\n", rowsAffected)

	// Example 5: UPDATE query with struct
	updatedUser := User{
		Name:   "John Doe Updated",
		Email:  "john.updated@example.com",
		Status: "active",
	}

	result, err = qb.Update("users").
		SetRecord(updatedUser).
		Where("id = ?", 100).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error updating user: %v\n", err)
		return
	}

	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Updated %d user(s)\n", rowsAffected)

	// Example 6: UPDATE query with Set
	result, err = qb.Update("users").
		Set("status", "inactive").
		Where("last_login < ?", "2020-01-01").
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error updating inactive users: %v\n", err)
		return
	}

	// Example 7: DELETE query
	result, err = qb.Delete("users").
		Where("status = ?", "deleted").
		Where("created_at < ?", "2019-01-01").
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error deleting users: %v\n", err)
		return
	}

	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Deleted %d user(s)\n", rowsAffected)

	// Example 8: Complex query with multiple operations
	// Select users that need updating
	staleUsers, err := qb.From("users").
		Where("status = ?", "pending").
		Where("created_at < ?", "2020-01-01").
		OrderBy("created_at ASC").
		Limit(100).
		Select(ctx)
	if err != nil {
		fmt.Printf("Error selecting stale users: %v\n", err)
		return
	}

	fmt.Printf("Found %d stale users to process\n", len(staleUsers))

	// Update them
	result, err = qb.Update("users").
		Set("status", "archived").
		Where("status = ?", "pending").
		Where("created_at < ?", "2020-01-01").
		Limit(100).
		Exec(ctx)
	if err != nil {
		fmt.Printf("Error archiving stale users: %v\n", err)
		return
	}

	rowsAffected, _ = result.RowsAffected()
	fmt.Printf("Archived %d stale user(s)\n", rowsAffected)

	// Example 9: Using ToSql() to inspect generated queries
	sql, args, err := qb.From("users").
		Where("status = ?", "active").
		OrderBy("created_at DESC").
		Limit(10).
		ToSql()
	if err != nil {
		fmt.Printf("Error generating SQL: %v\n", err)
		return
	}

	fmt.Printf("Generated SQL: %s\n", sql)
	fmt.Printf("Arguments: %v\n", args)
}
