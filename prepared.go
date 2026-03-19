package sqlc

import (
	"context"
	"fmt"
)

// PreparedSelect represents a prepared SELECT statement that can be executed
// multiple times with different parameter values. The SQL template is fixed
// at preparation time, and only the bind arguments change between executions.
//
// The caller is responsible for closing the prepared statement when it is no
// longer needed by calling Close().
//
// Example:
//
//	prepared, err := From("users").
//		WithClient(client).
//		Columns("id", "name").
//		Where("status = ?", "active").
//		Prepare(ctx)
//	if err != nil {
//		return err
//	}
//	defer prepared.Close()
//
//	// Execute with different arguments each time
//	var activeUsers []User
//	err = prepared.Select(ctx, &activeUsers, "active")
//
//	var inactiveUsers []User
//	err = prepared.Select(ctx, &inactiveUsers, "inactive")
type PreparedSelect struct {
	stmt *Stmt
}

// Get executes the prepared statement and scans exactly one result into dest.
// The dest parameter should be a pointer to a struct.
// The args are the bind parameters for the prepared statement placeholders,
// provided in the same positional order as the placeholders in the query.
//
// Example:
//
//	var user User
//	err := prepared.Get(ctx, &user, 123)
func (p *PreparedSelect) Get(ctx context.Context, dest any, args ...any) error {
	return p.stmt.GetContext(ctx, dest, args...)
}

// Select executes the prepared statement and scans all results into dest.
// The dest parameter should be a pointer to a slice of structs.
// The args are the bind parameters for the prepared statement placeholders,
// provided in the same positional order as the placeholders in the query.
//
// Example:
//
//	var users []User
//	err := prepared.Select(ctx, &users, "active")
func (p *PreparedSelect) Select(ctx context.Context, dest any, args ...any) error {
	return p.stmt.SelectContext(ctx, dest, args...)
}

// Query executes the prepared statement and returns a Rows object for iteration.
// The caller is responsible for calling Close on the returned Rows.
// The args are the bind parameters for the prepared statement placeholders.
//
// Example:
//
//	rows, err := prepared.Query(ctx, "active")
//	if err != nil {
//		return err
//	}
//	defer rows.Close()
//
//	for rows.Next() {
//		var user User
//		err := rows.StructScan(&user)
//		// ...
//	}
func (p *PreparedSelect) Query(ctx context.Context, args ...any) (*Rows, error) {
	return p.stmt.QueryContext(ctx, args...)
}

// Close closes the prepared statement and releases associated resources.
// After Close is called, the prepared statement can no longer be used.
func (p *PreparedSelect) Close() error {
	return p.stmt.Close()
}

// PreparedExec represents a prepared INSERT, UPDATE, or DELETE statement that
// can be executed multiple times with different parameter values. The SQL template
// is fixed at preparation time, and only the bind arguments change between executions.
//
// The caller is responsible for closing the prepared statement when it is no
// longer needed by calling Close().
//
// Example:
//
//	prepared, err := Update("users").
//		WithClient(client).
//		Set("last_seen", nil).
//		Where("id = ?", 0).
//		Prepare(ctx)
//	if err != nil {
//		return err
//	}
//	defer prepared.Close()
//
//	// Execute with different arguments each time
//	result, err := prepared.Exec(ctx, time.Now(), userID1)
//	result, err = prepared.Exec(ctx, time.Now(), userID2)
type PreparedExec struct {
	stmt *Stmt
}

// Exec executes the prepared statement with the given arguments.
// The args are the bind parameters for the prepared statement placeholders,
// provided in the same positional order as the placeholders in the query.
// Returns a Result containing the number of rows affected and the last insert ID.
//
// Example:
//
//	result, err := prepared.Exec(ctx, "John", "john@example.com")
//	rowsAffected, _ := result.RowsAffected()
func (p *PreparedExec) Exec(ctx context.Context, args ...any) (Result, error) {
	return p.stmt.ExecContext(ctx, args...)
}

// Close closes the prepared statement and releases associated resources.
// After Close is called, the prepared statement can no longer be used.
func (p *PreparedExec) Close() error {
	return p.stmt.Close()
}

// prepareSelect builds the SQL from a Sqler, prepares it on the client, and returns a PreparedSelect.
func prepareSelect(ctx context.Context, client Querier, sqler Sqler) (*PreparedSelect, error) {
	if client == nil {
		return nil, fmt.Errorf("no client set for query preparation")
	}

	query, _, err := sqler.ToSql()
	if err != nil {
		return nil, fmt.Errorf("could not build sql for prepare: %w", err)
	}

	stmt, err := client.Prepare(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement: %w", err)
	}

	return &PreparedSelect{stmt: stmt}, nil
}

// prepareExec builds the SQL from a Sqler, prepares it on the client, and returns a PreparedExec.
func prepareExec(ctx context.Context, client Querier, sqler Sqler) (*PreparedExec, error) {
	if client == nil {
		return nil, fmt.Errorf("no client set for query preparation")
	}

	query, _, err := sqler.ToSql()
	if err != nil {
		return nil, fmt.Errorf("could not build sql for prepare: %w", err)
	}

	stmt, err := client.Prepare(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement: %w", err)
	}

	return &PreparedExec{stmt: stmt}, nil
}
