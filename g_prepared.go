package sqlc

import (
	"context"
)

// PreparedSelectG is a type-safe wrapper around PreparedSelect that provides
// generic return types for Get and Select operations. Instead of passing a
// destination pointer, the methods return the result directly with proper typing.
//
// The caller is responsible for closing the prepared statement when it is no
// longer needed by calling Close().
//
// Example:
//
//	prepared, err := FromG[User]("users").
//		WithClient(client).
//		Where("status = ?", "active").
//		Prepare(ctx)
//	if err != nil {
//		return err
//	}
//	defer prepared.Close()
//
//	user, err := prepared.Get(ctx, "active")
//	users, err := prepared.Select(ctx, "active")
type PreparedSelectG[T any] struct {
	ps *PreparedSelect
}

// Get executes the prepared statement and returns exactly one result.
// The args are the bind parameters for the prepared statement placeholders,
// provided in the same positional order as the placeholders in the query.
//
// Example:
//
//	user, err := prepared.Get(ctx, 123)
//	fmt.Println(user.Name)
func (p *PreparedSelectG[T]) Get(ctx context.Context, args ...any) (*T, error) {
	var result T
	err := p.ps.Get(ctx, &result, args...)

	return &result, err
}

// Select executes the prepared statement and returns all results as a slice.
// The args are the bind parameters for the prepared statement placeholders,
// provided in the same positional order as the placeholders in the query.
//
// Example:
//
//	users, err := prepared.Select(ctx, "active")
//	for _, user := range users {
//		fmt.Println(user.Name)
//	}
func (p *PreparedSelectG[T]) Select(ctx context.Context, args ...any) ([]T, error) {
	var result []T
	err := p.ps.Select(ctx, &result, args...)

	return result, err
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
func (p *PreparedSelectG[T]) Query(ctx context.Context, args ...any) (*Rows, error) {
	return p.ps.Query(ctx, args...)
}

// Close closes the prepared statement and releases associated resources.
// After Close is called, the prepared statement can no longer be used.
func (p *PreparedSelectG[T]) Close() error {
	return p.ps.Close()
}
