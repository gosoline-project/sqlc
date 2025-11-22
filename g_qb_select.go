package sqlc

import (
	"context"
)

// SelectQueryBuilderG is a generic wrapper around SelectQueryBuilder that provides
// type-safe return values for Get and Select operations. Instead of passing a destination
// pointer as a parameter, the generic builder returns the result directly.
//
// Example usage:
//
//	type User struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//
//	// Get a single user - returns User directly
//	user, err := FromG[User]("users").
//	    WithClient(client).
//	    Where("id = ?", 123).
//	    Get(ctx)
//
//	// Select multiple users - returns []User directly
//	users, err := FromG[User]("users").
//	    WithClient(client).
//	    Where("status = ?", "active").
//	    Select(ctx)
type SelectQueryBuilderG[T any] struct {
	qb *SelectQueryBuilder
}

// FromG creates a new generic SelectQueryBuilder for the specified table.
// This is the entry point for building type-safe SELECT queries.
//
// Example:
//
//	FromG[User]("users")                   // SELECT * FROM `users`
//	FromG[Order]("orders").As("o")         // SELECT * FROM `orders` AS o
func FromG[T any](table string) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: From(table),
	}
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Select() or Get() methods.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := FromG[User]("users").WithClient(client).Limit(10)
//	users, err := query.Select(ctx)
func (q *SelectQueryBuilderG[T]) WithClient(client Querier) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.WithClient(client),
	}
}

// As sets an alias for the table in the FROM clause.
// Returns a new query builder with the table alias set.
//
// Example:
//
//	FromG[User]("users").As("u")           // FROM `users` AS u
//	FromG[OrderItem]("order_items").As("oi") // FROM `order_items` AS oi
func (q *SelectQueryBuilderG[T]) As(alias string) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.As(alias),
	}
}

// Columns replaces the current column list with the specified columns.
// Accepts strings (column names) or *Expression objects for more complex selections.
// Returns a new query builder with the updated column list.
//
// Example:
//
//	Columns("id", "name", "email")              // SELECT `id`, `name`, `email`
//	Columns(Col("id"), Col("name").As("user_name")) // SELECT `id`, `name` AS user_name
func (q *SelectQueryBuilderG[T]) Columns(cols ...any) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Columns(cols...),
	}
}

// Column appends a single column to the existing projection list.
// Unlike Columns(), this adds to the list rather than replacing it.
// Accepts a string (column name) or *Expression object.
// Returns a new query builder with the column added.
//
// Example:
//
//	FromG[User]("users").
//		Column("id").
//		Column("name").
//		Column(Col("email").As("contact"))  // SELECT `id`, `name`, `email` AS contact
func (q *SelectQueryBuilderG[T]) Column(col any) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Column(col),
	}
}

// Distinct adds the DISTINCT keyword to the SELECT clause.
// Returns a new query builder with DISTINCT enabled.
//
// Example:
//
//	FromG[Order]("orders").Distinct().Column("customer_id")  // SELECT DISTINCT `customer_id`
func (q *SelectQueryBuilderG[T]) Distinct() *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Distinct(),
	}
}

// Where adds a WHERE condition to the query.
// Multiple Where() calls are combined with AND.
// Accepts either:
//   - A raw SQL string with placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//   - An Eq map for creating equality conditions from column-value pairs
//
// Returns a new query builder with the condition added.
//
// Example:
//
//	Where("status = ?", "active")                    // WHERE status = ?
//	Where(Col("age").Gt(18))                         // WHERE `age` > ?
//	Where(And(Col("a").Eq(1), Col("b").Eq(2)))       // WHERE (`a` = ? AND `b` = ?)
//	Where(Eq{"status": "active", "role": "admin"})   // WHERE (`role` = ? AND `status` = ?)
func (q *SelectQueryBuilderG[T]) Where(condition any, params ...any) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Where(condition, params...),
	}
}

// GroupBy sets the GROUP BY columns for the query.
// Accepts strings (column names) or *Expression objects.
// Replaces any previously set GROUP BY clause.
// Returns a new query builder with the GROUP BY clause set.
//
// Example:
//
//	GroupBy("status")                           // GROUP BY `status`
//	GroupBy("country", "city")                  // GROUP BY `country`, `city`
func (q *SelectQueryBuilderG[T]) GroupBy(cols ...any) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.GroupBy(cols...),
	}
}

// Having adds a HAVING condition to the query (used with GROUP BY).
// Multiple Having() calls are combined with AND.
// Accepts a raw SQL string with placeholders and corresponding parameter values.
// Returns a new query builder with the HAVING condition added.
//
// Example:
//
//	GroupBy("status").Having("COUNT(*) > ?", 10)    // HAVING COUNT(*) > ?
func (q *SelectQueryBuilderG[T]) Having(condition string, params ...any) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Having(condition, params...),
	}
}

// OrderBy sets the ORDER BY clause for the query.
// Accepts strings (column names with optional ASC/DESC) or *Expression objects.
// Replaces any previously set ORDER BY clause.
// Returns a new query builder with the ORDER BY clause set.
//
// Example:
//
//	OrderBy("created_at DESC")                      // ORDER BY `created_at` DESC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
func (q *SelectQueryBuilderG[T]) OrderBy(cols ...any) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.OrderBy(cols...),
	}
}

// Limit sets the maximum number of rows to return.
// Returns a new query builder with the LIMIT clause set.
//
// Example:
//
//	Limit(10)   // LIMIT 10
func (q *SelectQueryBuilderG[T]) Limit(limit int) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Limit(limit),
	}
}

// Offset sets the number of rows to skip before returning results.
// Typically used with Limit() for pagination.
// Returns a new query builder with the OFFSET clause set.
//
// Example:
//
//	Limit(10).Offset(20)   // LIMIT 10 OFFSET 20 (page 3)
func (q *SelectQueryBuilderG[T]) Offset(offset int) *SelectQueryBuilderG[T] {
	return &SelectQueryBuilderG[T]{
		qb: q.qb.Offset(offset),
	}
}

// ToSql generates the final SQL query string and parameter list.
// Returns the SQL string, parameters slice, and any error encountered during building.
// This method should be called when you need the raw SQL for manual execution.
//
// Example:
//
//	sql, args, err := FromG[User]("users").
//		Where("status = ?", "active").
//		Limit(10).
//		ToSql()
//	// sql: "SELECT * FROM `users` WHERE status = ? LIMIT ?"
//	// args: []any{"active", 10}
func (q *SelectQueryBuilderG[T]) ToSql() (query string, params []any, err error) {
	return q.qb.ToSql()
}

// Get executes the query and returns exactly one result.
// Returns the entity and an error if the client is not set, if no rows are found,
// or if more than one row is returned.
//
// If no columns have been explicitly set via Columns() or Column(), Get will
// automatically call ForType() to map struct fields to database columns using the
// `db` struct tag.
//
// Example:
//
//	user, err := FromG[User]("users").
//		WithClient(client).
//		Where("id = ?", 123).
//		Get(ctx)
//	if err != nil {
//	    return err
//	}
//	fmt.Println(user.Name)  // user is of type User, not *User
func (q *SelectQueryBuilderG[T]) Get(ctx context.Context) (*T, error) {
	var result T
	err := q.qb.Get(ctx, &result)

	return &result, err
}

// Select executes the query and returns all results as a slice.
// Returns a slice of entities and an error if the client is not set or if the query fails.
//
// If no columns have been explicitly set via Columns() or Column(), Select will
// automatically call ForType() to map struct fields to database columns using the
// `db` struct tag.
//
// Example:
//
//	users, err := FromG[User]("users").
//		WithClient(client).
//		Where("status = ?", "active").
//		Select(ctx)
//	if err != nil {
//	    return err
//	}
//	for _, user := range users {
//	    fmt.Println(user.Name)  // users is []User, not []*User
//	}
func (q *SelectQueryBuilderG[T]) Select(ctx context.Context) ([]T, error) {
	var result []T
	err := q.qb.Select(ctx, &result)

	return result, err
}
