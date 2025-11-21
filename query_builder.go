package sqlg

const (
	// dbStructTag is the struct tag name used to identify database column mappings.
	// When using ForType(), this tag determines which struct fields map to database columns.
	//
	// Example:
	//   type User struct {
	//       ID    int    `db:"id"`
	//       Name  string `db:"name"`
	//       Email string `db:"email"`
	//   }
	dbStructTag = "db"

	// identifierQuote is the character used to quote SQL identifiers (table and column names).
	// Using backticks (`) for MySQL/MariaDB compatibility. This can be changed to
	// double quotes (") for PostgreSQL or square brackets ([]) for SQL Server.
	identifierQuote = "`"
)

// QueryBuilder provides a convenience wrapper for creating query builders with a pre-configured client.
// It eliminates the need to call WithClient() on each query by storing a client reference.
//
// Example usage:
//
//	qb := &QueryBuilder{client: myClient}
//	users := []User{}
//	err := qb.From("users").Where("status = ?", "active").Select(ctx, &users)
type QueryBuilder struct {
	client Client
}

// From creates a new SelectQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines From() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.From("users")  // Equivalent to: From("users").WithClient(myClient)
func (q *QueryBuilder) From(table string) *SelectQueryBuilder {
	return From(table).WithClient(q.client)
}
