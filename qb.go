package sqlc

import "fmt"

const (
	// dbStructTag is the default struct tag name used to identify database column mappings.
	// When using ForType(), this tag determines which struct fields map to database columns.
	// This can be overridden per query builder using WithConfig().
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

	// defaultPlaceholder is the default parameter placeholder format.
	// MySQL/SQLite use "?", PostgreSQL uses "$1, $2, ...", Oracle uses ":1, :2, ..."
	defaultPlaceholder = "?"
)

// Config holds configuration options for query builders.
// It allows customization of struct tags, placeholders, and other query generation settings.
type Config struct {
	// StructTag is the struct tag name to use when extracting column names from structs.
	// Default: "db"
	// Examples: "db", "json", "column", "sql"
	StructTag string

	// Placeholder is the parameter placeholder format.
	// Default: "?" (MySQL/SQLite style)
	// Examples:
	//   - "?" for MySQL, SQLite, SQL Server
	//   - "$" for PostgreSQL (will generate $1, $2, $3, ...)
	//   - ":" for Oracle (will generate :1, :2, :3, ...)
	//   - "@" for SQL Server named parameters (will generate @p1, @p2, ...)
	Placeholder string
}

// DefaultConfig returns the default configuration.
// StructTag: "db"
// Placeholder: "?" (MySQL/SQLite style)
func DefaultConfig() *Config {
	return &Config{
		StructTag:   dbStructTag,
		Placeholder: defaultPlaceholder,
	}
}

// PlaceholderFormat returns the appropriate placeholder for the given parameter index (0-based).
// For "?" it always returns "?".
// For "$" it returns "$1", "$2", "$3", etc. (PostgreSQL style)
// For ":" it returns ":1", ":2", ":3", etc. (Oracle style)
// For "@" it returns "@p1", "@p2", "@p3", etc. (SQL Server style)
func (c *Config) PlaceholderFormat(index int) string {
	switch c.Placeholder {
	case "$":
		return fmt.Sprintf("$%d", index+1)
	case ":":
		return fmt.Sprintf(":%d", index+1)
	case "@":
		return fmt.Sprintf("@p%d", index+1)
	default:
		return "?"
	}
}

// QueryBuilder provides a convenience wrapper for creating query builders with a pre-configured client.
// It eliminates the need to call WithClient() on each query by storing a client reference.
//
// Example usage:
//
//	qb := &QueryBuilder{client: myClient}
//	users := []User{}
//	err := qb.From("users").Where("status = ?", "active").Select(ctx, &users)
type QueryBuilder struct {
	client Querier
	config *Config
}

// NewQueryBuilder creates a new QueryBuilder with the specified client and default configuration.
//
// Example:
//
//	qb := NewQueryBuilder(client)
//	err := qb.From("users").Select(ctx, &users)
func NewQueryBuilder(client Querier) *QueryBuilder {
	return &QueryBuilder{
		client: client,
		config: DefaultConfig(),
	}
}

// NewQueryBuilderWithConfig creates a new QueryBuilder with the specified client and configuration.
//
// Example:
//
//	config := &Config{StructTag: "json", Placeholder: "$"}
//	qb := NewQueryBuilderWithConfig(client, config)
//	err := qb.From("users").Select(ctx, &users)
func NewQueryBuilderWithConfig(client Querier, config *Config) *QueryBuilder {
	return &QueryBuilder{
		client: client,
		config: config,
	}
}

// From creates a new SelectQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines From() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.From("users")  // Equivalent to: From("users").WithClient(myClient)
func (q *QueryBuilder) From(table string) *SelectQueryBuilder {
	builder := From(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}

// Into creates a new InsertQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines Into() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.Into("users")  // Equivalent to: Into("users").WithClient(myClient)
func (q *QueryBuilder) Into(table string) *InsertQueryBuilder {
	builder := Into(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}

// Update creates a new UpdateQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines Update() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.Update("users")  // Equivalent to: Update("users").WithClient(myClient)
func (q *QueryBuilder) Update(table string) *UpdateQueryBuilder {
	builder := Update(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}

// Delete creates a new DeleteQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines Delete() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.Delete("users")  // Equivalent to: Delete("users").WithClient(myClient)
func (q *QueryBuilder) Delete(table string) *DeleteQueryBuilder {
	builder := Delete(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}
