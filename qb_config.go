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

// QueryBuilderConfig holds configuration options for query builders.
// It allows customization of struct tags, placeholders, and other query generation settings.
type QueryBuilderConfig struct {
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

	// IdentifierQuote is the character used to quote SQL identifiers (table and column names).
	// Default: "`" (MySQL/MariaDB style)
	// Examples:
	//   - "`" for MySQL, MariaDB
	//   - "\"" for PostgreSQL, Oracle
	//   - "[" for SQL Server (uses [] pairs)
	IdentifierQuote string
}

// DefaultConfig returns the default configuration.
// StructTag: "db"
// Placeholder: "?" (MySQL/SQLite style)
// IdentifierQuote: "`" (MySQL/MariaDB style)
func DefaultConfig() *QueryBuilderConfig {
	return &QueryBuilderConfig{
		StructTag:       dbStructTag,
		Placeholder:     defaultPlaceholder,
		IdentifierQuote: identifierQuote,
	}
}

// PlaceholderFormat returns the appropriate placeholder for the given parameter index (0-based).
// For "?" it always returns "?".
// For "$" it returns "$1", "$2", "$3", etc. (PostgreSQL style)
// For ":" it returns ":1", ":2", ":3", etc. (Oracle style)
// For "@" it returns "@p1", "@p2", "@p3", etc. (SQL Server style)
func (c *QueryBuilderConfig) PlaceholderFormat(index int) string {
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
