package sqlc

import (
	"strings"

	"github.com/justtrackio/gosoline/pkg/funk"
)

// quoteIdentifier wraps a column name with identifier quotes
// Special cases: don't quote "*" or already quoted identifiers
// Handles table-qualified columns like "table.column" -> "`table`.`column`"
// Handles JSON expressions like "data->'$.field'" -> "`data`->'$.field'"
func quoteIdentifier(name string, quote string) string {
	// Use default quote if not specified
	if quote == "" {
		quote = identifierQuote
	}

	if name == "*" || strings.HasPrefix(name, quote) {
		return name
	}

	// Handle JSON operators (-> and ->>)
	// Extract column name before the JSON operator
	if idx := strings.Index(name, "->"); idx != -1 {
		columnPart := name[:idx]
		jsonPart := name[idx:]

		// Quote the column part (which might be table-qualified)
		quotedColumn := quoteIdentifier(columnPart, quote)

		return quotedColumn + jsonPart
	}

	// Handle table-qualified columns (e.g., "users.id" -> "`users`.`id`")
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		quoted := funk.Map(parts, func(part string) string {
			if part == "*" {
				return part
			}
			return quote + part + quote
		})

		return strings.Join(quoted, ".")
	}

	return quote + name + quote
}

// quoteOrderByClause handles ORDER BY clauses which may contain "column DESC" or "column ASC"
func quoteOrderByClause(clause string, quote string) string {
	// Use default quote if not specified
	if quote == "" {
		quote = identifierQuote
	}

	parts := strings.Fields(clause)
	if len(parts) == 0 {
		return clause
	}

	// Quote the column name (first part)
	quoted := quoteIdentifier(parts[0], quote)

	// Preserve ASC/DESC if present
	if len(parts) > 1 {
		quoted += " " + strings.Join(parts[1:], " ")
	}

	return quoted
}
