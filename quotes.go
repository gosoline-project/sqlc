package sqlc

import "strings"

// quoteIdentifier wraps a column name with identifier quotes
// Special cases: don't quote "*" or already quoted identifiers
// Handles table-qualified columns like "table.column" -> "`table`.`column`"
// Handles JSON expressions like "data->'$.field'" -> "`data`->'$.field'"
func quoteIdentifier(name string) string {
	if name == "*" || strings.HasPrefix(name, identifierQuote) {
		return name
	}

	// Handle JSON operators (-> and ->>)
	// Extract column name before the JSON operator
	if idx := strings.Index(name, "->"); idx != -1 {
		columnPart := name[:idx]
		jsonPart := name[idx:]

		// Quote the column part (which might be table-qualified)
		quotedColumn := quoteIdentifier(columnPart)

		return quotedColumn + jsonPart
	}

	// Handle table-qualified columns (e.g., "users.id" -> "`users`.`id`")
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		quoted := make([]string, len(parts))
		for i, part := range parts {
			if part == "*" {
				quoted[i] = part
			} else {
				quoted[i] = identifierQuote + part + identifierQuote
			}
		}

		return strings.Join(quoted, ".")
	}

	return identifierQuote + name + identifierQuote
}

// quoteOrderByClause handles ORDER BY clauses which may contain "column DESC" or "column ASC"
func quoteOrderByClause(clause string) string {
	parts := strings.Fields(clause)
	if len(parts) == 0 {
		return clause
	}

	// Quote the column name (first part)
	quoted := quoteIdentifier(parts[0])

	// Preserve ASC/DESC if present
	if len(parts) > 1 {
		quoted += " " + strings.Join(parts[1:], " ")
	}

	return quoted
}
