package sqlc

import (
	"strings"
	"unicode"

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

// sqlKeywords is the set of SQL keywords that should not be quoted in condition strings.
var sqlKeywords = map[string]bool{
	"AND":     true,
	"OR":      true,
	"NOT":     true,
	"IN":      true,
	"IS":      true,
	"NULL":    true,
	"BETWEEN": true,
	"LIKE":    true,
	"TRUE":    true,
	"FALSE":   true,
	"EXISTS":  true,
	"ON":      true,
}

// isOperator returns true if the token is a SQL comparison or logical operator.
func isOperator(token string) bool {
	switch token {
	case "=", "!=", "<>", "<", ">", "<=", ">=":
		return true
	default:
		return false
	}
}

// isNumericLiteral returns true if the token looks like a numeric literal (integer or decimal).
func isNumericLiteral(token string) bool {
	if len(token) == 0 {
		return false
	}

	dotSeen := false

	for i, ch := range token {
		if ch == '-' && i == 0 {
			continue // leading minus
		}

		if ch == '.' && !dotSeen {
			dotSeen = true

			continue
		}

		if !unicode.IsDigit(ch) {
			return false
		}
	}

	return true
}

// quoteConditionIdentifiers parses a raw SQL condition string and quotes any identifiers it finds.
// SQL keywords (AND, OR, NOT, etc.), operators (=, !=, etc.), placeholders (?),
// string literals ('...'), numeric literals, and already-quoted identifiers are left unchanged.
// All other tokens are treated as identifiers and quoted using quoteIdentifier().
//
// This is used to auto-quote identifiers in raw JOIN ON condition strings.
//
// Example:
//
//	quoteConditionIdentifiers("u.id = o.user_id", "`")
//	// returns: "`u`.`id` = `o`.`user_id`"
//
//	quoteConditionIdentifiers("u.id = o.user_id AND o.status = ?", "`")
//	// returns: "`u`.`id` = `o`.`user_id` AND `o`.`status` = ?"
func quoteConditionIdentifiers(condition string, quote string) string {
	if quote == "" {
		quote = identifierQuote
	}

	if condition == "" {
		return condition
	}

	tokens := strings.Fields(condition)
	quoted := make([]string, 0, len(tokens))

	for _, token := range tokens {
		upper := strings.ToUpper(token)

		switch {
		case sqlKeywords[upper]:
			// SQL keyword — leave as-is
			quoted = append(quoted, token)
		case isOperator(token):
			// Comparison operator — leave as-is
			quoted = append(quoted, token)
		case token == "?":
			// Placeholder — leave as-is
			quoted = append(quoted, token)
		case strings.HasPrefix(token, quote):
			// Already quoted — leave as-is
			quoted = append(quoted, token)
		case strings.HasPrefix(token, "'"):
			// String literal — leave as-is
			quoted = append(quoted, token)
		case strings.ContainsAny(token, "()"):
			// Contains parentheses (function call, subexpression) — leave as-is
			quoted = append(quoted, token)
		case isNumericLiteral(token):
			// Numeric literal — leave as-is
			quoted = append(quoted, token)
		default:
			// Identifier — quote it
			quoted = append(quoted, quoteIdentifier(token, quote))
		}
	}

	return strings.Join(quoted, " ")
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
