package sqlc

// Concat concatenates multiple expressions or values into a single string.
// Returns a new Expression representing CONCAT(...).
//
// Example:
//
//	Concat(Col("first_name"), Lit("' '"), Col("last_name")).As("full_name")
//	// CONCAT(`first_name`, ' ', `last_name`) AS full_name
func Concat(expressions ...*Expression) *Expression {
	return &Expression{
		function:       "CONCAT",
		subExpressions: expressions,
	}
}

// ConcatWs concatenates multiple expressions with a separator.
// The first argument is the separator, followed by the values to concatenate.
// Returns a new Expression representing CONCAT_WS(separator, ...).
//
// Example:
//
//	ConcatWs(Lit("', '"), Col("city"), Col("state"), Col("country")).As("location")
//	// CONCAT_WS(', ', `city`, `state`, `country`) AS location
func ConcatWs(separator *Expression, expressions ...*Expression) *Expression {
	allExprs := make([]*Expression, 0, len(expressions)+1)
	allExprs = append(allExprs, separator)
	allExprs = append(allExprs, expressions...)

	return &Expression{
		function:       "CONCAT_WS",
		subExpressions: allExprs,
	}
}

// Upper wraps the expression in an UPPER() function.
// Returns a new Expression that converts the string to uppercase.
//
// Example:
//
//	Col("name").Upper()              // UPPER(`name`)
//	Col("email").Upper().As("EMAIL") // UPPER(`email`) AS EMAIL
func (e *Expression) Upper() *Expression {
	return e.wrapWithFunction("UPPER")
}

// Lower wraps the expression in a LOWER() function.
// Returns a new Expression that converts the string to lowercase.
//
// Example:
//
//	Col("name").Lower()              // LOWER(`name`)
//	Col("EMAIL").Lower().As("email") // LOWER(`EMAIL`) AS email
func (e *Expression) Lower() *Expression {
	return e.wrapWithFunction("LOWER")
}

// Length wraps the expression in a LENGTH() function.
// Returns a new Expression that returns the length of the string in bytes.
//
// Example:
//
//	Col("name").Length()              // LENGTH(`name`)
//	Col("description").Length().As("desc_length") // LENGTH(`description`) AS desc_length
func (e *Expression) Length() *Expression {
	return e.wrapWithFunction("LENGTH")
}

// CharLength wraps the expression in a CHAR_LENGTH() function.
// Returns a new Expression that returns the length of the string in characters.
//
// Example:
//
//	Col("name").CharLength()              // CHAR_LENGTH(`name`)
//	Col("text").CharLength().As("char_count") // CHAR_LENGTH(`text`) AS char_count
func (e *Expression) CharLength() *Expression {
	return e.wrapWithFunction("CHAR_LENGTH")
}

// Trim wraps the expression in a TRIM() function.
// Returns a new Expression that removes leading and trailing spaces.
//
// Example:
//
//	Col("name").Trim()              // TRIM(`name`)
//	Col("text").Trim().As("cleaned") // TRIM(`text`) AS cleaned
func (e *Expression) Trim() *Expression {
	return e.wrapWithFunction("TRIM")
}

// Ltrim wraps the expression in a LTRIM() function.
// Returns a new Expression that removes leading spaces.
//
// Example:
//
//	Col("name").Ltrim()              // LTRIM(`name`)
//	Col("text").Ltrim().As("cleaned") // LTRIM(`text`) AS cleaned
func (e *Expression) Ltrim() *Expression {
	return e.wrapWithFunction("LTRIM")
}

// Rtrim wraps the expression in a RTRIM() function.
// Returns a new Expression that removes trailing spaces.
//
// Example:
//
//	Col("name").Rtrim()              // RTRIM(`name`)
//	Col("text").Rtrim().As("cleaned") // RTRIM(`text`) AS cleaned
func (e *Expression) Rtrim() *Expression {
	return e.wrapWithFunction("RTRIM")
}

// Substring wraps the expression in a SUBSTRING() function.
// Returns a new Expression that extracts a substring starting at position (1-indexed) for length characters.
//
// Example:
//
//	Col("name").Substring(1, 3)              // SUBSTRING(`name`, 1, 3)
//	Col("text").Substring(5, 10).As("excerpt") // SUBSTRING(`text`, 5, 10) AS excerpt
func (e *Expression) Substring(position, length int) *Expression {
	return e.wrapWithFunctionArgs("SUBSTRING", position, length)
}

// Left wraps the expression in a LEFT() function.
// Returns a new Expression that extracts the leftmost N characters.
//
// Example:
//
//	Col("name").Left(3)              // LEFT(`name`, 3)
//	Col("code").Left(2).As("prefix") // LEFT(`code`, 2) AS prefix
func (e *Expression) Left(length int) *Expression {
	return e.wrapWithFunctionArgs("LEFT", length)
}

// Right wraps the expression in a RIGHT() function.
// Returns a new Expression that extracts the rightmost N characters.
//
// Example:
//
//	Col("name").Right(3)              // RIGHT(`name`, 3)
//	Col("code").Right(4).As("suffix") // RIGHT(`code`, 4) AS suffix
func (e *Expression) Right(length int) *Expression {
	return e.wrapWithFunctionArgs("RIGHT", length)
}

// Replace wraps the expression in a REPLACE() function.
// Returns a new Expression that replaces all occurrences of fromStr with toStr.
//
// Example:
//
//	Col("text").Replace("old", "new")              // REPLACE(`text`, 'old', 'new')
//	Col("name").Replace(" ", "_").As("slug") // REPLACE(`name`, ' ', '_') AS slug
func (e *Expression) Replace(fromStr, toStr string) *Expression {
	return e.wrapWithFunctionArgs("REPLACE", quoteString(fromStr), quoteString(toStr))
}

// Reverse wraps the expression in a REVERSE() function.
// Returns a new Expression that reverses the string.
//
// Example:
//
//	Col("text").Reverse()              // REVERSE(`text`)
//	Col("name").Reverse().As("reversed") // REVERSE(`name`) AS reversed
func (e *Expression) Reverse() *Expression {
	return e.wrapWithFunction("REVERSE")
}

// Repeat wraps the expression in a REPEAT() function.
// Returns a new Expression that repeats the string N times.
//
// Example:
//
//	Col("char").Repeat(5)              // REPEAT(`char`, 5)
//	Col("pattern").Repeat(3).As("repeated") // REPEAT(`pattern`, 3) AS repeated
func (e *Expression) Repeat(count int) *Expression {
	return e.wrapWithFunctionArgs("REPEAT", count)
}

// Locate wraps the expression in a LOCATE() function to find substring position.
// Returns a new Expression that finds the position of substr in the string (1-indexed, 0 if not found).
//
// Example:
//
//	Col("text").Locate("word")              // LOCATE('word', `text`)
//	Col("email").Locate("@").As("at_position") // LOCATE('@', `email`) AS at_position
func (e *Expression) Locate(substr string) *Expression {
	return e.wrapWithFunctionArgs("LOCATE", quoteString(substr))
}

// Lpad wraps the expression in a LPAD() function.
// Returns a new Expression that left-pads the string to length with padStr.
//
// Example:
//
//	Col("id").Lpad(5, "0")              // LPAD(`id`, 5, '0')
//	Col("code").Lpad(10, " ").As("padded") // LPAD(`code`, 10, ' ') AS padded
func (e *Expression) Lpad(length int, padStr string) *Expression {
	return e.wrapWithFunctionArgs("LPAD", length, quoteString(padStr))
}

// Rpad wraps the expression in a RPAD() function.
// Returns a new Expression that right-pads the string to length with padStr.
//
// Example:
//
//	Col("id").Rpad(5, "0")              // RPAD(`id`, 5, '0')
//	Col("code").Rpad(10, " ").As("padded") // RPAD(`code`, 10, ' ') AS padded
func (e *Expression) Rpad(length int, padStr string) *Expression {
	return e.wrapWithFunctionArgs("RPAD", length, quoteString(padStr))
}

// quoteString wraps a string in single quotes for SQL
func quoteString(s string) string {
	// Escape single quotes by doubling them
	escaped := ""
	for _, ch := range s {
		if ch == '\'' {
			escaped += "''"
		} else {
			escaped += string(ch)
		}
	}

	return "'" + escaped + "'"
}
