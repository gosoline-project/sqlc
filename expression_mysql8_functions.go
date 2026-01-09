package sqlc

// This file contains MySQL 8.0 specific helper functions including:
// - JSON functions (JSON_EXTRACT, JSON_UNQUOTE, JSON_CONTAINS, JSON_LENGTH, etc.)
// - Date/Time functions (NOW, CURDATE, DATE, DATE_FORMAT, UNIX_TIMESTAMP, etc.)
// - Regex functions (REGEXP_LIKE, REGEXP_REPLACE)
// - NULL/conditional functions (IFNULL, NULLIF, IF)
// - Utility functions (GREATEST, LEAST)
//
// All function helpers that accept value arguments use bind parameters by default.
// Use Col() explicitly to reference columns, Lit() for inline literals.

// -----------------------------------------------------------------------------
// JSON Functions (MySQL 5.7+/8.0)
// -----------------------------------------------------------------------------

// JsonExtract extracts data from a JSON document at the specified path.
// The path argument is treated as a bind parameter.
// Returns a new Expression representing JSON_EXTRACT(doc, path).
//
// Example:
//
//	JsonExtract(Col("data"), "$.name")           // JSON_EXTRACT(`data`, ?)
//	JsonExtract(Col("config"), "$.settings.theme").As("theme")
func JsonExtract(doc *Expression, path any) *Expression {
	return buildFunc("JSON_EXTRACT", doc, path)
}

// JsonUnquote removes quotes from a JSON string value.
// Returns a new Expression representing JSON_UNQUOTE(expr).
//
// Example:
//
//	JsonUnquote(JsonExtract(Col("data"), "$.name"))  // JSON_UNQUOTE(JSON_EXTRACT(`data`, ?))
//	Col("json_field").JsonUnquote()                  // JSON_UNQUOTE(`json_field`)
func JsonUnquote(expr *Expression) *Expression {
	return buildFunc("JSON_UNQUOTE", expr)
}

// JsonUnquote as a method wraps the expression in JSON_UNQUOTE().
func (e *Expression) JsonUnquote() *Expression {
	return buildFunc("JSON_UNQUOTE", e)
}

// JsonContains checks if a JSON document contains a specific value at the given path.
// The candidate and optional path arguments are treated as bind parameters.
// Returns a new Expression representing JSON_CONTAINS(doc, candidate[, path]).
//
// Example:
//
//	JsonContains(Col("tags"), `"admin"`)                    // JSON_CONTAINS(`tags`, ?)
//	JsonContains(Col("data"), `{"active": true}`, "$.user") // JSON_CONTAINS(`data`, ?, ?)
func JsonContains(doc *Expression, candidate any, path ...any) *Expression {
	args := []any{doc, candidate}
	if len(path) > 0 {
		args = append(args, path[0])
	}

	return buildFunc("JSON_CONTAINS", args...)
}

// JsonLength returns the length of a JSON document or array at the optional path.
// The optional path argument is treated as a bind parameter.
// Returns a new Expression representing JSON_LENGTH(doc[, path]).
//
// Example:
//
//	JsonLength(Col("items"))              // JSON_LENGTH(`items`)
//	JsonLength(Col("data"), "$.array")    // JSON_LENGTH(`data`, ?)
func JsonLength(doc *Expression, path ...any) *Expression {
	args := []any{doc}
	if len(path) > 0 {
		args = append(args, path[0])
	}

	return buildFunc("JSON_LENGTH", args...)
}

// JsonType returns the type of a JSON value.
// Returns a new Expression representing JSON_TYPE(expr).
//
// Example:
//
//	JsonType(Col("data"))                         // JSON_TYPE(`data`)
//	JsonType(JsonExtract(Col("doc"), "$.field"))  // JSON_TYPE(JSON_EXTRACT(`doc`, ?))
func JsonType(expr *Expression) *Expression {
	return buildFunc("JSON_TYPE", expr)
}

// JsonKeys returns the keys from the top-level of a JSON object or at the optional path.
// Returns a new Expression representing JSON_KEYS(doc[, path]).
//
// Example:
//
//	JsonKeys(Col("config"))             // JSON_KEYS(`config`)
//	JsonKeys(Col("data"), "$.settings") // JSON_KEYS(`data`, ?)
func JsonKeys(doc *Expression, path ...any) *Expression {
	args := []any{doc}
	if len(path) > 0 {
		args = append(args, path[0])
	}

	return buildFunc("JSON_KEYS", args...)
}

// JsonValid checks if a string is valid JSON.
// Returns a new Expression representing JSON_VALID(expr).
//
// Example:
//
//	JsonValid(Col("json_string"))  // JSON_VALID(`json_string`)
func JsonValid(expr *Expression) *Expression {
	return buildFunc("JSON_VALID", expr)
}

// -----------------------------------------------------------------------------
// Date/Time Functions
// -----------------------------------------------------------------------------

// Now returns the current date and time.
// Returns a new Expression representing NOW().
//
// Example:
//
//	Now()              // NOW()
//	Now().As("current_time")
func Now() *Expression {
	return &Expression{function: "NOW", funcArgs: []*Expression{}}
}

// CurDate returns the current date.
// Returns a new Expression representing CURDATE().
//
// Example:
//
//	CurDate()              // CURDATE()
//	CurDate().As("today")
func CurDate() *Expression {
	return &Expression{function: "CURDATE", funcArgs: []*Expression{}}
}

// CurTime returns the current time.
// Returns a new Expression representing CURTIME().
//
// Example:
//
//	CurTime()              // CURTIME()
func CurTime() *Expression {
	return &Expression{function: "CURTIME", funcArgs: []*Expression{}}
}

// Date extracts the date part from a datetime expression.
// Returns a new Expression representing DATE(expr).
//
// Example:
//
//	Date(Col("created_at"))              // DATE(`created_at`)
//	Date(Col("timestamp")).As("date")
func Date(expr *Expression) *Expression {
	return buildFunc("DATE", expr)
}

// Date as a method extracts the date part from the expression.
func (e *Expression) Date() *Expression {
	return buildFunc("DATE", e)
}

// DateFormat formats a date according to the specified format string.
// The format argument is treated as a bind parameter.
// Returns a new Expression representing DATE_FORMAT(date, format).
//
// Example:
//
//	DateFormat(Col("created_at"), "%Y-%m-%d")     // DATE_FORMAT(`created_at`, ?)
//	DateFormat(Col("ts"), "%Y-%m").As("month")    // DATE_FORMAT(`ts`, ?) AS month
func DateFormat(date *Expression, format any) *Expression {
	return buildFunc("DATE_FORMAT", date, format)
}

// DateFormat as a method formats the expression according to the specified format.
func (e *Expression) DateFormat(format any) *Expression {
	return buildFunc("DATE_FORMAT", e, format)
}

// StrToDate parses a string into a date using the specified format.
// Both arguments can be expressions or values (values become bind parameters).
// Returns a new Expression representing STR_TO_DATE(str, format).
//
// Example:
//
//	StrToDate("2026-01-15", "%Y-%m-%d")           // STR_TO_DATE(?, ?)
//	StrToDate(Col("date_str"), "%Y-%m-%d")        // STR_TO_DATE(`date_str`, ?)
func StrToDate(str any, format any) *Expression {
	return buildFunc("STR_TO_DATE", str, format)
}

// UnixTimestamp returns a Unix timestamp.
// Without arguments, returns the current Unix timestamp.
// With an argument, converts the date/datetime to a Unix timestamp.
// Returns a new Expression representing UNIX_TIMESTAMP([date]).
//
// Example:
//
//	UnixTimestamp()                       // UNIX_TIMESTAMP()
//	UnixTimestamp(Col("created_at"))      // UNIX_TIMESTAMP(`created_at`)
func UnixTimestamp(date ...*Expression) *Expression {
	if len(date) == 0 {
		return &Expression{function: "UNIX_TIMESTAMP", funcArgs: []*Expression{}}
	}

	return buildFunc("UNIX_TIMESTAMP", date[0])
}

// FromUnixTime converts a Unix timestamp to a datetime.
// The optional format argument specifies the output format.
// Returns a new Expression representing FROM_UNIXTIME(timestamp[, format]).
//
// Example:
//
//	FromUnixTime(Col("ts"))                        // FROM_UNIXTIME(`ts`)
//	FromUnixTime(Col("ts"), "%Y-%m-%d")            // FROM_UNIXTIME(`ts`, ?)
func FromUnixTime(timestamp *Expression, format ...any) *Expression {
	args := []any{timestamp}
	if len(format) > 0 {
		args = append(args, format[0])
	}

	return buildFunc("FROM_UNIXTIME", args...)
}

// Year extracts the year from a date expression.
// Returns a new Expression representing YEAR(date).
//
// Example:
//
//	Year(Col("created_at"))              // YEAR(`created_at`)
func Year(date *Expression) *Expression {
	return buildFunc("YEAR", date)
}

// Year as a method extracts the year from the expression.
func (e *Expression) Year() *Expression {
	return buildFunc("YEAR", e)
}

// Month extracts the month from a date expression (1-12).
// Returns a new Expression representing MONTH(date).
//
// Example:
//
//	Month(Col("created_at"))              // MONTH(`created_at`)
func Month(date *Expression) *Expression {
	return buildFunc("MONTH", date)
}

// Month as a method extracts the month from the expression.
func (e *Expression) Month() *Expression {
	return buildFunc("MONTH", e)
}

// Day extracts the day of the month from a date expression (1-31).
// Returns a new Expression representing DAY(date).
//
// Example:
//
//	Day(Col("created_at"))              // DAY(`created_at`)
func Day(date *Expression) *Expression {
	return buildFunc("DAY", date)
}

// Day as a method extracts the day from the expression.
func (e *Expression) Day() *Expression {
	return buildFunc("DAY", e)
}

// Hour extracts the hour from a time/datetime expression (0-23).
// Returns a new Expression representing HOUR(time).
//
// Example:
//
//	Hour(Col("created_at"))              // HOUR(`created_at`)
func Hour(time *Expression) *Expression {
	return buildFunc("HOUR", time)
}

// Hour as a method extracts the hour from the expression.
func (e *Expression) Hour() *Expression {
	return buildFunc("HOUR", e)
}

// Minute extracts the minute from a time/datetime expression (0-59).
// Returns a new Expression representing MINUTE(time).
//
// Example:
//
//	Minute(Col("created_at"))              // MINUTE(`created_at`)
func Minute(time *Expression) *Expression {
	return buildFunc("MINUTE", time)
}

// Minute as a method extracts the minute from the expression.
func (e *Expression) Minute() *Expression {
	return buildFunc("MINUTE", e)
}

// Second extracts the second from a time/datetime expression (0-59).
// Returns a new Expression representing SECOND(time).
//
// Example:
//
//	Second(Col("created_at"))              // SECOND(`created_at`)
func Second(time *Expression) *Expression {
	return buildFunc("SECOND", time)
}

// Second as a method extracts the second from the expression.
func (e *Expression) Second() *Expression {
	return buildFunc("SECOND", e)
}

// DateDiff calculates the difference in days between two dates.
// Returns a new Expression representing DATEDIFF(date1, date2).
//
// Example:
//
//	DateDiff(Col("end_date"), Col("start_date"))  // DATEDIFF(`end_date`, `start_date`)
//	DateDiff(Now(), Col("created_at"))            // DATEDIFF(NOW(), `created_at`)
func DateDiff(date1, date2 *Expression) *Expression {
	return buildFunc("DATEDIFF", date1, date2)
}

// TimestampDiff calculates the difference between two datetime expressions in the specified unit.
// The unit should be a SQL interval keyword (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR, etc.)
// Note: unit is rendered as a literal (not a bind parameter) since it's a SQL keyword.
// Returns a new Expression representing TIMESTAMPDIFF(unit, datetime1, datetime2).
//
// Example:
//
//	TimestampDiff("DAY", Col("start"), Col("end"))    // TIMESTAMPDIFF(DAY, `start`, `end`)
//	TimestampDiff("HOUR", Col("created_at"), Now())   // TIMESTAMPDIFF(HOUR, `created_at`, NOW())
func TimestampDiff(unit string, datetime1, datetime2 *Expression) *Expression {
	// unit is a SQL keyword, use Lit to render it as-is (not as a bind param)
	return buildFunc("TIMESTAMPDIFF", Lit(unit), datetime1, datetime2)
}

// LastDay returns the last day of the month for a date expression.
// Returns a new Expression representing LAST_DAY(date).
//
// Example:
//
//	LastDay(Col("created_at"))  // LAST_DAY(`created_at`)
func LastDay(date *Expression) *Expression {
	return buildFunc("LAST_DAY", date)
}

// LastDay as a method returns the last day of the month for the expression.
func (e *Expression) LastDay() *Expression {
	return buildFunc("LAST_DAY", e)
}

// -----------------------------------------------------------------------------
// Regex Functions (MySQL 8.0)
// -----------------------------------------------------------------------------

// RegexpLike checks if a string matches a regular expression pattern.
// The pattern argument is treated as a bind parameter.
// Returns a new Expression representing REGEXP_LIKE(expr, pattern).
//
// Example:
//
//	RegexpLike(Col("email"), "^[a-z]+@")           // REGEXP_LIKE(`email`, ?)
//	RegexpLike(Col("name"), "^(John|Jane)")        // REGEXP_LIKE(`name`, ?)
func RegexpLike(expr *Expression, pattern any) *Expression {
	return buildFunc("REGEXP_LIKE", expr, pattern)
}

// RegexpLike as a method checks if the expression matches the pattern.
func (e *Expression) RegexpLike(pattern any) *Expression {
	return buildFunc("REGEXP_LIKE", e, pattern)
}

// RegexpReplace replaces occurrences of a pattern in a string.
// The pattern and replacement arguments are treated as bind parameters.
// Returns a new Expression representing REGEXP_REPLACE(expr, pattern, replacement).
//
// Example:
//
//	RegexpReplace(Col("text"), "[0-9]+", "X")      // REGEXP_REPLACE(`text`, ?, ?)
//	RegexpReplace(Col("name"), "\\s+", " ")        // REGEXP_REPLACE(`name`, ?, ?)
func RegexpReplace(expr *Expression, pattern, replacement any) *Expression {
	return buildFunc("REGEXP_REPLACE", expr, pattern, replacement)
}

// RegexpReplace as a method replaces pattern matches in the expression.
func (e *Expression) RegexpReplace(pattern, replacement any) *Expression {
	return buildFunc("REGEXP_REPLACE", e, pattern, replacement)
}

// RegexpInstr returns the position of a pattern match in a string.
// Returns a new Expression representing REGEXP_INSTR(expr, pattern).
//
// Example:
//
//	RegexpInstr(Col("text"), "[0-9]+")      // REGEXP_INSTR(`text`, ?)
func RegexpInstr(expr *Expression, pattern any) *Expression {
	return buildFunc("REGEXP_INSTR", expr, pattern)
}

// RegexpSubstr returns the substring that matches a pattern.
// Returns a new Expression representing REGEXP_SUBSTR(expr, pattern).
//
// Example:
//
//	RegexpSubstr(Col("text"), "[0-9]+")      // REGEXP_SUBSTR(`text`, ?)
func RegexpSubstr(expr *Expression, pattern any) *Expression {
	return buildFunc("REGEXP_SUBSTR", expr, pattern)
}

// -----------------------------------------------------------------------------
// NULL/Conditional Functions
// -----------------------------------------------------------------------------

// IfNull returns the first argument if it is not NULL, otherwise returns the second argument.
// The fallback argument is treated as a bind parameter if it's not an *Expression.
// Returns a new Expression representing IFNULL(expr, fallback).
//
// Example:
//
//	IfNull(Col("nickname"), "Anonymous")           // IFNULL(`nickname`, ?)
//	IfNull(Col("score"), Col("default_score"))     // IFNULL(`score`, `default_score`)
//	IfNull(Col("count"), 0)                        // IFNULL(`count`, ?)
func IfNull(expr *Expression, fallback any) *Expression {
	return buildFunc("IFNULL", expr, fallback)
}

// IfNull as a method returns the expression if not NULL, otherwise the fallback.
func (e *Expression) IfNull(fallback any) *Expression {
	return buildFunc("IFNULL", e, fallback)
}

// NullIf returns NULL if the two arguments are equal, otherwise returns the first argument.
// Returns a new Expression representing NULLIF(expr1, expr2).
//
// Example:
//
//	NullIf(Col("value"), 0)                // NULLIF(`value`, ?)
//	NullIf(Col("status"), "")              // NULLIF(`status`, ?)
func NullIf(expr1 *Expression, expr2 any) *Expression {
	return buildFunc("NULLIF", expr1, expr2)
}

// NullIf as a method returns NULL if the expression equals the value.
func (e *Expression) NullIf(value any) *Expression {
	return buildFunc("NULLIF", e, value)
}

// If evaluates a condition and returns one of two values.
// Returns a new Expression representing IF(condition, then_value, else_value).
//
// Example:
//
//	If(Col("status").Eq("active"), "Yes", "No")    // IF(`status` = ?, ?, ?)
//	If(Col("score").Gt(50), Col("score"), Lit(0))  // IF(`score` > ?, `score`, 0)
func If(condition *Expression, thenValue, elseValue any) *Expression {
	return buildFunc("IF", condition, thenValue, elseValue)
}

// -----------------------------------------------------------------------------
// Utility Functions
// -----------------------------------------------------------------------------

// Greatest returns the largest value from a list of arguments.
// All arguments are treated as bind parameters if they're not *Expression.
// Returns a new Expression representing GREATEST(arg1, arg2, ...).
//
// Example:
//
//	Greatest(Col("a"), Col("b"), Col("c"))         // GREATEST(`a`, `b`, `c`)
//	Greatest(Col("score"), 0)                      // GREATEST(`score`, ?)
//	Greatest(Col("val1"), Col("val2"), 100)        // GREATEST(`val1`, `val2`, ?)
func Greatest(args ...any) *Expression {
	return buildFunc("GREATEST", args...)
}

// Least returns the smallest value from a list of arguments.
// All arguments are treated as bind parameters if they're not *Expression.
// Returns a new Expression representing LEAST(arg1, arg2, ...).
//
// Example:
//
//	Least(Col("a"), Col("b"), Col("c"))            // LEAST(`a`, `b`, `c`)
//	Least(Col("score"), 100)                       // LEAST(`score`, ?)
//	Least(Col("val1"), Col("val2"), 0)             // LEAST(`val1`, `val2`, ?)
func Least(args ...any) *Expression {
	return buildFunc("LEAST", args...)
}

// Cast converts an expression to the specified type.
// Note: type is rendered as a literal since it's a SQL type keyword.
// Returns a new Expression representing CAST(expr AS type).
//
// Example:
//
//	Cast(Col("price"), "DECIMAL(10,2)")            // CAST(`price` AS DECIMAL(10,2))
//	Cast(Col("created_at"), "DATE")                // CAST(`created_at` AS DATE)
func Cast(expr *Expression, sqlType string) *Expression {
	// CAST has special syntax: CAST(expr AS type), not CAST(expr, type)
	// We use subExpressions with a special marker for the AS keyword
	return &Expression{
		function:       "CAST",
		subExpressions: []*Expression{expr, Literal("AS " + sqlType)},
	}
}

// Cast as a method converts the expression to the specified type.
func (e *Expression) Cast(sqlType string) *Expression {
	return Cast(e, sqlType)
}

// -----------------------------------------------------------------------------
// String Functions with Bind Parameters
// -----------------------------------------------------------------------------

// SubstringIndex returns a substring before/after a specified number of delimiter occurrences.
// The delimiter argument is treated as a bind parameter.
// Returns a new Expression representing SUBSTRING_INDEX(str, delim, count).
//
// Example:
//
//	SubstringIndex(Col("email"), "@", 1)           // SUBSTRING_INDEX(`email`, ?, ?)
//	SubstringIndex(Col("path"), "/", -1)           // SUBSTRING_INDEX(`path`, ?, ?)
func SubstringIndex(str *Expression, delim any, count any) *Expression {
	return buildFunc("SUBSTRING_INDEX", str, delim, count)
}

// SubstringIndex as a method returns a substring based on delimiter occurrences.
func (e *Expression) SubstringIndex(delim any, count any) *Expression {
	return buildFunc("SUBSTRING_INDEX", e, delim, count)
}
