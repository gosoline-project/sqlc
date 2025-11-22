package sqlc

// Count wraps the expression in a COUNT() aggregate function.
// Returns a new Expression that counts non-NULL values.
//
// Example:
//
//	Col("id").Count()        // COUNT(`id`)
//	Col("*").Count().As("total") // COUNT(*) AS total
func (e *Expression) Count() *Expression {
	return e.wrapWithFunction("COUNT")
}

// Sum wraps the expression in a SUM() aggregate function.
// Returns a new Expression that sums numeric values.
//
// Example:
//
//	Col("amount").Sum()           // SUM(`amount`)
//	Col("price").Sum().As("total") // SUM(`price`) AS total
func (e *Expression) Sum() *Expression {
	return e.wrapWithFunction("SUM")
}

// Avg wraps the expression in an AVG() aggregate function.
// Returns a new Expression that calculates the average of numeric values.
//
// Example:
//
//	Col("rating").Avg()             // AVG(`rating`)
//	Col("score").Avg().As("average") // AVG(`score`) AS average
func (e *Expression) Avg() *Expression {
	return e.wrapWithFunction("AVG")
}

// Min wraps the expression in a MIN() aggregate function.
// Returns a new Expression that finds the minimum value.
//
// Example:
//
//	Col("price").Min()              // MIN(`price`)
//	Col("created_at").Min().As("earliest") // MIN(`created_at`) AS earliest
func (e *Expression) Min() *Expression {
	return e.wrapWithFunction("MIN")
}

// Max wraps the expression in a MAX() aggregate function.
// Returns a new Expression that finds the maximum value.
//
// Example:
//
//	Col("price").Max()              // MAX(`price`)
//	Col("updated_at").Max().As("latest") // MAX(`updated_at`) AS latest
func (e *Expression) Max() *Expression {
	return e.wrapWithFunction("MAX")
}

// Abs wraps the expression in an ABS() function.
// Returns a new Expression that calculates the absolute value.
//
// Example:
//
//	Col("balance").Abs()              // ABS(`balance`)
//	Col("temperature").Abs().As("abs_temp") // ABS(`temperature`) AS abs_temp
func (e *Expression) Abs() *Expression {
	return e.wrapWithFunction("ABS")
}

// Ceil wraps the expression in a CEIL() function.
// Returns a new Expression that rounds up to the nearest integer.
//
// Example:
//
//	Col("price").Ceil()              // CEIL(`price`)
//	Col("value").Ceil().As("rounded_up") // CEIL(`value`) AS rounded_up
func (e *Expression) Ceil() *Expression {
	return e.wrapWithFunction("CEIL")
}

// Floor wraps the expression in a FLOOR() function.
// Returns a new Expression that rounds down to the nearest integer.
//
// Example:
//
//	Col("price").Floor()              // FLOOR(`price`)
//	Col("value").Floor().As("rounded_down") // FLOOR(`value`) AS rounded_down
func (e *Expression) Floor() *Expression {
	return e.wrapWithFunction("FLOOR")
}

// Round wraps the expression in a ROUND() function.
// Returns a new Expression that rounds to the nearest integer or specified decimal places.
//
// Example:
//
//	Col("price").Round()              // ROUND(`price`)
//	Col("price").RoundN(2)            // ROUND(`price`, 2)
//	Col("amount").Round().As("rounded") // ROUND(`amount`) AS rounded
func (e *Expression) Round() *Expression {
	return e.wrapWithFunction("ROUND")
}

// RoundN wraps the expression in a ROUND() function with specified decimal places.
// Returns a new Expression that rounds to N decimal places.
//
// Example:
//
//	Col("price").RoundN(2)              // ROUND(`price`, 2)
//	Col("amount").RoundN(0).As("whole") // ROUND(`amount`, 0) AS whole
func (e *Expression) RoundN(decimals int) *Expression {
	return e.wrapWithFunctionArgs("ROUND", decimals)
}

// Sqrt wraps the expression in a SQRT() function.
// Returns a new Expression that calculates the square root.
//
// Example:
//
//	Col("area").Sqrt()              // SQRT(`area`)
//	Col("value").Sqrt().As("square_root") // SQRT(`value`) AS square_root
func (e *Expression) Sqrt() *Expression {
	return e.wrapWithFunction("SQRT")
}

// Pow wraps the expression in a POW() function with a specified exponent.
// Returns a new Expression that raises the value to the given power.
//
// Example:
//
//	Col("value").Pow(2)              // POW(`value`, 2)
//	Col("base").Pow(3).As("cubed")   // POW(`base`, 3) AS cubed
func (e *Expression) Pow(exponent any) *Expression {
	return e.wrapWithFunctionArgs("POW", exponent)
}

// Mod wraps the expression in a MOD() function for modulo operation.
// Returns a new Expression that calculates the remainder of division.
//
// Example:
//
//	Col("id").Mod(10)              // MOD(`id`, 10)
//	Col("value").Mod(5).As("remainder") // MOD(`value`, 5) AS remainder
func (e *Expression) Mod(divisor any) *Expression {
	return e.wrapWithFunctionArgs("MOD", divisor)
}

// Sign wraps the expression in a SIGN() function.
// Returns a new Expression that returns the sign of the value (-1, 0, or 1).
//
// Example:
//
//	Col("balance").Sign()              // SIGN(`balance`)
//	Col("profit").Sign().As("direction") // SIGN(`profit`) AS direction
func (e *Expression) Sign() *Expression {
	return e.wrapWithFunction("SIGN")
}

// Truncate wraps the expression in a TRUNCATE() function.
// Returns a new Expression that truncates to the specified decimal places.
//
// Example:
//
//	Col("price").Truncate(2)              // TRUNCATE(`price`, 2)
//	Col("value").Truncate(0).As("truncated") // TRUNCATE(`value`, 0) AS truncated
func (e *Expression) Truncate(decimals int) *Expression {
	return e.wrapWithFunctionArgs("TRUNCATE", decimals)
}

// Rand creates a RAND() function expression.
// This is a standalone function that generates random numbers.
//
// Example:
//
//	Rand()              // RAND()
//	Rand().As("random") // RAND() AS random
func Rand() *Expression {
	return &Expression{
		raw:      "",
		function: "RAND",
	}
}

// GroupConcat wraps the expression in a GROUP_CONCAT() aggregate function.
// Returns a new Expression that concatenates values from a group into a string.
//
// Example:
//
//	Col("name").GroupConcat()              // GROUP_CONCAT(`name`)
//	Col("tag").GroupConcat().As("all_tags") // GROUP_CONCAT(`tag`) AS all_tags
func (e *Expression) GroupConcat() *Expression {
	return e.wrapWithFunction("GROUP_CONCAT")
}

// StdDev wraps the expression in a STDDEV() or STD() aggregate function.
// Returns a new Expression that calculates the standard deviation.
//
// Example:
//
//	Col("score").StdDev()              // STDDEV(`score`)
//	Col("value").StdDev().As("std_dev") // STDDEV(`value`) AS std_dev
func (e *Expression) StdDev() *Expression {
	return e.wrapWithFunction("STDDEV")
}

// Variance wraps the expression in a VARIANCE() or VAR_POP() aggregate function.
// Returns a new Expression that calculates the variance.
//
// Example:
//
//	Col("score").Variance()              // VARIANCE(`score`)
//	Col("value").Variance().As("variance") // VARIANCE(`value`) AS variance
func (e *Expression) Variance() *Expression {
	return e.wrapWithFunction("VARIANCE")
}
