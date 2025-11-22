package main

import (
	"context"
	"fmt"
	"log"

	sqlc "github.com/gosoline-project/sqlc"
)

// This example demonstrates how to use JSON-based filters to build SQL WHERE clauses.
// The JSON filter system allows you to construct complex WHERE conditions using a
// simple JSON structure, making it ideal for:
//   - Building dynamic queries from API requests
//   - Storing filter configurations in databases
//   - Creating user-defined search criteria
//   - Building query builders for frontend applications
func ExampleJsonFilter() {
	fmt.Println("JSON Filter Examples for MySQL")
	fmt.Println("=" + repeat("=", 50) + "\n")

	// Example 0: Empty filter (no-op)
	example0EmptyFilter()

	// Example 1: Simple equality filter
	example1SimpleEquality()

	// Example 2: Range queries with BETWEEN
	example2BetweenRange()

	// Example 3: Set membership with IN
	example3InOperator()

	// Example 4: Complex nested conditions (AND/OR)
	example4NestedConditions()

	// Example 5: NULL checks
	example5NullChecks()

	// Example 6: Pattern matching with LIKE
	example6PatternMatching()

	// Example 7: Building from JSON string (API use case)
	example7FromJSON()

	// Example 8: Real-world e-commerce product search
	example8EcommerceSearch()
}

func example0EmptyFilter() {
	fmt.Println("Example 0: Empty Filter (No-Op)")
	fmt.Println("-" + repeat("-", 40))

	// Empty filter - acts as a no-op, no WHERE clause added
	filter := sqlc.JsonFilter{}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Println("Empty filter: {}")
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n", params)
	fmt.Println("Result: No WHERE clause - returns all records")
}

func example1SimpleEquality() {
	fmt.Println("Example 1: Simple Equality Filter")
	fmt.Println("-" + repeat("-", 40))

	// Create a filter: WHERE status = 'active'
	filter := sqlc.JsonFilter{
		Type:   sqlc.JsonFilterEq,
		Column: "status",
		Value:  "active",
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Printf("Filter: %+v\n", filter)
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example2BetweenRange() {
	fmt.Println("Example 2: Range Query with BETWEEN")
	fmt.Println("-" + repeat("-", 40))

	// Create a filter: WHERE age BETWEEN 18 AND 65
	filter := sqlc.JsonFilter{
		Type:   sqlc.JsonFilterBetween,
		Column: "age",
		From:   18,
		To:     65,
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Printf("Filter: age BETWEEN %v AND %v\n", filter.From, filter.To)
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example3InOperator() {
	fmt.Println("Example 3: Set Membership with IN")
	fmt.Println("-" + repeat("-", 40))

	// Create a filter: WHERE country IN ('US', 'CA', 'MX')
	filter := sqlc.JsonFilter{
		Type:   sqlc.JsonFilterIn,
		Column: "country",
		Values: []any{"US", "CA", "MX"},
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Printf("Filter: country IN %v\n", filter.Values)
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example4NestedConditions() {
	fmt.Println("Example 4: Complex Nested Conditions (AND/OR)")
	fmt.Println("-" + repeat("-", 40))

	// Create a filter: WHERE (status = 'active' AND age > 18) OR role = 'admin'
	filter := sqlc.JsonFilter{
		Type: sqlc.JsonFilterOr,
		Fields: []sqlc.JsonFilter{
			{
				Type: sqlc.JsonFilterAnd,
				Fields: []sqlc.JsonFilter{
					{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
					{Type: sqlc.JsonFilterGt, Column: "age", Value: 18},
				},
			},
			{Type: sqlc.JsonFilterEq, Column: "role", Value: "admin"},
		},
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Println("Filter: (status = 'active' AND age > 18) OR role = 'admin'")
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example5NullChecks() {
	fmt.Println("Example 5: NULL Checks")
	fmt.Println("-" + repeat("-", 40))

	// Create a filter: WHERE deleted_at IS NULL (active records)
	filter := sqlc.JsonFilter{
		Type:   sqlc.JsonFilterIsNull,
		Column: "deleted_at",
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Println("Filter: WHERE deleted_at IS NULL")
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example6PatternMatching() {
	fmt.Println("Example 6: Pattern Matching with LIKE")
	fmt.Println("-" + repeat("-", 40))

	// Create a filter: WHERE email LIKE '%@gmail.com'
	filter := sqlc.JsonFilter{
		Type:   sqlc.JsonFilterLike,
		Column: "email",
		Value:  "%@gmail.com",
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Printf("Filter: email LIKE '%s'\n", filter.Value)
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example7FromJSON() {
	fmt.Println("Example 7: Building from JSON String (API Use Case)")
	fmt.Println("-" + repeat("-", 40))

	// Simulate receiving JSON from an API request
	jsonStr := `{
		"type": "and",
		"fields": [
			{"type": "eq", "column": "status", "value": "active"},
			{"type": "gte", "column": "created_at", "value": "2024-01-01"}
		]
	}`

	filter, err := sqlc.JsonFilterFromJSON(jsonStr)
	if err != nil {
		log.Fatal(err)
	}

	expr, _ := filter.ToExpression()
	sql, params, _ := sqlc.From("users").Where(expr).ToSql()

	fmt.Println("Input JSON:")
	fmt.Println(jsonStr)
	fmt.Printf("\nSQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

func example8EcommerceSearch() {
	fmt.Println("Example 8: Real-World E-commerce Product Search")
	fmt.Println("-" + repeat("-", 40))

	// Complex filter for product search:
	// - Active products only
	// - Price between $10 and $100
	// - In specific categories
	// - Has stock available
	// - (Brand is 'Apple' OR rating >= 4.0)
	filter := sqlc.JsonFilter{
		Type: sqlc.JsonFilterAnd,
		Fields: []sqlc.JsonFilter{
			{Type: sqlc.JsonFilterEq, Column: "status", Value: "active"},
			{Type: sqlc.JsonFilterBetween, Column: "price", From: 10.0, To: 100.0},
			{Type: sqlc.JsonFilterIn, Column: "category", Values: []any{"electronics", "accessories"}},
			{Type: sqlc.JsonFilterGt, Column: "stock", Value: 0},
			{
				Type: sqlc.JsonFilterOr,
				Fields: []sqlc.JsonFilter{
					{Type: sqlc.JsonFilterEq, Column: "brand", Value: "Apple"},
					{Type: sqlc.JsonFilterGte, Column: "rating", Value: 4.0},
				},
			},
		},
	}

	expr, _ := filter.ToExpression()
	query := sqlc.From("products").
		Where(expr).
		OrderBy(sqlc.Col("price").Asc()).
		Limit(20)

	sql, params, _ := query.ToSql()

	fmt.Println("Search Criteria:")
	fmt.Println("  - Active products")
	fmt.Println("  - Price: $10 - $100")
	fmt.Println("  - Categories: electronics, accessories")
	fmt.Println("  - In stock")
	fmt.Println("  - (Apple brand OR rating >= 4.0)")
	fmt.Println()
	fmt.Printf("SQL:    %s\n", sql)
	fmt.Printf("Params: %v\n\n", params)
}

// Helper function to repeat a string
func repeat(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}

	return result
}

// mockClient for demonstration purposes
//
type mockClient struct{}

func (m *mockClient) Query(_ context.Context, _ string, _ ...any) (sqlc.Result, error) {
	return nil, nil
}

func (m *mockClient) QueryRow(_ context.Context, _ string, _ ...any) sqlc.Result {
	return nil
}

func (m *mockClient) Exec(_ context.Context, _ string, _ ...any) (sqlc.Result, error) {
	return nil, nil
}

func (m *mockClient) Begin(_ context.Context) (sqlc.Tx, error) {
	return nil, nil
}
