# JSON Filter System

The JSON filter system provides a way to build SQL WHERE clauses using JSON structures. This is particularly useful for:

- Building dynamic queries from API requests
- Storing filter configurations in databases
- Creating user-defined search criteria
- Building query builders for frontend applications

## Quick Start

### Empty Filter (No-Op)

An empty filter with no type acts as a no-op and doesn't apply any filtering:

```go
// Empty filter - no WHERE clause
filter := sqlc.JsonFilter{}

expr, _ := filter.ToExpression()
// expr is nil - no filtering applied

users := []User{}
err = sqlc.From("users").
    Where(expr).  // This is ignored
    Select(ctx, &users)
// SQL: SELECT * FROM `users`
```

This is useful for dynamic queries where filters are optional:

```go
jsonStr := `{}`  // Empty JSON from API
filter, _ := sqlc.JsonFilterFromJSON(jsonStr)
expr, _ := filter.ToExpression()  // Returns nil, no error
// Query returns all records
```

### Basic Example

```go
// Create a simple filter: WHERE status = 'active'
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterEq,
    Column: "status",
    Value:  "active",
}

// Convert to Expression and use in query
expr, err := filter.ToExpression()
if err != nil {
    return err
}

users := []User{}
err = sqlc.From("users").
    Where(expr).
    Select(ctx, &users)
```

### From JSON String

```go
jsonStr := `{
    "type": "and",
    "fields": [
        {"type": "eq", "column": "status", "value": "active"},
        {"type": "gte", "column": "created_at", "value": "2024-01-01"}
    ]
}`

filter, err := sqlc.JsonFilterFromJSON(jsonStr)
if err != nil {
    return err
}

expr, err := filter.ToExpression()
// Use expr in your query...
```

## JsonFilter Types

### Comparison Operators

| JsonFilter Type | SQL Operator | Example |
|------------|--------------|---------|
| `JsonFilterEq` | `=` | `{"type": "eq", "column": "age", "value": 25}` |
| `JsonFilterNe` | `!=` | `{"type": "ne", "column": "status", "value": "deleted"}` |
| `JsonFilterGt` | `>` | `{"type": "gt", "column": "price", "value": 100}` |
| `JsonJsonFilterGte` | `>=` | `{"type": "gte", "column": "score", "value": 70}` |
| `JsonFilterLt` | `<` | `{"type": "lt", "column": "age", "value": 65}` |
| `JsonJsonFilterLte` | `<=` | `{"type": "lte", "column": "quantity", "value": 50}` |

### Set Operations

| JsonFilter Type | SQL Operator | Example |
|------------|--------------|---------|
| `JsonFilterIn` | `IN` | `{"type": "in", "column": "country", "values": ["US", "CA", "MX"]}` |
| `JsonJsonFilterNotIn` | `NOT IN` | `{"type": "not_in", "column": "status", "values": ["deleted", "archived"]}` |

### Range Operations

| JsonFilter Type | SQL Equivalent | Example |
|------------|----------------|---------|
| `JsonFilterBetween` | `column >= from AND column <= to` | `{"type": "between", "column": "age", "from": 18, "to": 65}` |

### Pattern Matching

| JsonFilter Type | SQL Operator | Example |
|------------|--------------|---------|
| `JsonFilterLike` | `LIKE` | `{"type": "like", "column": "email", "value": "%@gmail.com"}` |
| `JsonJsonFilterNotLike` | `NOT LIKE` | `{"type": "not_like", "column": "name", "value": "%test%"}` |

### NULL Checks

| JsonFilter Type | SQL Operator | Example |
|------------|--------------|---------|
| `JsonFilterIsNull` | `IS NULL` | `{"type": "is_null", "column": "deleted_at"}` |
| `JsonFilterIsNotNull` | `IS NOT NULL` | `{"type": "is_not_null", "column": "email"}` |

### Boolean Operators

| JsonFilter Type | SQL Operator | Example |
|------------|--------------|---------|
| `JsonFilterAnd` | `AND` | See nested examples below |
| `JsonFilterOr` | `OR` | See nested examples below |
| `JsonFilterNot` | `NOT` | See nested examples below |

## Examples

### Empty Filter (No-Op)

```go
// Empty filter - no WHERE clause
filter := sqlc.JsonFilter{}

expr, _ := filter.ToExpression()
// expr is nil

sql, params, _ := sqlc.From("users").Where(expr).ToSql()
// SQL: SELECT * FROM `users`
// Params: []
```

From JSON:
```go
filter, _ := sqlc.JsonFilterFromJSON("{}")
expr, _ := filter.ToExpression()  // Returns nil, no error
// Query returns all records without filtering
```

### Simple Equality

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterEq,
    Column: "status",
    Value:  "active",
}
// SQL: WHERE `status` = ?
// Params: ["active"]
```

### Range Query (BETWEEN)

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterBetween,
    Column: "age",
    From:   18,
    To:     65,
}
// SQL: WHERE (`age` >= ? AND `age` <= ?)
// Params: [18, 65]
```

### Set Membership (IN)

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterIn,
    Column: "country",
    Values: []any{"US", "CA", "MX"},
}
// SQL: WHERE `country` IN (?, ?, ?)
// Params: ["US", "CA", "MX"]
```

### Pattern Matching (LIKE)

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterLike,
    Column: "email",
    Value:  "%@gmail.com",
}
// SQL: WHERE `email` LIKE ?
// Params: ["%@gmail.com"]
```

### NULL Checks

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterIsNull,
    Column: "deleted_at",
}
// SQL: WHERE `deleted_at` IS NULL
// Params: []
```

### Complex Nested Conditions

```go
// (status = 'active' AND age > 18) OR role = 'admin'
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
// SQL: WHERE ((`status` = ? AND `age` > ?) OR `role` = ?)
// Params: ["active", 18, "admin"]
```

### E-commerce Product Search

```go
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
                {Type: sqlc.JsonJsonFilterGte, Column: "rating", Value: 4.0},
            },
        },
    },
}

expr, _ := filter.ToExpression()
products := []Product{}
err := sqlc.From("products").
    Where(expr).
    OrderBy(sqlc.Col("price").Asc()).
    Limit(20).
    Select(ctx, &products)
```

## JSON Format

### Basic Structure

```json
{
  "type": "filter_type",
  "column": "column_name",
  "value": "single_value",
  "values": ["array", "of", "values"],
  "from": "range_start",
  "to": "range_end",
  "fields": [
    { "type": "..." }
  ]
}
```

### Field Usage by JsonFilter Type

| JsonFilter Type | Required Fields | Optional Fields |
|------------|-----------------|-----------------|
| `eq`, `ne`, `lt`, `lte`, `gt`, `gte` | `type`, `column`, `value` | - |
| `in`, `not_in` | `type`, `column`, `values` | - |
| `between` | `type`, `column`, `from`, `to` | - |
| `like`, `not_like` | `type`, `column`, `value` (string) | - |
| `is_null`, `is_not_null` | `type`, `column` | - |
| `and`, `or` | `type`, `fields` (array) | - |
| `not` | `type`, `fields` (single element) | - |

### JSON Examples

#### Simple Equality
```json
{
  "type": "eq",
  "column": "status",
  "value": "active"
}
```

#### Complex Nested Query
```json
{
  "type": "and",
  "fields": [
    {
      "type": "eq",
      "column": "status",
      "value": "active"
    },
    {
      "type": "between",
      "column": "created_at",
      "from": "2024-01-01",
      "to": "2024-12-31"
    },
    {
      "type": "or",
      "fields": [
        {
          "type": "in",
          "column": "country",
          "values": ["US", "CA"]
        },
        {
          "type": "is_null",
          "column": "country"
        }
      ]
    }
  ]
}
```

## API Integration Example

### REST API Handler

```go
type SearchRequest struct {
    JsonFilter json.RawMessage `json:"filter"`
}

func SearchProducts(w http.ResponseWriter, r *http.Request) {
    var req SearchRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request", http.StatusBadRequest)
        return
    }

    // Parse the filter from JSON
    filter, err := sqlc.JsonFilterFromJSON(string(req.JsonFilter))
    if err != nil {
        http.Error(w, "Invalid filter", http.StatusBadRequest)
        return
    }

    // Convert to expression
    expr, err := filter.ToExpression()
    if err != nil {
        http.Error(w, "Invalid filter expression", http.StatusBadRequest)
        return
    }

    // Execute query
    products := []Product{}
    err = sqlc.From("products").
        Where(expr).
        Select(r.Context(), &products)
    
    if err != nil {
        http.Error(w, "Query failed", http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(products)
}
```

### Client Request

```javascript
fetch('/api/products/search', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    filter: {
      type: 'and',
      fields: [
        { type: 'eq', column: 'status', value: 'active' },
        { type: 'between', column: 'price', from: 10, to: 100 },
        { type: 'in', column: 'category', values: ['electronics', 'books'] }
      ]
    }
  })
})
```

## Error Handling

The `ToExpression()` method validates the filter structure and returns an error if:

- Required fields are missing (e.g., `column` for comparison operators)
- Invalid filter type is used
- Wrong value types (e.g., non-string value for LIKE)
- Boolean operators have wrong number of fields (e.g., NOT with multiple fields)
- Empty fields arrays for AND/OR operators

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterEq,
    // Missing Column field
    Value:  "active",
}

expr, err := filter.ToExpression()
if err != nil {
    // Error: "eq filter requires a column"
    fmt.Println(err)
}
```

## Serialization

### JsonFilter to JSON

```go
filter := sqlc.JsonFilter{
    Type:   sqlc.JsonFilterEq,
    Column: "status",
    Value:  "active",
}

jsonStr, err := filter.ToJSON()
// Returns: {"type":"eq","column":"status","value":"active"}
```

### JSON to JsonFilter

```go
jsonStr := `{"type":"eq","column":"status","value":"active"}`
filter, err := sqlc.JsonFilterFromJSON(jsonStr)
```

## Performance Considerations

- Filters are converted to parameterized queries with placeholders (`?`), preventing SQL injection
- Complex nested filters are efficiently translated to SQL with proper operator precedence
- All values are passed as parameters, enabling query plan caching in the database
- JsonFilter validation happens at conversion time, not during query execution

## Limitations

- Column names are not validated against the actual database schema
- Type checking is minimal - ensure values match column types in your database
- Complex SQL expressions (subqueries, window functions) are not supported
- Database-specific functions must use the programmatic Expression API

## See Also

- [Expression API](./expression.go) - For more complex programmatic query building
- [ParseWhere](./parser.go) - For parsing SQL WHERE strings into Expressions
- [Examples](./examples/example_json_filter.go) - Comprehensive examples
