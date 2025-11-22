package sqlc

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/encoding/json"
)

// JsonFilterType represents the type of JSON filter operation.
type JsonFilterType string

// JSON filter types for boolean logic operators.
const (
	JsonFilterAnd JsonFilterType = "and" // Combines multiple conditions with AND
	JsonFilterOr  JsonFilterType = "or"  // Combines multiple conditions with OR
	JsonFilterNot JsonFilterType = "not" // Negates a condition
)

// JSON filter types for comparison operators.
const (
	JsonFilterEq  JsonFilterType = "eq"  // Equal: column = value
	JsonFilterNe  JsonFilterType = "ne"  // Not equal: column != value
	JsonFilterLt  JsonFilterType = "lt"  // Less than: column < value
	JsonFilterLte JsonFilterType = "lte" // Less than or equal: column <= value
	JsonFilterGt  JsonFilterType = "gt"  // Greater than: column > value
	JsonFilterGte JsonFilterType = "gte" // Greater than or equal: column >= value
)

// JSON filter types for set and range operations.
const (
	JsonFilterIn      JsonFilterType = "in"      // In list: column IN (val1, val2, ...)
	JsonFilterNotIn   JsonFilterType = "not_in"  // Not in list: column NOT IN (val1, val2, ...)
	JsonFilterBetween JsonFilterType = "between" // Between range: column BETWEEN from AND to
)

// JSON filter types for pattern matching and NULL checks.
const (
	JsonFilterLike      JsonFilterType = "like"        // Pattern match: column LIKE pattern
	JsonFilterNotLike   JsonFilterType = "not_like"    // Negated pattern match: column NOT LIKE pattern
	JsonFilterIsNull    JsonFilterType = "is_null"     // NULL check: column IS NULL
	JsonFilterIsNotNull JsonFilterType = "is_not_null" // Not NULL check: column IS NOT NULL
)

// Filter represents a JSON-based filter that can be converted to SQL WHERE clauses.
// It supports nested conditions with boolean operators (AND, OR, NOT) and various
// comparison operators.
//
// Example JSON for: status = 'ACTIVE' AND created_at BETWEEN '2024-01-01' AND '2024-12-31'
//
//	{
//	  "type": "and",
//	  "fields": [
//	    {
//	      "type": "eq",
//	      "column": "status",
//	      "value": "ACTIVE"
//	    },
//	    {
//	      "type": "between",
//	      "column": "created_at",
//	      "from": "2024-01-01",
//	      "to": "2024-12-31"
//	    }
//	  ]
//	}
//
// Example JSON for: country IN ('US', 'CA') OR country IS NULL
//
//	{
//	  "type": "or",
//	  "fields": [
//	    {
//	      "type": "in",
//	      "column": "country",
//	      "values": ["US", "CA"]
//	    },
//	    {
//	      "type": "is_null",
//	      "column": "country"
//	    }
//	  ]
//	}
type JsonFilter struct {
	// Type specifies the filter operation type (e.g., "eq", "and", "in").
	Type JsonFilterType `json:"type"`

	// Column is the database column name for leaf filters (comparison, NULL checks, etc.).
	// Not used for boolean operators (and, or, not).
	Column string `json:"column,omitempty"`

	// Value is used for single-value comparisons (eq, ne, lt, lte, gt, gte, like, not_like).
	Value any `json:"value,omitempty"`

	// Values is used for multi-value operations (in, not_in).
	Values []any `json:"values,omitempty"`

	// From and To are used for BETWEEN operations.
	From any `json:"from,omitempty"`
	To   any `json:"to,omitempty"`

	// Fields contains sub-filters for boolean operators (and, or, not).
	Fields []JsonFilter `json:"fields,omitempty"`
}

// JsonFilterFromJSON parses a JSON string into a Filter.
//
// Example:
//
//	jsonStr := `{"type": "eq", "column": "status", "value": "active"}`
//	filter, err := JsonFilterFromJSON(jsonStr)
//	if err != nil {
//	    return err
//	}
//	expr := filter.ToExpression()
func JsonFilterFromJSON(jsonStr string) (*JsonFilter, error) {
	var filter JsonFilter
	if err := json.Unmarshal([]byte(jsonStr), &filter); err != nil {
		return nil, fmt.Errorf("failed to parse filter JSON: %w", err)
	}

	return &filter, nil
}

// ToExpression converts the Filter to an Expression that can be used in SQL queries.
// It recursively processes the filter tree and returns an Expression suitable for
// use with WHERE clauses.
//
// An empty filter (no type specified) returns nil, which represents a no-op filter
// that doesn't apply any filtering.
//
// Example:
//
//	filter := JsonFilter{
//	    Type: JsonFilterAnd,
//	    Fields: []JsonFilter{
//	        {Type: JsonFilterEq, Column: "status", Value: "active"},
//	        {Type: JsonFilterGt, Column: "age", Value: 18},
//	    },
//	}
//	expr := filter.ToExpression()
//	// Can now be used in query: From("users").Where(expr)
func (f *JsonFilter) ToExpression() (*Expression, error) {
	// Empty filter (no type) is a no-op - returns nil which means no WHERE clause
	if f.Type == "" {
		return nil, nil
	}

	switch f.Type {
	// Boolean operators
	case JsonFilterAnd:
		return f.toAndExpression()
	case JsonFilterOr:
		return f.toOrExpression()
	case JsonFilterNot:
		return f.toNotExpression()

	// Comparison operators
	case JsonFilterEq:
		return f.toEqExpression()
	case JsonFilterNe:
		return f.toNeExpression()
	case JsonFilterLt:
		return f.toLtExpression()
	case JsonFilterLte:
		return f.toLteExpression()
	case JsonFilterGt:
		return f.toGtExpression()
	case JsonFilterGte:
		return f.toGteExpression()

	// Set and range operators
	case JsonFilterIn:
		return f.toInExpression()
	case JsonFilterNotIn:
		return f.toNotInExpression()
	case JsonFilterBetween:
		return f.toBetweenExpression()

	// Pattern matching and NULL checks
	case JsonFilterLike:
		return f.toLikeExpression()
	case JsonFilterNotLike:
		return f.toNotLikeExpression()
	case JsonFilterIsNull:
		return f.toIsNullExpression()
	case JsonFilterIsNotNull:
		return f.toIsNotNullExpression()

	default:
		return nil, fmt.Errorf("unknown filter type: %s", f.Type)
	}
}

// Boolean operator implementations

func (f *JsonFilter) toAndExpression() (*Expression, error) {
	if len(f.Fields) == 0 {
		return nil, fmt.Errorf("and filter requires at least one child")
	}

	expressions := make([]*Expression, len(f.Fields))
	for i, child := range f.Fields {
		expr, err := child.ToExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to convert child filter at index %d: %w", i, err)
		}
		expressions[i] = expr
	}

	return And(expressions...), nil
}

func (f *JsonFilter) toOrExpression() (*Expression, error) {
	if len(f.Fields) == 0 {
		return nil, fmt.Errorf("or filter requires at least one child")
	}

	expressions := make([]*Expression, len(f.Fields))
	for i, child := range f.Fields {
		expr, err := child.ToExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to convert child filter at index %d: %w", i, err)
		}
		expressions[i] = expr
	}

	return Or(expressions...), nil
}

func (f *JsonFilter) toNotExpression() (*Expression, error) {
	if len(f.Fields) != 1 {
		return nil, fmt.Errorf("not filter requires exactly one child, got %d", len(f.Fields))
	}

	expr, err := f.Fields[0].ToExpression()
	if err != nil {
		return nil, fmt.Errorf("failed to convert not filter child: %w", err)
	}

	return Not(expr), nil
}

// Comparison operator implementations

func (f *JsonFilter) toEqExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("eq filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("eq filter requires a value")
	}

	return Col(f.Column).Eq(f.Value), nil
}

func (f *JsonFilter) toNeExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("ne filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("ne filter requires a value")
	}

	return Col(f.Column).NotEq(f.Value), nil
}

func (f *JsonFilter) toLtExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("lt filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("lt filter requires a value")
	}

	return Col(f.Column).Lt(f.Value), nil
}

func (f *JsonFilter) toLteExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("lte filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("lte filter requires a value")
	}

	return Col(f.Column).Lte(f.Value), nil
}

func (f *JsonFilter) toGtExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("gt filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("gt filter requires a value")
	}

	return Col(f.Column).Gt(f.Value), nil
}

func (f *JsonFilter) toGteExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("gte filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("gte filter requires a value")
	}

	return Col(f.Column).Gte(f.Value), nil
}

// Set and range operator implementations

func (f *JsonFilter) toInExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("in filter requires a column")
	}
	if len(f.Values) == 0 {
		return nil, fmt.Errorf("in filter requires at least one value")
	}

	return Col(f.Column).In(f.Values...), nil
}

func (f *JsonFilter) toNotInExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("not_in filter requires a column")
	}
	if len(f.Values) == 0 {
		return nil, fmt.Errorf("not_in filter requires at least one value")
	}

	return Col(f.Column).NotIn(f.Values...), nil
}

func (f *JsonFilter) toBetweenExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("between filter requires a column")
	}
	if f.From == nil {
		return nil, fmt.Errorf("between filter requires a 'from' value")
	}
	if f.To == nil {
		return nil, fmt.Errorf("between filter requires a 'to' value")
	}

	// BETWEEN is implemented as: column >= from AND column <= to
	return And(
		Col(f.Column).Gte(f.From),
		Col(f.Column).Lte(f.To),
	), nil
}

// Pattern matching and NULL check implementations

func (f *JsonFilter) toLikeExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("like filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("like filter requires a value")
	}
	strValue, ok := f.Value.(string)
	if !ok {
		return nil, fmt.Errorf("like filter requires a string value, got %T", f.Value)
	}

	return Col(f.Column).Like(strValue), nil
}

func (f *JsonFilter) toNotLikeExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("not_like filter requires a column")
	}
	if f.Value == nil {
		return nil, fmt.Errorf("not_like filter requires a value")
	}
	strValue, ok := f.Value.(string)
	if !ok {
		return nil, fmt.Errorf("not_like filter requires a string value, got %T", f.Value)
	}
	// NOT LIKE is implemented as: NOT (column LIKE pattern)
	return Not(Col(f.Column).Like(strValue)), nil
}

func (f *JsonFilter) toIsNullExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("is_null filter requires a column")
	}

	return Col(f.Column).IsNull(), nil
}

func (f *JsonFilter) toIsNotNullExpression() (*Expression, error) {
	if f.Column == "" {
		return nil, fmt.Errorf("is_not_null filter requires a column")
	}

	return Col(f.Column).IsNotNull(), nil
}

// ToJSON converts the Filter to a JSON string.
// This is useful for serializing filters for storage or transmission.
//
// Example:
//
//	filter := JsonFilter{
//	    Type:   FilterEq,
//	    Column: "status",
//	    Value:  "active",
//	}
//	jsonStr, err := filter.ToJSON()
//	// Returns: {"type":"eq","column":"status","value":"active"}
func (f *JsonFilter) ToJSON() (string, error) {
	data, err := json.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("failed to marshal filter to JSON: %w", err)
	}

	return string(data), nil
}
