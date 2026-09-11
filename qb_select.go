package sqlc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/justtrackio/gosoline/pkg/refl"
)

// SelectQueryBuilder provides a fluent API for building SQL SELECT queries.
// It implements an immutable builder pattern - each method returns a new instance
// rather than modifying the receiver. This allows for query reuse and prevents
// accidental mutations.
//
// Example usage:
//
//	query := From("users").
//		Columns("id", "name", "email").
//		Where("status = ?", "active").
//		OrderBy("created_at DESC").
//		Limit(10)
//	sql, args, err := query.ToSql()
//
// JOIN example:
//
//	query := From("users").As("u").
//		LeftJoin("orders").As("o").On("u.id = o.user_id").
//		Columns("u.name", "o.total").
//		Where("u.status = ?", "active")
//	sql, args, err := query.ToSql()
type SelectQueryBuilder struct {
	client          Querier
	config          *QueryBuilderConfig
	table           string
	tableAlias      string
	projections     []string
	projectionExprs []*Expression // expressions in projections that may have bind parameters
	distinct        bool
	sqlerJoin       *SqlerJoin
	sqlerWhere      *SqlerWhere
	sqlerGroupBy    *SqlerGroupBy
	sqlerHaving     *SqlerHaving
	sqlerOrderBy    *SqlerOrderBy
	limitValue      *int
	offsetValue     *int
	forUpdate       bool
	err             error
}

// JoinBuilder is an intermediate builder for constructing JOIN clauses fluently.
// It is created by Join(), InnerJoin(), LeftJoin(), or RightJoin() methods on
// SelectQueryBuilder, and must be finalized with On() to produce a new SelectQueryBuilder.
//
// Example:
//
//	From("users").As("u").
//		LeftJoin("orders").As("o").On("u.id = o.user_id")
type JoinBuilder struct {
	query    *SelectQueryBuilder
	joinType JoinType
	table    string
	alias    string
}

// As sets a table alias for the joined table.
// Returns the same JoinBuilder for method chaining.
//
// Example:
//
//	LeftJoin("orders").As("o").On("u.id = o.user_id")
//	// LEFT JOIN `orders` AS o ON u.id = o.user_id
func (j *JoinBuilder) As(alias string) *JoinBuilder {
	j.alias = alias

	return j
}

// On finalizes the JOIN clause with the specified ON condition.
// Accepts either:
//   - A raw SQL string with optional placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//
// Returns a new SelectQueryBuilder with the JOIN clause added.
//
// Example with raw string:
//
//	LeftJoin("orders").As("o").On("u.id = o.user_id")
//	InnerJoin("payments").On("orders.id = payments.order_id AND payments.status = ?", "completed")
//
// Example with Expression:
//
//	LeftJoin("orders").As("o").On(Col("u.id").Eq(Col("o.user_id")))
//	InnerJoin("payments").On(And(Col("orders.id").Eq(Col("payments.order_id")), Col("payments.status").Eq("completed")))
func (j *JoinBuilder) On(condition any, params ...any) *SelectQueryBuilder {
	newQuery := j.query.copyQuery()

	clause := JoinClause{
		joinType: j.joinType,
		table:    j.table,
		alias:    j.alias,
		config:   newQuery.config,
	}

	switch v := condition.(type) {
	case string:
		clause.condition = v
		clause.params = params
	case *Expression:
		clause.condExpr = v
	default:
		newQuery.err = fmt.Errorf("invalid type for Join ON condition: expected string or *Expression, got %T", condition)

		return newQuery
	}

	newQuery.sqlerJoin.AddJoin(clause)

	return newQuery
}

// From creates a new SelectQueryBuilder for the specified table.
// This is the entry point for building SELECT queries.
//
// Example:
//
//	From("users")                   // SELECT * FROM `users`
//	From("orders").As("o")          // SELECT * FROM `orders` AS o
func From(table string) *SelectQueryBuilder {
	cfg := DefaultConfig()

	return &SelectQueryBuilder{
		table:        table,
		config:       cfg,
		projections:  []string{},
		sqlerJoin:    NewSqlerJoin().WithConfig(cfg),
		sqlerWhere:   NewSqlerWhere().WithConfig(cfg),
		sqlerGroupBy: NewSqlerGroupBy().WithConfig(cfg),
		sqlerHaving:  NewSqlerHaving().WithConfig(cfg),
		sqlerOrderBy: NewSqlerOrderBy().WithConfig(cfg),
	}
}

// copyQuery creates a shallow copy of the query builder.
// This is used internally to implement the immutable builder pattern.
// Each builder method creates a copy, modifies it, and returns the new copy.
func (q *SelectQueryBuilder) copyQuery() *SelectQueryBuilder {
	// Copy the SqlerJoin by creating a new instance with copied slices
	newSqlerJoin := &SqlerJoin{
		clauses: append([]JoinClause{}, q.sqlerJoin.clauses...),
		config:  q.sqlerJoin.config,
		err:     q.sqlerJoin.err,
	}

	// Copy the SqlerWhere by creating a new instance with copied slices
	newSqlerWhere := &SqlerWhere{
		clauses: append([]string{}, q.sqlerWhere.clauses...),
		params:  append([]any{}, q.sqlerWhere.params...),
		config:  q.sqlerWhere.config,
		err:     q.sqlerWhere.err,
	}

	// Copy the SqlerGroupBy
	newSqlerGroupBy := &SqlerGroupBy{
		clauses: append([]string{}, q.sqlerGroupBy.clauses...),
		config:  q.sqlerGroupBy.config,
		err:     q.sqlerGroupBy.err,
	}

	// Copy the SqlerHaving
	newSqlerHaving := &SqlerHaving{
		clauses: append([]string{}, q.sqlerHaving.clauses...),
		params:  append([]any{}, q.sqlerHaving.params...),
		config:  q.sqlerHaving.config,
		err:     q.sqlerHaving.err,
	}

	// Copy the SqlerOrderBy
	newSqlerOrderBy := &SqlerOrderBy{
		clauses: append([]string{}, q.sqlerOrderBy.clauses...),
		config:  q.sqlerOrderBy.config,
		err:     q.sqlerOrderBy.err,
	}

	newQuery := &SelectQueryBuilder{
		client:          q.client,
		config:          q.config,
		table:           q.table,
		tableAlias:      q.tableAlias,
		projections:     append([]string{}, q.projections...),
		projectionExprs: append([]*Expression{}, q.projectionExprs...),
		distinct:        q.distinct,
		sqlerJoin:       newSqlerJoin,
		sqlerWhere:      newSqlerWhere,
		sqlerGroupBy:    newSqlerGroupBy,
		sqlerHaving:     newSqlerHaving,
		sqlerOrderBy:    newSqlerOrderBy,
		forUpdate:       q.forUpdate,
		err:             q.err,
	}
	if q.limitValue != nil {
		val := *q.limitValue
		newQuery.limitValue = &val
	}
	if q.offsetValue != nil {
		val := *q.offsetValue
		newQuery.offsetValue = &val
	}

	return newQuery
}

// WithClient associates a database client with the query builder.
// The client is required for executing queries using Select() or Get() methods.
// Returns a new query builder with the client attached.
//
// Example:
//
//	query := From("users").WithClient(client).Limit(10)
//	err := query.Select(ctx, &users)
func (q *SelectQueryBuilder) WithClient(client Querier) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.client = client

	return newQuery
}

// WithConfig sets a custom configuration for the query builder.
// This allows customization of struct tags and parameter placeholders.
// Returns a new query builder with the config attached.
//
// Example:
//
//	config := &QueryBuilderConfig{StructTag: "json", Placeholder: "$"}
//	query := From("users").WithConfig(config).Limit(10)
//	sql, args, err := query.ToSql()
func (q *SelectQueryBuilder) WithConfig(config *QueryBuilderConfig) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.config = config
	newQuery.sqlerJoin.WithConfig(config)
	newQuery.sqlerWhere.WithConfig(config)
	newQuery.sqlerHaving.WithConfig(config)
	newQuery.sqlerGroupBy.WithConfig(config)
	newQuery.sqlerOrderBy.WithConfig(config)

	return newQuery
}

// As sets an alias for the table in the FROM clause.
// Returns a new query builder with the table alias set.
//
// Example:
//
//	From("users").As("u")           // FROM `users` AS u
//	From("order_items").As("oi")    // FROM `order_items` AS oi
func (q *SelectQueryBuilder) As(alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.tableAlias = alias

	return newQuery
}

// Join starts building an INNER JOIN clause for the specified table.
// Returns a JoinBuilder that must be finalized with On() to set the join condition.
// The JoinBuilder also supports As() for setting a table alias.
//
// Example:
//
//	From("users").As("u").
//		Join("orders").As("o").On("u.id = o.user_id")
//	// SELECT * FROM `users` AS u JOIN `orders` AS o ON u.id = o.user_id
func (q *SelectQueryBuilder) Join(table string) *JoinBuilder {
	return &JoinBuilder{
		query:    q,
		joinType: JoinInner,
		table:    table,
	}
}

// InnerJoin starts building an INNER JOIN clause for the specified table.
// This is an alias for Join(). Returns a JoinBuilder that must be finalized with On().
//
// Example:
//
//	From("users").As("u").
//		InnerJoin("orders").As("o").On("u.id = o.user_id")
//	// SELECT * FROM `users` AS u JOIN `orders` AS o ON u.id = o.user_id
func (q *SelectQueryBuilder) InnerJoin(table string) *JoinBuilder {
	return &JoinBuilder{
		query:    q,
		joinType: JoinInner,
		table:    table,
	}
}

// LeftJoin starts building a LEFT JOIN clause for the specified table.
// Returns a JoinBuilder that must be finalized with On() to set the join condition.
//
// Example:
//
//	From("users").As("u").
//		LeftJoin("profiles").As("p").On("u.id = p.user_id")
//	// SELECT * FROM `users` AS u LEFT JOIN `profiles` AS p ON u.id = p.user_id
func (q *SelectQueryBuilder) LeftJoin(table string) *JoinBuilder {
	return &JoinBuilder{
		query:    q,
		joinType: JoinLeft,
		table:    table,
	}
}

// RightJoin starts building a RIGHT JOIN clause for the specified table.
// Returns a JoinBuilder that must be finalized with On() to set the join condition.
//
// Example:
//
//	From("users").As("u").
//		RightJoin("orders").As("o").On("u.id = o.user_id")
//	// SELECT * FROM `users` AS u RIGHT JOIN `orders` AS o ON u.id = o.user_id
func (q *SelectQueryBuilder) RightJoin(table string) *JoinBuilder {
	return &JoinBuilder{
		query:    q,
		joinType: JoinRight,
		table:    table,
	}
}

// CrossJoin adds a CROSS JOIN clause for the specified table.
// CROSS JOIN produces a cartesian product and does not require an ON condition.
// Returns a new SelectQueryBuilder with the CROSS JOIN added.
//
// Example:
//
//	From("colors").CrossJoin("sizes")
//	// SELECT * FROM `colors` CROSS JOIN `sizes`
//
//	From("colors").CrossJoinAs("sizes", "s")
//	// SELECT * FROM `colors` CROSS JOIN `sizes` AS s
func (q *SelectQueryBuilder) CrossJoin(table string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinCross,
		table:    table,
		config:   newQuery.config,
	})

	return newQuery
}

// CrossJoinAs adds a CROSS JOIN clause with a table alias.
// CROSS JOIN produces a cartesian product and does not require an ON condition.
// Returns a new SelectQueryBuilder with the CROSS JOIN added.
//
// Example:
//
//	From("colors").CrossJoinAs("sizes", "s")
//	// SELECT * FROM `colors` CROSS JOIN `sizes` AS s
func (q *SelectQueryBuilder) CrossJoinAs(table string, alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinCross,
		table:    table,
		alias:    alias,
		config:   newQuery.config,
	})

	return newQuery
}

// FullOuterJoin starts building a FULL OUTER JOIN clause for the specified table.
// Returns a JoinBuilder that must be finalized with On() to set the join condition.
//
// Example:
//
//	From("employees").As("e").
//		FullOuterJoin("departments").As("d").On("e.dept_id = d.id")
//	// SELECT * FROM `employees` AS e FULL OUTER JOIN `departments` AS d ON `e`.`dept_id` = `d`.`id`
func (q *SelectQueryBuilder) FullOuterJoin(table string) *JoinBuilder {
	return &JoinBuilder{
		query:    q,
		joinType: JoinFullOuter,
		table:    table,
	}
}

// NaturalJoin adds a NATURAL JOIN clause for the specified table.
// NATURAL JOIN automatically matches on columns with the same name and does not
// require an ON condition. Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("orders").NaturalJoin("customers")
//	// SELECT * FROM `orders` NATURAL JOIN `customers`
func (q *SelectQueryBuilder) NaturalJoin(table string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNatural,
		table:    table,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalJoinAs adds a NATURAL JOIN clause with a table alias.
// Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("orders").NaturalJoinAs("customers", "c")
//	// SELECT * FROM `orders` NATURAL JOIN `customers` AS c
func (q *SelectQueryBuilder) NaturalJoinAs(table string, alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNatural,
		table:    table,
		alias:    alias,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalLeftJoin adds a NATURAL LEFT JOIN clause for the specified table.
// Combines NATURAL JOIN semantics (auto-match columns) with LEFT JOIN behavior
// (all rows from left table). Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("orders").NaturalLeftJoin("customers")
//	// SELECT * FROM `orders` NATURAL LEFT JOIN `customers`
func (q *SelectQueryBuilder) NaturalLeftJoin(table string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNaturalLeft,
		table:    table,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalLeftJoinAs adds a NATURAL LEFT JOIN clause with a table alias.
// Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("orders").NaturalLeftJoinAs("customers", "c")
//	// SELECT * FROM `orders` NATURAL LEFT JOIN `customers` AS c
func (q *SelectQueryBuilder) NaturalLeftJoinAs(table string, alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNaturalLeft,
		table:    table,
		alias:    alias,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalRightJoin adds a NATURAL RIGHT JOIN clause for the specified table.
// Combines NATURAL JOIN semantics (auto-match columns) with RIGHT JOIN behavior
// (all rows from right table). Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("orders").NaturalRightJoin("customers")
//	// SELECT * FROM `orders` NATURAL RIGHT JOIN `customers`
func (q *SelectQueryBuilder) NaturalRightJoin(table string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNaturalRight,
		table:    table,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalRightJoinAs adds a NATURAL RIGHT JOIN clause with a table alias.
// Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("orders").NaturalRightJoinAs("customers", "c")
//	// SELECT * FROM `orders` NATURAL RIGHT JOIN `customers` AS c
func (q *SelectQueryBuilder) NaturalRightJoinAs(table string, alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNaturalRight,
		table:    table,
		alias:    alias,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalFullJoin adds a NATURAL FULL OUTER JOIN clause for the specified table.
// Combines NATURAL JOIN semantics (auto-match columns) with FULL OUTER JOIN behavior
// (all rows from both tables). Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("employees").NaturalFullJoin("departments")
//	// SELECT * FROM `employees` NATURAL FULL OUTER JOIN `departments`
func (q *SelectQueryBuilder) NaturalFullJoin(table string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNaturalFull,
		table:    table,
		config:   newQuery.config,
	})

	return newQuery
}

// NaturalFullJoinAs adds a NATURAL FULL OUTER JOIN clause with a table alias.
// Returns a new SelectQueryBuilder with the join added.
//
// Example:
//
//	From("employees").NaturalFullJoinAs("departments", "d")
//	// SELECT * FROM `employees` NATURAL FULL OUTER JOIN `departments` AS d
func (q *SelectQueryBuilder) NaturalFullJoinAs(table string, alias string) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerJoin.AddJoin(JoinClause{
		joinType: JoinNaturalFull,
		table:    table,
		alias:    alias,
		config:   newQuery.config,
	})

	return newQuery
}

// Columns replaces the current column list with the specified columns.
// Accepts strings (column names) or *Expression objects for more complex selections.
// Returns a new query builder with the updated column list.
//
// Example:
//
//	Columns("id", "name", "email")              // SELECT `id`, `name`, `email`
//	Columns(Col("id"), Col("name").As("user_name")) // SELECT `id`, `name` AS user_name
//	Columns(Col("id").Count().As("total"))      // SELECT COUNT(`id`) AS total
func (q *SelectQueryBuilder) Columns(cols ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.projections = []string{}
	newQuery.projectionExprs = []*Expression{}

	for i, col := range cols {
		switch v := col.(type) {
		case string:
			newQuery.projections = append(newQuery.projections, quoteIdentifier(v, newQuery.config.IdentifierQuote))
			newQuery.projectionExprs = append(newQuery.projectionExprs, nil) // no expression for string columns
		case *Expression:
			newQuery.projections = append(newQuery.projections, v.toSQL(newQuery.config.IdentifierQuote))
			newQuery.projectionExprs = append(newQuery.projectionExprs, v) // store expression for parameter collection
		default:
			newQuery.err = fmt.Errorf("invalid type for Columns argument %d: expected string or *Expression, got %T", i, col)

			return newQuery
		}
	}

	return newQuery
}

// Column appends a single column to the existing projection list.
// Unlike Columns(), this adds to the list rather than replacing it.
// Accepts a string (column name) or *Expression object.
// Returns a new query builder with the column added.
//
// Example:
//
//	From("users").
//		Column("id").
//		Column("name").
//		Column(Col("email").As("contact"))  // SELECT `id`, `name`, `email` AS contact
func (q *SelectQueryBuilder) Column(col any) *SelectQueryBuilder {
	newQuery := q.copyQuery()

	switch v := col.(type) {
	case string:
		newQuery.projections = append(newQuery.projections, quoteIdentifier(v, newQuery.config.IdentifierQuote))
		newQuery.projectionExprs = append(newQuery.projectionExprs, nil) // no expression for string columns
	case *Expression:
		newQuery.projections = append(newQuery.projections, v.toSQL(newQuery.config.IdentifierQuote))
		newQuery.projectionExprs = append(newQuery.projectionExprs, v) // store expression for parameter collection
	default:
		newQuery.err = fmt.Errorf("invalid type for Column argument: expected string or *Expression, got %T", col)

		return newQuery
	}

	return newQuery
}

// ForType automatically sets the column list based on struct field tags.
// It uses the `db` struct tag to determine which columns to select.
// Returns a new query builder with columns set based on the struct type.
//
// Example:
//
//	type User struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//	From("users").ForType(&User{})  // SELECT `id`, `name`, `email`
func (q *SelectQueryBuilder) ForType(t any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	structTag := dbStructTag
	if newQuery.config != nil && newQuery.config.StructTag != "" {
		structTag = newQuery.config.StructTag
	}
	tags := refl.GetTags(t, structTag)

	var err error
	var cols []any

	if cols, err = refl.InterfaceToInterfaceSlice(tags); err != nil {
		newQuery.err = fmt.Errorf("could not convert tags to slice of interface: %w", err)

		return newQuery
	}

	return newQuery.Columns(cols...)
}

// Distinct adds the DISTINCT keyword to the SELECT clause.
// Returns a new query builder with DISTINCT enabled.
//
// Example:
//
//	From("orders").Distinct().Column("customer_id")  // SELECT DISTINCT `customer_id`
func (q *SelectQueryBuilder) Distinct() *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.distinct = true

	return newQuery
}

// Where adds a WHERE condition to the query.
// Multiple Where() calls are combined with AND.
// Accepts either:
//   - A raw SQL string with placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//   - An Eq map for creating equality conditions from column-value pairs
//
// Returns a new query builder with the condition added.
//
// Example:
//
//	Where("status = ?", "active")                    // WHERE status = ?
//	Where(Col("age").Gt(18))                         // WHERE `age` > ?
//	Where(And(Col("a").Eq(1), Col("b").Eq(2)))       // WHERE (`a` = ? AND `b` = ?)
//	Where("status = ?", "active").Where("age > ?", 18) // WHERE status = ? AND age > ?
//	Where(Eq{"status": "active", "role": "admin"})   // WHERE (`role` = ? AND `status` = ?)
func (q *SelectQueryBuilder) Where(condition any, params ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerWhere.Where(condition, params...)

	// Propagate any error from SqlerWhere to the query builder
	if newQuery.sqlerWhere.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerWhere.err
	}

	return newQuery
}

// GroupBy sets the GROUP BY columns for the query.
// Accepts strings (column names) or *Expression objects.
// Replaces any previously set GROUP BY clause.
// Returns a new query builder with the GROUP BY clause set.
//
// Example:
//
//	GroupBy("status")                           // GROUP BY `status`
//	GroupBy("country", "city")                  // GROUP BY `country`, `city`
//	GroupBy(Col("DATE(created_at)"))            // GROUP BY DATE(created_at)
func (q *SelectQueryBuilder) GroupBy(cols ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerGroupBy.GroupBy(cols...)

	// Propagate any error from SqlerGroupBy to the query builder
	if newQuery.sqlerGroupBy.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerGroupBy.err
	}

	return newQuery
}

// Having adds a HAVING condition to the query (used with GROUP BY).
// Multiple Having() calls are combined with AND.
// Accepts either:
//   - A raw SQL string with placeholders and corresponding parameter values
//   - An *Expression object that encapsulates the condition and parameters
//
// Returns a new query builder with the HAVING condition added.
//
// Example:
//
//	GroupBy("status").Having("COUNT(*) > ?", 10)     // HAVING COUNT(*) > ?
//	Having("SUM(amount) > ?", 1000)                  // HAVING SUM(amount) > ?
//	Having(Col("*").Count().Gt(10))                  // HAVING COUNT(*) > ?
//	Having(And(Col("*").Count().Gt(5), Col("amount").Sum().Gt(1000))) // HAVING (COUNT(*) > ? AND SUM(`amount`) > ?)
func (q *SelectQueryBuilder) Having(condition any, params ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerHaving.Having(condition, params...)

	// Propagate any error from SqlerHaving to the query builder
	if newQuery.sqlerHaving.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerHaving.err
	}

	return newQuery
}

// OrderBy sets the ORDER BY clause for the query.
// Accepts strings (column names with optional ASC/DESC) or *Expression objects.
// Replaces any previously set ORDER BY clause.
// Returns a new query builder with the ORDER BY clause set.
//
// Example:
//
//	OrderBy("created_at DESC")                      // ORDER BY `created_at` DESC
//	OrderBy("name ASC", "created_at DESC")          // ORDER BY `name` ASC, `created_at` DESC
//	OrderBy(Col("price").Desc())                    // ORDER BY `price` DESC
//	OrderBy(Col("name").Asc(), Col("id").Desc())    // ORDER BY `name` ASC, `id` DESC
func (q *SelectQueryBuilder) OrderBy(cols ...any) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.sqlerOrderBy.OrderBy(cols...)

	// Propagate any error from SqlerOrderBy to the query builder
	if newQuery.sqlerOrderBy.err != nil && newQuery.err == nil {
		newQuery.err = newQuery.sqlerOrderBy.err
	}

	return newQuery
}

// Limit sets the maximum number of rows to return.
// Returns a new query builder with the LIMIT clause set.
//
// Example:
//
//	Limit(10)   // LIMIT 10
//	Limit(100)  // LIMIT 100
func (q *SelectQueryBuilder) Limit(limit int) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.limitValue = &limit

	return newQuery
}

// Offset sets the number of rows to skip before returning results.
// Typically used with Limit() for pagination.
// Returns a new query builder with the OFFSET clause set.
//
// Example:
//
//	Limit(10).Offset(20)   // LIMIT 10 OFFSET 20 (page 3)
//	Limit(50).Offset(0)    // LIMIT 50 OFFSET 0 (page 1)
func (q *SelectQueryBuilder) Offset(offset int) *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.offsetValue = &offset

	return newQuery
}

// ForUpdate locks the selected rows until the current transaction ends.
// Returns a new query builder with the FOR UPDATE clause enabled.
func (q *SelectQueryBuilder) ForUpdate() *SelectQueryBuilder {
	newQuery := q.copyQuery()
	newQuery.forUpdate = true

	return newQuery
}

func (q *SelectQueryBuilder) writeProjections(sqlBuilder *strings.Builder) (params []any) {
	if len(q.projections) == 0 {
		sqlBuilder.WriteString("*")

		return nil
	}

	sqlBuilder.WriteString(strings.Join(q.projections, ", "))
	for _, expr := range q.projectionExprs {
		if expr != nil {
			params = append(params, expr.collectParameters()...)
		}
	}

	return params
}

// ToSql generates the final SQL query string and parameter list.
// Returns the SQL string, parameters slice, and any error encountered during building.
// This method should be called when you need the raw SQL for manual execution.
//
// Example:
//
//	sql, args, err := From("users").
//		Where("status = ?", "active").
//		Limit(10).
//		ToSql()
//	// sql: "SELECT * FROM `users` WHERE status = ? LIMIT ?"
//	// args: []any{"active", 10}
func (q *SelectQueryBuilder) ToSql() (query string, params []any, err error) {
	// Check if the query has any errors from previous operations
	if q.err != nil {
		return "", nil, q.err
	}

	if q.table == "" {
		return "", nil, errors.New("table name is required")
	}

	var sql string
	var args []any
	var sqlBuilder strings.Builder
	paramIndex := 0 // Track current parameter index for numbered placeholders (0-based)

	// SELECT clause
	sqlBuilder.WriteString("SELECT ")
	if q.distinct {
		sqlBuilder.WriteString("DISTINCT ")
	}

	params = q.writeProjections(&sqlBuilder)
	paramIndex = len(params)

	// FROM clause
	sqlBuilder.WriteString(" FROM ")
	sqlBuilder.WriteString(quoteIdentifier(q.table, q.config.IdentifierQuote))
	if q.tableAlias != "" {
		sqlBuilder.WriteString(" AS ")
		sqlBuilder.WriteString(q.tableAlias)
	}

	// JOIN clauses
	if sql, args, err = q.sqlerJoin.toSqlWithStartIndex(paramIndex); err != nil {
		return "", nil, fmt.Errorf("could not build JOIN clause: %w", err)
	}
	if sql != "" {
		sqlBuilder.WriteString(" ")
		sqlBuilder.WriteString(sql)
		params = append(params, args...)
		paramIndex += len(args)
	}

	// WHERE clause
	if sql, args, err = q.sqlerWhere.toSqlWithStartIndex(paramIndex); err != nil {
		return "", nil, fmt.Errorf("could not build WHERE clause: %w", err)
	}
	if sql != "" {
		sqlBuilder.WriteString(" WHERE ")
		sqlBuilder.WriteString(sql)
		params = append(params, args...)
		paramIndex += len(args)
	}

	// GROUP BY clause
	if sql, err = q.sqlerGroupBy.ToSql(); err != nil {
		return "", nil, fmt.Errorf("could not build GROUP BY clause: %w", err)
	}
	if sql != "" {
		sqlBuilder.WriteString(" GROUP BY ")
		sqlBuilder.WriteString(sql)
	}

	// HAVING clause
	if sql, args, err = q.sqlerHaving.toSqlWithStartIndex(paramIndex); err != nil {
		return "", nil, fmt.Errorf("could not build HAVING clause: %w", err)
	}
	if sql != "" {
		sqlBuilder.WriteString(" HAVING ")
		sqlBuilder.WriteString(sql)
		params = append(params, args...)
		paramIndex += len(args)
	}

	// ORDER BY clause
	if sql, err = q.sqlerOrderBy.ToSql(); err != nil {
		return "", nil, fmt.Errorf("could not build ORDER BY clause: %w", err)
	}
	if sql != "" {
		sqlBuilder.WriteString(" ORDER BY ")
		sqlBuilder.WriteString(sql)
	}

	// LIMIT clause
	if q.limitValue != nil {
		_, _ = fmt.Fprintf(&sqlBuilder, " LIMIT %s", q.config.PlaceholderFormat(paramIndex))
		params = append(params, *q.limitValue)
		paramIndex++
	}

	// OFFSET clause
	if q.offsetValue != nil {
		_, _ = fmt.Fprintf(&sqlBuilder, " OFFSET %s", q.config.PlaceholderFormat(paramIndex))
		params = append(params, *q.offsetValue)
	}

	if q.forUpdate {
		sqlBuilder.WriteString(" FOR UPDATE")
	}

	return sqlBuilder.String(), params, nil
}

// Select executes the query and scans all results into the provided destination.
// The destination should be a pointer to a slice of structs.
//
// If no columns have been explicitly set via Columns() or Column(), Select will
// automatically call ForType() to map struct fields to database columns using the
// `db` struct tag. If columns have been explicitly set, those columns will be used
// as-is without calling ForType().
//
// Returns an error if the client is not set or if the query fails.
//
// Example with automatic column detection:
//
//	var users []User
//	err := From("users").
//		WithClient(client).
//		Where("status = ?", "active").
//		Select(ctx, &users)  // Automatically selects columns based on User struct
//
// Example with explicit columns:
//
//	var users []User
//	err := From("users").
//		Columns("id", "name").  // Explicit columns - ForType() not called
//		WithClient(client).
//		Select(ctx, &users)
func (q *SelectQueryBuilder) Select(ctx context.Context, dest any) error {
	if err := validatePointer(dest, "Select", true); err != nil {
		return err
	}

	if q.client == nil {
		return errors.New("no client set for query execution")
	}

	qb := q
	if len(q.projections) == 0 {
		qb = qb.ForType(dest)
	}

	var err error
	var sql string
	var args []any

	if sql, args, err = qb.ToSql(); err != nil {
		return fmt.Errorf("could not build sql for execution: %w", err)
	}

	return qb.client.Select(ctx, dest, sql, args...)
}

// Get executes the query and scans exactly one result into the provided destination.
// The destination should be a pointer to a single struct.
//
// If no columns have been explicitly set via Columns() or Column(), Get will
// automatically call ForType() to map struct fields to database columns using the
// `db` struct tag. If columns have been explicitly set, those columns will be used
// as-is without calling ForType().
//
// Returns an error if the client is not set, if no rows are found,
// or if more than one row is returned.
//
// Example with automatic column detection:
//
//	var user User
//	err := From("users").
//		WithClient(client).
//		Where("id = ?", 123).
//		Get(ctx, &user)  // Automatically selects columns based on User struct
//
// Example with explicit columns:
//
//	var user User
//	err := From("users").
//		Columns("id", "name").  // Explicit columns - ForType() not called
//		WithClient(client).
//		Where("id = ?", 123).
//		Get(ctx, &user)
func (q *SelectQueryBuilder) Get(ctx context.Context, dest any) error {
	if err := validatePointer(dest, "Get", false); err != nil {
		return err
	}

	// Get should receive a single struct, not a slice
	rv := reflect.ValueOf(dest)
	// Get the element that the pointer points to
	elem := rv.Elem()
	if elem.Kind() == reflect.Slice {
		return fmt.Errorf("Get: destination must be a single struct, not a slice (got %T). Use Select() for multiple results or pass a pointer to a struct", dest)
	}

	if q.client == nil {
		return errors.New("no client set for query execution")
	}

	qb := q
	if len(q.projections) == 0 {
		// Only call ForType for struct destinations
		// For primitive types and maps, let the row scanner handle scanning directly
		if elem.Kind() == reflect.Struct {
			qb = qb.ForType(dest)
		}
	}

	var err error
	var sql string
	var args []any

	if sql, args, err = qb.ToSql(); err != nil {
		return fmt.Errorf("could not build sql for execution: %w", err)
	}

	return qb.client.Get(ctx, dest, sql, args...)
}

// Prepare creates a prepared SELECT statement from the current query builder state.
// The SQL template is fixed at preparation time. The placeholder values used in
// builder methods like Where() are discarded - only the SQL template matters.
// The caller must supply all bind arguments when executing the prepared statement.
//
// The caller is responsible for closing the prepared statement when it is no
// longer needed by calling Close() on the returned PreparedSelect.
//
// Example:
//
//	prepared, err := From("users").
//		WithClient(client).
//		Columns("id", "name").
//		Where("status = ?", "active").
//		Prepare(ctx)
//	if err != nil {
//		return err
//	}
//	defer prepared.Close()
//
//	var activeUsers []User
//	err = prepared.Select(ctx, &activeUsers, "active")
func (q *SelectQueryBuilder) Prepare(ctx context.Context) (*PreparedSelect, error) {
	return prepareSelect(ctx, q.client, q)
}

// validatePointer checks if the provided value is a pointer.
// If requireStructOrSlice is true, it also checks that the pointer
// points to a struct or slice. Returns a descriptive error if the
// value is not a valid pointer.
func validatePointer(v any, funcName string, requireStructOrSlice bool) error {
	if v == nil {
		return fmt.Errorf("%s: destination cannot be nil", funcName)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer {
		return fmt.Errorf("%s: destination must be a pointer, got %T (use &%T instead)", funcName, v, v)
	}

	if rv.IsNil() {
		return fmt.Errorf("%s: destination pointer cannot be nil", funcName)
	}

	if !requireStructOrSlice {
		return nil
	}

	// Check that the pointer points to a struct or slice
	elem := rv.Elem()
	elemKind := elem.Kind()
	if elemKind != reflect.Struct && elemKind != reflect.Slice {
		return fmt.Errorf("%s: destination must be a pointer to a struct or slice, got pointer to %s", funcName, elemKind)
	}

	return nil
}
