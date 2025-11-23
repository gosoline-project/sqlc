package sqlc

// QueryBuilder provides a convenience wrapper for creating query builders with a pre-configured client.
// It eliminates the need to call WithClient() on each query by storing a client reference.
//
// Example usage:
//
//	qb := &QueryBuilder{client: myClient}
//	users := []User{}
//	err := qb.From("users").Where("status = ?", "active").Select(ctx, &users)
type QueryBuilder struct {
	client Querier
	config *QueryBuilderConfig
}

// NewQueryBuilder creates a new QueryBuilder with the specified client and default configuration.
//
// Example:
//
//	qb := NewQueryBuilder(client)
//	err := qb.From("users").Select(ctx, &users)
func NewQueryBuilder(client Querier, config *QueryBuilderConfig) *QueryBuilder {
	return &QueryBuilder{
		client: client,
		config: config,
	}
}

// NewQueryBuilderWithConfig creates a new QueryBuilder with the specified client and configuration.
//
// Example:
//
//	config := &QueryBuilderConfig{StructTag: "json", Placeholder: "$"}
//	qb := NewQueryBuilderWithConfig(client, config)
//	err := qb.From("users").Select(ctx, &users)
func NewQueryBuilderWithConfig(client Querier, config *QueryBuilderConfig) *QueryBuilder {
	return &QueryBuilder{
		client: client,
		config: config,
	}
}

// From creates a new SelectQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines From() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.From("users")  // Equivalent to: From("users").WithClient(myClient)
func (q *QueryBuilder) From(table string) *SelectQueryBuilder {
	builder := From(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}

// Into creates a new InsertQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines Into() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.Into("users")  // Equivalent to: Into("users").WithClient(myClient)
func (q *QueryBuilder) Into(table string) *InsertQueryBuilder {
	builder := Into(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}

// Update creates a new UpdateQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines Update() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.Update("users")  // Equivalent to: Update("users").WithClient(myClient)
func (q *QueryBuilder) Update(table string) *UpdateQueryBuilder {
	builder := Update(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}

// Delete creates a new DeleteQueryBuilder for the specified table with the client already attached.
// This is a convenience method that combines Delete() and WithClient() into a single call.
//
// Example:
//
//	qb := &QueryBuilder{client: myClient}
//	query := qb.Delete("users")  // Equivalent to: Delete("users").WithClient(myClient)
func (q *QueryBuilder) Delete(table string) *DeleteQueryBuilder {
	builder := Delete(table).WithClient(q.client)
	if q.config != nil {
		builder = builder.WithConfig(q.config)
	}
	return builder
}
