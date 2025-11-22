package sqlc

// QG creates a new generic QueryBuilder with a database client.
// This provides a convenient entry point for building type-safe queries
// with the client pre-attached to all query builders.
//
// Example:
//
//	type User struct {
//	    ID    int    `db:"id"`
//	    Name  string `db:"name"`
//	    Email string `db:"email"`
//	}
//
//	qb := sqlc.QG[User](client)
//
//	// SELECT query
//	users, err := qb.From("users").Where("status = ?", "active").Select(ctx)
//
//	// INSERT query
//	result, err := qb.Into("users").Records(user).Exec(ctx)
//
//	// UPDATE query
//	result, err := qb.Update("users").SetRecord(user).Where("id = ?", 1).Exec(ctx)
//
//	// DELETE query
//	result, err := qb.Delete("users").Where("id = ?", 1).Exec(ctx)
func QG[T any](client Querier) *QueryBuilderG[T] {
	return &QueryBuilderG[T]{client: client}
}

// QueryBuilderG is a generic query builder factory that provides convenient
// methods to create type-safe query builders with a pre-attached database client.
// This eliminates the need to call WithClient() on each query builder.
type QueryBuilderG[T any] struct {
	client Querier
}

// From creates a new generic SelectQueryBuilder for the specified table
// with the client pre-attached. This is a convenience method equivalent to
// FromG[T](table).WithClient(client).
//
// Example:
//
//	users, err := qb.From("users").
//		Where("status = ?", "active").
//		Select(ctx)
func (q *QueryBuilderG[T]) From(table string) *SelectQueryBuilderG[T] {
	return FromG[T](table).WithClient(q.client)
}

// Into creates a new generic InsertQueryBuilder for the specified table
// with the client pre-attached. This is a convenience method equivalent to
// IntoG[T](table).WithClient(client).
//
// Example:
//
//	result, err := qb.Into("users").
//		Records(user).
//		Exec(ctx)
func (q *QueryBuilderG[T]) Into(table string) *InsertQueryBuilderG[T] {
	return IntoG[T](table).WithClient(q.client)
}

// Update creates a new generic UpdateQueryBuilder for the specified table
// with the client pre-attached. This is a convenience method equivalent to
// UpdateG[T](table).WithClient(client).
//
// Example:
//
//	result, err := qb.Update("users").
//		SetRecord(user).
//		Where("id = ?", 1).
//		Exec(ctx)
func (q *QueryBuilderG[T]) Update(table string) *UpdateQueryBuilderG[T] {
	return UpdateG[T](table).WithClient(q.client)
}

// Delete creates a new generic DeleteQueryBuilder for the specified table
// with the client pre-attached. This is a convenience method equivalent to
// DeleteG[T](table).WithClient(client).
//
// Example:
//
//	result, err := qb.Delete("users").
//		Where("status = ?", "inactive").
//		Exec(ctx)
func (q *QueryBuilderG[T]) Delete(table string) *DeleteQueryBuilderG[T] {
	return DeleteG[T](table).WithClient(q.client)
}
