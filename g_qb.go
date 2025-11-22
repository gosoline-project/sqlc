package sqlc

func QG[T any](client Querier) *QueryBuilderG[T] {
	return &QueryBuilderG[T]{client: client}
}

type QueryBuilderG[T any] struct {
	client Querier
}

func (q *QueryBuilderG[T]) From(table string) *SelectQueryBuilderG[T] {
	return FromG[T](table).WithClient(q.client)
}
