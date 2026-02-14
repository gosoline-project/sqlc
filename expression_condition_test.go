package sqlc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionColumnToColumnEq(t *testing.T) {
	expr := Col("users.id").Eq(Col("orders.user_id"))
	sql := expr.toConditionSQL("`")
	params := expr.collectParameters()

	assert.Equal(t, "`users`.`id` = `orders`.`user_id`", sql)
	assert.Empty(t, params)
}

func TestExpressionColumnToColumnNotEq(t *testing.T) {
	expr := Col("a.x").NotEq(Col("b.y"))
	sql := expr.toConditionSQL("`")
	params := expr.collectParameters()

	assert.Equal(t, "`a`.`x` != `b`.`y`", sql)
	assert.Empty(t, params)
}

func TestExpressionColumnToColumnGt(t *testing.T) {
	expr := Col("a.score").Gt(Col("b.min_score"))
	sql := expr.toConditionSQL("`")
	params := expr.collectParameters()

	assert.Equal(t, "`a`.`score` > `b`.`min_score`", sql)
	assert.Empty(t, params)
}

func TestExpressionColumnToValueStillWorks(t *testing.T) {
	expr := Col("status").Eq("active")
	sql := expr.toConditionSQL("`")
	params := expr.collectParameters()

	assert.Equal(t, "`status` = ?", sql)
	assert.Equal(t, []any{"active"}, params)
}

func TestExpressionColumnToColumnInWhere(t *testing.T) {
	q := From("users").
		Columns("*").
		Where(Col("users.manager_id").Eq(Col("users.id")))

	sql, params, err := q.ToSql()
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM `users` WHERE `users`.`manager_id` = `users`.`id`", sql)
	assert.Empty(t, params)
}

func TestExpressionColumnToColumnComposite(t *testing.T) {
	expr := And(
		Col("a.id").Eq(Col("b.a_id")),
		Col("a.type").Eq("active"),
		Col("b.score").Gte(Col("a.min_score")),
	)
	sql := expr.toConditionSQL("`")
	params := expr.collectParameters()

	assert.Equal(t, "(`a`.`id` = `b`.`a_id` AND `a`.`type` = ? AND `b`.`score` >= `a`.`min_score`)", sql)
	assert.Equal(t, []any{"active"}, params)
}
