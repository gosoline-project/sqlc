package sqlc_test

import (
	"testing"

	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
)

// TestConfigCustomStructTag tests that custom struct tags work
func TestConfigCustomStructTag(t *testing.T) {
	type User struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	config := &sqlc.Config{
		StructTag:   "json",
		Placeholder: "?",
	}

	t.Run("SELECT with custom struct tag", func(t *testing.T) {
		user := User{ID: 1, Name: "John", Email: "john@example.com"}

		query := sqlc.From("users").
			WithConfig(config).
			ForType(user)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users`", sql)
		assert.Empty(t, args)
	})

	t.Run("INSERT with custom struct tag", func(t *testing.T) {
		user := User{ID: 1, Name: "John", Email: "john@example.com"}

		query := sqlc.Into("users").
			WithConfig(config).
			Records(user)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql)
		assert.Equal(t, []any{1, "John", "john@example.com"}, args)
	})

	t.Run("UPDATE with custom struct tag", func(t *testing.T) {
		user := User{Name: "John", Email: "john@example.com"}

		query := sqlc.Update("users").
			WithConfig(config).
			SetRecord(user).
			Where("id = ?", 1)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		// Note: fields extracted from struct include ID=0 since it's not set
		assert.Contains(t, sql, "UPDATE `users` SET")
		assert.Contains(t, sql, "WHERE id = ?")
		// ID field will be 0 (zero value) since we didn't set it
		assert.Len(t, args, 4) // id(0), name, email, where param(1)
	})
}

// TestConfigDefaultBehavior tests that default config behavior is unchanged
func TestConfigDefaultBehavior(t *testing.T) {
	t.Run("SELECT with default config uses ? placeholders", func(t *testing.T) {
		query := sqlc.From("users").
			Columns("id", "name").
			Where("status = ?", "active").
			Limit(10)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE status = ? LIMIT ?", sql)
		assert.Equal(t, []any{"active", 10}, args)
	})

	t.Run("INSERT with default config uses ? placeholders", func(t *testing.T) {
		query := sqlc.Into("users").
			Columns("id", "name").
			Values(1, "John")

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", sql)
		assert.Equal(t, []any{1, "John"}, args)
	})

	t.Run("UPDATE with default config uses ? placeholders", func(t *testing.T) {
		query := sqlc.Update("users").
			Set("name", "John").
			Where("id = ?", 1).
			Limit(1)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "UPDATE `users` SET `name` = ? WHERE id = ? LIMIT ?", sql)
		assert.Equal(t, []any{"John", 1, 1}, args)
	})

	t.Run("DELETE with default config uses ? placeholders", func(t *testing.T) {
		query := sqlc.Delete("users").
			Where("status = ?", "inactive").
			Limit(100)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "DELETE FROM `users` WHERE status = ? LIMIT ?", sql)
		assert.Equal(t, []any{"inactive", 100}, args)
	})
}

// TestConfigWithClientMethod tests that WithConfig works with all builders
func TestConfigWithClientMethod(t *testing.T) {
	config := &sqlc.Config{
		StructTag:   "json",
		Placeholder: "?",
	}

	t.Run("SELECT builder has WithConfig method", func(t *testing.T) {
		query := sqlc.From("users").WithConfig(config)
		assert.NotNil(t, query)
	})

	t.Run("INSERT builder has WithConfig method", func(t *testing.T) {
		query := sqlc.Into("users").WithConfig(config)
		assert.NotNil(t, query)
	})

	t.Run("UPDATE builder has WithConfig method", func(t *testing.T) {
		query := sqlc.Update("users").WithConfig(config)
		assert.NotNil(t, query)
	})

	t.Run("DELETE builder has WithConfig method", func(t *testing.T) {
		query := sqlc.Delete("users").WithConfig(config)
		assert.NotNil(t, query)
	})
}

// TestConfigPostgreSQLPlaceholders tests that PostgreSQL-style placeholders ($1, $2, $3) work
func TestConfigPostgreSQLPlaceholders(t *testing.T) {
	config := &sqlc.Config{
		StructTag:   "db",
		Placeholder: "$",
	}

	t.Run("SELECT with PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.From("users").
			WithConfig(config).
			Columns("id", "name", "email").
			Where("status = ?", "active").
			Where("age > ?", 18).
			Limit(10).
			Offset(5)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT `id`, `name`, `email` FROM `users` WHERE status = $1 AND age > $2 LIMIT $3 OFFSET $4", sql)
		assert.Equal(t, []any{"active", 18, 10, 5}, args)
	})

	t.Run("SELECT with HAVING and PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.From("orders").
			WithConfig(config).
			Columns("user_id", "COUNT(*) as order_count").
			GroupBy("user_id").
			Having("COUNT(*) > ?", 5).
			Having("SUM(amount) > ?", 1000).
			Limit(20)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT `user_id`, `COUNT(*) as order_count` FROM `orders` GROUP BY `user_id` HAVING COUNT(*) > $1 AND SUM(amount) > $2 LIMIT $3", sql)
		assert.Equal(t, []any{5, 1000, 20}, args)
	})

	t.Run("INSERT with PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.Into("users").
			WithConfig(config).
			Columns("id", "name", "email").
			Values(1, "John", "john@example.com").
			Values(2, "Jane", "jane@example.com")

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `email`) VALUES ($1, $2, $3), ($4, $5, $6)", sql)
		assert.Equal(t, []any{1, "John", "john@example.com", 2, "Jane", "jane@example.com"}, args)
	})

	t.Run("UPDATE with PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.Update("users").
			WithConfig(config).
			Set("name", "John").
			Set("email", "john@example.com").
			Where("id = ?", 1).
			Limit(1)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "UPDATE `users` SET `name` = $1, `email` = $2 WHERE id = $3 LIMIT $4", sql)
		assert.Equal(t, []any{"John", "john@example.com", 1, 1}, args)
	})

	t.Run("UPDATE with multiple WHERE and PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.Update("users").
			WithConfig(config).
			Set("status", "inactive").
			Where("last_login < ?", "2020-01-01").
			Where("status = ?", "active").
			Limit(100)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "UPDATE `users` SET `status` = $1 WHERE last_login < $2 AND status = $3 LIMIT $4", sql)
		assert.Equal(t, []any{"inactive", "2020-01-01", "active", 100}, args)
	})

	t.Run("DELETE with PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.Delete("users").
			WithConfig(config).
			Where("status = ?", "inactive").
			Where("created_at < ?", "2020-01-01").
			OrderBy("created_at ASC").
			Limit(100)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "DELETE FROM `users` WHERE status = $1 AND created_at < $2 ORDER BY `created_at` ASC LIMIT $3", sql)
		assert.Equal(t, []any{"inactive", "2020-01-01", 100}, args)
	})

	t.Run("INSERT with ON DUPLICATE KEY UPDATE and PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.Into("users").
			WithConfig(config).
			Columns("id", "name", "count").
			Values(1, "John", 5).
			OnDuplicateKeyUpdate(
				sqlc.Assign("count", 10),
			)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES ($1, $2, $3) ON DUPLICATE KEY UPDATE `count` = $4", sql)
		assert.Equal(t, []any{1, "John", 5, 10}, args)
	})

	t.Run("INSERT with multiple ON DUPLICATE KEY UPDATE and PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.Into("users").
			WithConfig(config).
			Columns("id", "name", "count").
			Values(1, "John", 5).
			Values(2, "Jane", 3).
			OnDuplicateKeyUpdate(
				sqlc.Assign("count", 10),
				sqlc.AssignExpr("updated_at", "NOW()"),
			)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "INSERT INTO `users` (`id`, `name`, `count`) VALUES ($1, $2, $3), ($4, $5, $6) ON DUPLICATE KEY UPDATE `count` = $7, `updated_at` = NOW()", sql)
		assert.Equal(t, []any{1, "John", 5, 2, "Jane", 3, 10}, args)
	})
}

// TestConfigOraclePlaceholders tests that Oracle-style placeholders (:1, :2, :3) work
func TestConfigOraclePlaceholders(t *testing.T) {
	config := &sqlc.Config{
		StructTag:   "db",
		Placeholder: ":",
	}

	t.Run("SELECT with Oracle placeholders", func(t *testing.T) {
		query := sqlc.From("users").
			WithConfig(config).
			Columns("id", "name").
			Where("status = ?", "active").
			Limit(10)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT `id`, `name` FROM `users` WHERE status = :1 LIMIT :2", sql)
		assert.Equal(t, []any{"active", 10}, args)
	})

	t.Run("UPDATE with Oracle placeholders", func(t *testing.T) {
		query := sqlc.Update("users").
			WithConfig(config).
			Set("name", "John").
			Where("id = ?", 1)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "UPDATE `users` SET `name` = :1 WHERE id = :2", sql)
		assert.Equal(t, []any{"John", 1}, args)
	})
}

// TestConfigPlaceholderEdgeCases tests edge cases with placeholder replacement
func TestConfigPlaceholderEdgeCases(t *testing.T) {
	config := &sqlc.Config{
		StructTag:   "db",
		Placeholder: "$",
	}

	t.Run("Complex WHERE with multiple conditions", func(t *testing.T) {
		query := sqlc.From("users").
			WithConfig(config).
			Columns("*").
			Where("(status = ? OR status = ?)", "active", "pending").
			Where("age > ?", 18).
			Where("country IN (?, ?, ?)", "US", "CA", "UK")

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` WHERE (status = $1 OR status = $2) AND age > $3 AND country IN ($4, $5, $6)", sql)
		assert.Equal(t, []any{"active", "pending", 18, "US", "CA", "UK"}, args)
	})

	t.Run("No WHERE clause with PostgreSQL placeholders", func(t *testing.T) {
		query := sqlc.From("users").
			WithConfig(config).
			Columns("*").
			Limit(10)

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` LIMIT $1", sql)
		assert.Equal(t, []any{10}, args)
	})

	t.Run("UPDATE without WHERE", func(t *testing.T) {
		query := sqlc.Update("users").
			WithConfig(config).
			Set("status", "inactive")

		sql, args, err := query.ToSql()

		assert.NoError(t, err)
		assert.Equal(t, "UPDATE `users` SET `status` = $1", sql)
		assert.Equal(t, []any{"inactive"}, args)
	})
}
