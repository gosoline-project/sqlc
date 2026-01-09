package sqlc_test

import (
	"testing"

	"github.com/gosoline-project/sqlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Param and buildFunc infrastructure tests
// -----------------------------------------------------------------------------

func TestParam(t *testing.T) {
	t.Run("Param creates bind parameter expression", func(t *testing.T) {
		// Use Param in a function context
		expr := sqlc.IfNull(sqlc.Col("name"), sqlc.Param("default"))

		sql, params, err := sqlc.From("users").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IFNULL(`name`, ?) FROM `users`", sql)
		assert.Equal(t, []any{"default"}, params)
	})

	t.Run("Param with numeric value", func(t *testing.T) {
		expr := sqlc.IfNull(sqlc.Col("count"), sqlc.Param(0))

		sql, params, err := sqlc.From("items").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IFNULL(`count`, ?) FROM `items`", sql)
		assert.Equal(t, []any{0}, params)
	})
}

func TestStringArgsAreBindParameters(t *testing.T) {
	t.Run("String args in function helpers become bind params", func(t *testing.T) {
		// DateFormat with string format
		expr := sqlc.DateFormat(sqlc.Col("created_at"), "%Y-%m-%d")

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT DATE_FORMAT(`created_at`, ?) FROM `events`", sql)
		assert.Equal(t, []any{"%Y-%m-%d"}, params)
	})
}

// -----------------------------------------------------------------------------
// JSON Function tests
// -----------------------------------------------------------------------------

func TestJsonExtract(t *testing.T) {
	t.Run("basic JSON_EXTRACT", func(t *testing.T) {
		expr := sqlc.JsonExtract(sqlc.Col("data"), "$.name")

		sql, params, err := sqlc.From("users").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_EXTRACT(`data`, ?) FROM `users`", sql)
		assert.Equal(t, []any{"$.name"}, params)
	})

	t.Run("JSON_EXTRACT with alias", func(t *testing.T) {
		expr := sqlc.JsonExtract(sqlc.Col("config"), "$.theme").As("theme")

		sql, params, err := sqlc.From("settings").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_EXTRACT(`config`, ?) AS theme FROM `settings`", sql)
		assert.Equal(t, []any{"$.theme"}, params)
	})

	t.Run("JSON_EXTRACT in WHERE clause", func(t *testing.T) {
		sql, params, err := sqlc.From("users").
			Where(sqlc.JsonExtract(sqlc.Col("data"), "$.status").Eq("active")).
			ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` WHERE JSON_EXTRACT(`data`, ?) = ?", sql)
		assert.Equal(t, []any{"$.status", "active"}, params)
	})
}

func TestJsonUnquote(t *testing.T) {
	t.Run("JSON_UNQUOTE with JsonExtract", func(t *testing.T) {
		expr := sqlc.JsonUnquote(sqlc.JsonExtract(sqlc.Col("data"), "$.name"))

		sql, params, err := sqlc.From("users").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_UNQUOTE(JSON_EXTRACT(`data`, ?)) FROM `users`", sql)
		assert.Equal(t, []any{"$.name"}, params)
	})

	t.Run("JSON_UNQUOTE as method", func(t *testing.T) {
		expr := sqlc.Col("json_field").JsonUnquote()

		sql, params, err := sqlc.From("data").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_UNQUOTE(`json_field`) FROM `data`", sql)
		assert.Empty(t, params)
	})
}

func TestJsonContains(t *testing.T) {
	t.Run("JSON_CONTAINS with two args", func(t *testing.T) {
		expr := sqlc.JsonContains(sqlc.Col("tags"), `"admin"`)

		sql, params, err := sqlc.From("users").
			Where(expr.Eq(1)).
			ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` WHERE JSON_CONTAINS(`tags`, ?) = ?", sql)
		assert.Equal(t, []any{`"admin"`, 1}, params)
	})

	t.Run("JSON_CONTAINS with path", func(t *testing.T) {
		expr := sqlc.JsonContains(sqlc.Col("data"), `{"active": true}`, "$.user")

		sql, params, err := sqlc.From("settings").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_CONTAINS(`data`, ?, ?) FROM `settings`", sql)
		assert.Equal(t, []any{`{"active": true}`, "$.user"}, params)
	})
}

func TestJsonLength(t *testing.T) {
	t.Run("JSON_LENGTH without path", func(t *testing.T) {
		expr := sqlc.JsonLength(sqlc.Col("items"))

		sql, params, err := sqlc.From("orders").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_LENGTH(`items`) FROM `orders`", sql)
		assert.Empty(t, params)
	})

	t.Run("JSON_LENGTH with path", func(t *testing.T) {
		expr := sqlc.JsonLength(sqlc.Col("data"), "$.items")

		sql, params, err := sqlc.From("orders").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_LENGTH(`data`, ?) FROM `orders`", sql)
		assert.Equal(t, []any{"$.items"}, params)
	})
}

func TestJsonType(t *testing.T) {
	expr := sqlc.JsonType(sqlc.Col("data"))

	sql, params, err := sqlc.From("items").Columns(expr).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT JSON_TYPE(`data`) FROM `items`", sql)
	assert.Empty(t, params)
}

func TestJsonKeys(t *testing.T) {
	t.Run("JSON_KEYS without path", func(t *testing.T) {
		expr := sqlc.JsonKeys(sqlc.Col("config"))

		sql, params, err := sqlc.From("settings").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_KEYS(`config`) FROM `settings`", sql)
		assert.Empty(t, params)
	})

	t.Run("JSON_KEYS with path", func(t *testing.T) {
		expr := sqlc.JsonKeys(sqlc.Col("data"), "$.settings")

		sql, params, err := sqlc.From("config").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT JSON_KEYS(`data`, ?) FROM `config`", sql)
		assert.Equal(t, []any{"$.settings"}, params)
	})
}

func TestJsonValid(t *testing.T) {
	expr := sqlc.JsonValid(sqlc.Col("json_string"))

	sql, params, err := sqlc.From("data").Columns(expr).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT JSON_VALID(`json_string`) FROM `data`", sql)
	assert.Empty(t, params)
}

// -----------------------------------------------------------------------------
// Date/Time Function tests
// -----------------------------------------------------------------------------

func TestNow(t *testing.T) {
	expr := sqlc.Now()

	sql, params, err := sqlc.From("events").Columns(expr.As("current_time")).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT NOW() AS current_time FROM `events`", sql)
	assert.Empty(t, params)
}

func TestCurDate(t *testing.T) {
	expr := sqlc.CurDate()

	sql, params, err := sqlc.From("events").Columns(expr.As("today")).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT CURDATE() AS today FROM `events`", sql)
	assert.Empty(t, params)
}

func TestCurTime(t *testing.T) {
	expr := sqlc.CurTime()

	sql, params, err := sqlc.From("events").Columns(expr).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT CURTIME() FROM `events`", sql)
	assert.Empty(t, params)
}

func TestDate(t *testing.T) {
	t.Run("Date function", func(t *testing.T) {
		expr := sqlc.Date(sqlc.Col("created_at"))

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT DATE(`created_at`) FROM `events`", sql)
		assert.Empty(t, params)
	})

	t.Run("Date as method", func(t *testing.T) {
		expr := sqlc.Col("timestamp").Date().As("date_only")

		sql, params, err := sqlc.From("logs").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT DATE(`timestamp`) AS date_only FROM `logs`", sql)
		assert.Empty(t, params)
	})
}

func TestDateFormat(t *testing.T) {
	t.Run("DateFormat function", func(t *testing.T) {
		expr := sqlc.DateFormat(sqlc.Col("created_at"), "%Y-%m-%d")

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT DATE_FORMAT(`created_at`, ?) FROM `events`", sql)
		assert.Equal(t, []any{"%Y-%m-%d"}, params)
	})

	t.Run("DateFormat as method with alias", func(t *testing.T) {
		expr := sqlc.Col("ts").DateFormat("%Y-%m").As("month")

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT DATE_FORMAT(`ts`, ?) AS month FROM `events`", sql)
		assert.Equal(t, []any{"%Y-%m"}, params)
	})

	t.Run("DateFormat in WHERE clause", func(t *testing.T) {
		sql, params, err := sqlc.From("events").
			Where(sqlc.DateFormat(sqlc.Col("created_at"), "%Y-%m").Eq("2026-01")).
			ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `events` WHERE DATE_FORMAT(`created_at`, ?) = ?", sql)
		assert.Equal(t, []any{"%Y-%m", "2026-01"}, params)
	})
}

func TestStrToDate(t *testing.T) {
	t.Run("StrToDate with string value", func(t *testing.T) {
		expr := sqlc.StrToDate("2026-01-15", "%Y-%m-%d")

		sql, params, err := sqlc.From("dummy").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT STR_TO_DATE(?, ?) FROM `dummy`", sql)
		assert.Equal(t, []any{"2026-01-15", "%Y-%m-%d"}, params)
	})

	t.Run("StrToDate with column", func(t *testing.T) {
		expr := sqlc.StrToDate(sqlc.Col("date_str"), "%Y-%m-%d")

		sql, params, err := sqlc.From("data").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT STR_TO_DATE(`date_str`, ?) FROM `data`", sql)
		assert.Equal(t, []any{"%Y-%m-%d"}, params)
	})
}

func TestUnixTimestamp(t *testing.T) {
	t.Run("UnixTimestamp without args", func(t *testing.T) {
		expr := sqlc.UnixTimestamp()

		sql, params, err := sqlc.From("dummy").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT UNIX_TIMESTAMP() FROM `dummy`", sql)
		assert.Empty(t, params)
	})

	t.Run("UnixTimestamp with column", func(t *testing.T) {
		expr := sqlc.UnixTimestamp(sqlc.Col("created_at"))

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT UNIX_TIMESTAMP(`created_at`) FROM `events`", sql)
		assert.Empty(t, params)
	})
}

func TestFromUnixTime(t *testing.T) {
	t.Run("FromUnixTime without format", func(t *testing.T) {
		expr := sqlc.FromUnixTime(sqlc.Col("ts"))

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT FROM_UNIXTIME(`ts`) FROM `events`", sql)
		assert.Empty(t, params)
	})

	t.Run("FromUnixTime with format", func(t *testing.T) {
		expr := sqlc.FromUnixTime(sqlc.Col("ts"), "%Y-%m-%d")

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT FROM_UNIXTIME(`ts`, ?) FROM `events`", sql)
		assert.Equal(t, []any{"%Y-%m-%d"}, params)
	})
}

func TestDateParts(t *testing.T) {
	tests := []struct {
		name     string
		expr     *sqlc.Expression
		expected string
	}{
		{"Year function", sqlc.Year(sqlc.Col("date")), "SELECT YEAR(`date`) FROM `t`"},
		{"Year method", sqlc.Col("date").Year(), "SELECT YEAR(`date`) FROM `t`"},
		{"Month function", sqlc.Month(sqlc.Col("date")), "SELECT MONTH(`date`) FROM `t`"},
		{"Month method", sqlc.Col("date").Month(), "SELECT MONTH(`date`) FROM `t`"},
		{"Day function", sqlc.Day(sqlc.Col("date")), "SELECT DAY(`date`) FROM `t`"},
		{"Day method", sqlc.Col("date").Day(), "SELECT DAY(`date`) FROM `t`"},
		{"Hour function", sqlc.Hour(sqlc.Col("time")), "SELECT HOUR(`time`) FROM `t`"},
		{"Hour method", sqlc.Col("time").Hour(), "SELECT HOUR(`time`) FROM `t`"},
		{"Minute function", sqlc.Minute(sqlc.Col("time")), "SELECT MINUTE(`time`) FROM `t`"},
		{"Minute method", sqlc.Col("time").Minute(), "SELECT MINUTE(`time`) FROM `t`"},
		{"Second function", sqlc.Second(sqlc.Col("time")), "SELECT SECOND(`time`) FROM `t`"},
		{"Second method", sqlc.Col("time").Second(), "SELECT SECOND(`time`) FROM `t`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params, err := sqlc.From("t").Columns(tt.expr).ToSql()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, sql)
			assert.Empty(t, params)
		})
	}
}

func TestDateDiff(t *testing.T) {
	expr := sqlc.DateDiff(sqlc.Col("end_date"), sqlc.Col("start_date"))

	sql, params, err := sqlc.From("projects").Columns(expr.As("duration")).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT DATEDIFF(`end_date`, `start_date`) AS duration FROM `projects`", sql)
	assert.Empty(t, params)
}

func TestTimestampDiff(t *testing.T) {
	expr := sqlc.TimestampDiff("DAY", sqlc.Col("start"), sqlc.Col("end"))

	sql, params, err := sqlc.From("events").Columns(expr.As("days")).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT TIMESTAMPDIFF(DAY, `start`, `end`) AS days FROM `events`", sql)
	assert.Empty(t, params) // unit is literal, not param
}

func TestLastDay(t *testing.T) {
	t.Run("LastDay function", func(t *testing.T) {
		expr := sqlc.LastDay(sqlc.Col("date"))

		sql, params, err := sqlc.From("calendar").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT LAST_DAY(`date`) FROM `calendar`", sql)
		assert.Empty(t, params)
	})

	t.Run("LastDay method", func(t *testing.T) {
		expr := sqlc.Col("created_at").LastDay()

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT LAST_DAY(`created_at`) FROM `events`", sql)
		assert.Empty(t, params)
	})
}

// -----------------------------------------------------------------------------
// Regex Function tests
// -----------------------------------------------------------------------------

func TestRegexpLike(t *testing.T) {
	t.Run("RegexpLike function", func(t *testing.T) {
		sql, params, err := sqlc.From("users").
			Where(sqlc.RegexpLike(sqlc.Col("email"), "^[a-z]+@").Eq(1)).
			ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` WHERE REGEXP_LIKE(`email`, ?) = ?", sql)
		assert.Equal(t, []any{"^[a-z]+@", 1}, params)
	})

	t.Run("RegexpLike method", func(t *testing.T) {
		sql, params, err := sqlc.From("users").
			Where(sqlc.Col("name").RegexpLike("^(John|Jane)").Eq(1)).
			ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` WHERE REGEXP_LIKE(`name`, ?) = ?", sql)
		assert.Equal(t, []any{"^(John|Jane)", 1}, params)
	})
}

func TestRegexpReplace(t *testing.T) {
	t.Run("RegexpReplace function", func(t *testing.T) {
		expr := sqlc.RegexpReplace(sqlc.Col("text"), "[0-9]+", "X")

		sql, params, err := sqlc.From("documents").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT REGEXP_REPLACE(`text`, ?, ?) FROM `documents`", sql)
		assert.Equal(t, []any{"[0-9]+", "X"}, params)
	})

	t.Run("RegexpReplace method", func(t *testing.T) {
		expr := sqlc.Col("name").RegexpReplace(`\s+`, " ")

		sql, params, err := sqlc.From("users").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT REGEXP_REPLACE(`name`, ?, ?) FROM `users`", sql)
		assert.Equal(t, []any{`\s+`, " "}, params)
	})
}

func TestRegexpInstr(t *testing.T) {
	expr := sqlc.RegexpInstr(sqlc.Col("text"), "[0-9]+")

	sql, params, err := sqlc.From("documents").Columns(expr).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT REGEXP_INSTR(`text`, ?) FROM `documents`", sql)
	assert.Equal(t, []any{"[0-9]+"}, params)
}

func TestRegexpSubstr(t *testing.T) {
	expr := sqlc.RegexpSubstr(sqlc.Col("text"), "[0-9]+")

	sql, params, err := sqlc.From("documents").Columns(expr).ToSql()
	require.NoError(t, err)
	assert.Equal(t, "SELECT REGEXP_SUBSTR(`text`, ?) FROM `documents`", sql)
	assert.Equal(t, []any{"[0-9]+"}, params)
}

// -----------------------------------------------------------------------------
// NULL/Conditional Function tests
// -----------------------------------------------------------------------------

func TestIfNull(t *testing.T) {
	t.Run("IfNull function with string", func(t *testing.T) {
		expr := sqlc.IfNull(sqlc.Col("nickname"), "Anonymous")

		sql, params, err := sqlc.From("users").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IFNULL(`nickname`, ?) FROM `users`", sql)
		assert.Equal(t, []any{"Anonymous"}, params)
	})

	t.Run("IfNull function with column", func(t *testing.T) {
		expr := sqlc.IfNull(sqlc.Col("score"), sqlc.Col("default_score"))

		sql, params, err := sqlc.From("users").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IFNULL(`score`, `default_score`) FROM `users`", sql)
		assert.Empty(t, params)
	})

	t.Run("IfNull method with number", func(t *testing.T) {
		expr := sqlc.Col("count").IfNull(0)

		sql, params, err := sqlc.From("items").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IFNULL(`count`, ?) FROM `items`", sql)
		assert.Equal(t, []any{0}, params)
	})

	t.Run("IfNull in WHERE clause", func(t *testing.T) {
		sql, params, err := sqlc.From("users").
			Where(sqlc.IfNull(sqlc.Col("status"), "inactive").Eq("active")).
			ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT * FROM `users` WHERE IFNULL(`status`, ?) = ?", sql)
		assert.Equal(t, []any{"inactive", "active"}, params)
	})
}

func TestNullIf(t *testing.T) {
	t.Run("NullIf function", func(t *testing.T) {
		expr := sqlc.NullIf(sqlc.Col("value"), 0)

		sql, params, err := sqlc.From("data").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT NULLIF(`value`, ?) FROM `data`", sql)
		assert.Equal(t, []any{0}, params)
	})

	t.Run("NullIf method with string", func(t *testing.T) {
		expr := sqlc.Col("status").NullIf("")

		sql, params, err := sqlc.From("items").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT NULLIF(`status`, ?) FROM `items`", sql)
		assert.Equal(t, []any{""}, params)
	})
}

func TestIf(t *testing.T) {
	t.Run("If with string values", func(t *testing.T) {
		expr := sqlc.If(sqlc.Col("status").Eq("active"), "Yes", "No")

		sql, params, err := sqlc.From("users").Columns(expr.As("is_active")).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IF(`status` = ?, ?, ?) AS is_active FROM `users`", sql)
		assert.Equal(t, []any{"active", "Yes", "No"}, params)
	})

	t.Run("If with column and literal", func(t *testing.T) {
		expr := sqlc.If(sqlc.Col("score").Gt(50), sqlc.Col("score"), sqlc.Lit(0))

		sql, params, err := sqlc.From("results").Columns(expr.As("adjusted_score")).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT IF(`score` > ?, `score`, 0) AS adjusted_score FROM `results`", sql)
		assert.Equal(t, []any{50}, params)
	})
}

// -----------------------------------------------------------------------------
// Utility Function tests
// -----------------------------------------------------------------------------

func TestGreatest(t *testing.T) {
	t.Run("Greatest with columns", func(t *testing.T) {
		expr := sqlc.Greatest(sqlc.Col("a"), sqlc.Col("b"), sqlc.Col("c"))

		sql, params, err := sqlc.From("data").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT GREATEST(`a`, `b`, `c`) FROM `data`", sql)
		assert.Empty(t, params)
	})

	t.Run("Greatest with column and value", func(t *testing.T) {
		expr := sqlc.Greatest(sqlc.Col("score"), 0)

		sql, params, err := sqlc.From("results").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT GREATEST(`score`, ?) FROM `results`", sql)
		assert.Equal(t, []any{0}, params)
	})
}

func TestLeast(t *testing.T) {
	t.Run("Least with columns", func(t *testing.T) {
		expr := sqlc.Least(sqlc.Col("a"), sqlc.Col("b"), sqlc.Col("c"))

		sql, params, err := sqlc.From("data").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT LEAST(`a`, `b`, `c`) FROM `data`", sql)
		assert.Empty(t, params)
	})

	t.Run("Least with column and value", func(t *testing.T) {
		expr := sqlc.Least(sqlc.Col("score"), 100)

		sql, params, err := sqlc.From("results").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT LEAST(`score`, ?) FROM `results`", sql)
		assert.Equal(t, []any{100}, params)
	})
}

func TestCast(t *testing.T) {
	t.Run("Cast function", func(t *testing.T) {
		expr := sqlc.Cast(sqlc.Col("price"), "DECIMAL(10,2)")

		sql, params, err := sqlc.From("products").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT CAST(`price` AS DECIMAL(10,2)) FROM `products`", sql)
		assert.Empty(t, params)
	})

	t.Run("Cast method", func(t *testing.T) {
		expr := sqlc.Col("created_at").Cast("DATE")

		sql, params, err := sqlc.From("events").Columns(expr).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT CAST(`created_at` AS DATE) FROM `events`", sql)
		assert.Empty(t, params)
	})
}

func TestSubstringIndex(t *testing.T) {
	t.Run("SubstringIndex function", func(t *testing.T) {
		expr := sqlc.SubstringIndex(sqlc.Col("email"), "@", 1)

		sql, params, err := sqlc.From("users").Columns(expr.As("username")).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT SUBSTRING_INDEX(`email`, ?, ?) AS username FROM `users`", sql)
		assert.Equal(t, []any{"@", 1}, params)
	})

	t.Run("SubstringIndex method", func(t *testing.T) {
		expr := sqlc.Col("path").SubstringIndex("/", -1)

		sql, params, err := sqlc.From("files").Columns(expr.As("filename")).ToSql()
		require.NoError(t, err)
		assert.Equal(t, "SELECT SUBSTRING_INDEX(`path`, ?, ?) AS filename FROM `files`", sql)
		assert.Equal(t, []any{"/", -1}, params)
	})
}

// -----------------------------------------------------------------------------
// Complex/Integration tests
// -----------------------------------------------------------------------------

func TestComplexQueryWithMultipleFunctions(t *testing.T) {
	t.Run("SELECT with JSON and Date functions", func(t *testing.T) {
		sql, params, err := sqlc.From("users").
			Columns(
				sqlc.Col("id"),
				sqlc.JsonUnquote(sqlc.JsonExtract(sqlc.Col("profile"), "$.name")).As("name"),
				sqlc.DateFormat(sqlc.Col("created_at"), "%Y-%m-%d").As("joined"),
				sqlc.IfNull(sqlc.Col("score"), 0).As("score"),
			).
			Where(sqlc.JsonExtract(sqlc.Col("profile"), "$.active").Eq(true)).
			Where(sqlc.Col("created_at").DateFormat("%Y").Eq("2026")).
			ToSql()

		require.NoError(t, err)
		assert.Equal(t,
			"SELECT `id`, JSON_UNQUOTE(JSON_EXTRACT(`profile`, ?)) AS name, DATE_FORMAT(`created_at`, ?) AS joined, IFNULL(`score`, ?) AS score FROM `users` WHERE JSON_EXTRACT(`profile`, ?) = ? AND DATE_FORMAT(`created_at`, ?) = ?",
			sql,
		)
		assert.Equal(t, []any{"$.name", "%Y-%m-%d", 0, "$.active", true, "%Y", "2026"}, params)
	})

	t.Run("WHERE with REGEXP and NULL handling", func(t *testing.T) {
		sql, params, err := sqlc.From("products").
			Where(sqlc.Col("name").RegexpLike("^[A-Z]").Eq(1)).
			Where(sqlc.IfNull(sqlc.Col("category"), "uncategorized").NotEq("hidden")).
			ToSql()

		require.NoError(t, err)
		assert.Equal(t,
			"SELECT * FROM `products` WHERE REGEXP_LIKE(`name`, ?) = ? AND IFNULL(`category`, ?) != ?",
			sql,
		)
		assert.Equal(t, []any{"^[A-Z]", 1, "uncategorized", "hidden"}, params)
	})

	t.Run("HAVING with aggregates and functions", func(t *testing.T) {
		sql, params, err := sqlc.From("orders").
			Columns(
				sqlc.Col("customer_id"),
				sqlc.Col("amount").Sum().As("total"),
			).
			GroupBy("customer_id").
			Having(sqlc.IfNull(sqlc.Col("amount").Sum(), 0).Gt(1000)).
			ToSql()

		require.NoError(t, err)
		assert.Equal(t,
			"SELECT `customer_id`, SUM(`amount`) AS total FROM `orders` GROUP BY `customer_id` HAVING IFNULL(SUM(`amount`), ?) > ?",
			sql,
		)
		assert.Equal(t, []any{0, 1000}, params)
	})
}
