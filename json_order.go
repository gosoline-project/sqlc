package sqlc

import (
	"fmt"
	"regexp"
	"strings"
)

// JsonOrder describes a JSON ordering key. Column accepts simple or qualified
// identifiers, not SQL expressions. Direction accepts asc or desc, ignoring
// case; an omitted direction defaults to asc.
//
// Example JSON:
//
//	{"column":"users.created_at","direction":"desc"}
type JsonOrder struct {
	Column    string `json:"column"`
	Direction string `json:"direction,omitempty"`
}

var jsonOrderColumnPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

// ToExpression converts an ordering key to a column expression with direction.
// Invalid identifiers and directions return an error rather than raw SQL.
func (o JsonOrder) ToExpression() (*Expression, error) {
	if !jsonOrderColumnPattern.MatchString(o.Column) {
		return nil, fmt.Errorf("invalid column %q", o.Column)
	}

	switch strings.ToLower(o.Direction) {
	case "", "asc":
		return Col(o.Column).Asc(), nil
	case "desc":
		return Col(o.Column).Desc(), nil
	default:
		return nil, fmt.Errorf("direction must be asc or desc")
	}
}
