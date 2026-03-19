package sqlc

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"unicode"

	"github.com/jmoiron/sqlx/reflectx"
)

type bindType int

const (
	bindUnknown bindType = iota
	bindQuestion
	bindDollar
	bindNamed
	bindAt
)

var valuesRegexp = regexp.MustCompile(`\)\s*(?i)VALUES\s*\(`)

func bindTypeForDriverName(driverName string) bindType {
	switch driverName {
	case "postgres", "pgx", "pq-timeouts", "cloudsqlpostgres", "ql", "nrpostgres", "cockroach":
		return bindDollar
	case "oci8", "ora", "goracle", "godror":
		return bindNamed
	case "sqlserver":
		return bindAt
	default:
		return bindQuestion
	}
}

func bindNamedMapper(driverName, query string, arg any, mapper *reflectx.Mapper) (bound string, args []any, err error) {
	if arg == nil {
		return "", nil, fmt.Errorf("can not bind nil argument")
	}

	t := reflect.TypeOf(arg)
	k := t.Kind()

	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		mapArg, ok := convertMapStringAny(arg)
		if !ok {
			return "", nil, fmt.Errorf("unsupported map type: %T", arg)
		}

		return bindMap(bindTypeForDriverName(driverName), query, mapArg)
	case k == reflect.Array || k == reflect.Slice:
		return bindArray(bindTypeForDriverName(driverName), query, arg, mapper)
	default:
		return bindStruct(bindTypeForDriverName(driverName), query, arg, mapper)
	}
}

func convertMapStringAny(v any) (map[string]any, bool) {
	var m map[string]any
	mType := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if t == nil || !t.ConvertibleTo(mType) {
		return nil, false
	}

	return reflect.ValueOf(v).Convert(mType).Interface().(map[string]any), true
}

func bindStruct(bindType bindType, query string, arg any, mapper *reflectx.Mapper) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", nil, err
	}

	args, err := bindAnyArgs(names, arg, mapper)
	if err != nil {
		return "", nil, err
	}

	return bound, args, nil
}

func bindMap(bindType bindType, query string, arg map[string]any) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", nil, err
	}

	args, err := bindMapArgs(names, arg)
	return bound, args, err
}

func bindArray(bindType bindType, query string, arg any, mapper *reflectx.Mapper) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindQuestion)
	if err != nil {
		return "", nil, err
	}

	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", nil, fmt.Errorf("length of array is 0: %#v", arg)
	}

	args := make([]any, 0, len(names)*arrayLen)
	for i := 0; i < arrayLen; i++ {
		elementArgs, err := bindAnyArgs(names, arrayValue.Index(i).Interface(), mapper)
		if err != nil {
			return "", nil, err
		}

		args = append(args, elementArgs...)
	}

	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}

	if bindType != bindQuestion {
		bound = rebindQuery(bindType, bound)
	}

	return bound, args, nil
}

func bindAnyArgs(names []string, arg any, mapper *reflectx.Mapper) ([]any, error) {
	if mapArg, ok := convertMapStringAny(arg); ok {
		return bindMapArgs(names, mapArg)
	}

	return bindArgs(names, arg, mapper)
}

func bindMapArgs(names []string, arg map[string]any) ([]any, error) {
	args := make([]any, 0, len(names))

	for _, name := range names {
		value, ok := arg[name]
		if !ok {
			return args, fmt.Errorf("could not find name %s in %#v", name, arg)
		}

		args = append(args, value)
	}

	return args, nil
}

func bindArgs(names []string, arg any, mapper *reflectx.Mapper) ([]any, error) {
	args := make([]any, 0, len(names))
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	err := effectiveMapper(mapper).TraversalsByNameFunc(v.Type(), names, func(i int, traversal []int) error {
		if len(traversal) == 0 {
			return fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}

		args = append(args, reflectx.FieldByIndexesReadOnly(v, traversal).Interface())
		return nil
	})

	return args, err
}

func compileNamedQuery(query []byte, bindType bindType) (string, []string, error) {
	names := make([]string, 0, 10)
	rebound := make([]byte, 0, len(query))

	inName := false
	last := len(query) - 1
	currentVar := 1
	name := make([]byte, 0, 10)

	for i, b := range query {
		switch {
		case b == ':':
			if inName && i > 0 && query[i-1] == ':' {
				rebound = append(rebound, ':')
				inName = false
				continue
			}

			if inName {
				return "", nil, fmt.Errorf("unexpected `:` while reading named param at %d", i)
			}

			inName = true
			name = []byte{}
		case inName && i > 0 && b == '=' && len(name) == 0:
			rebound = append(rebound, ':', '=')
			inName = false
		case inName && (unicode.IsOneOf(namedBindRunes, rune(b)) || b == '_' || b == '.') && i != last:
			name = append(name, b)
		case inName:
			inName = false
			if i == last && unicode.IsOneOf(namedBindRunes, rune(b)) {
				name = append(name, b)
			}

			names = append(names, string(name))
			switch bindType {
			case bindNamed:
				rebound = append(rebound, ':')
				rebound = append(rebound, name...)
			case bindDollar:
				rebound = append(rebound, '$')
				rebound = strconv.AppendInt(rebound, int64(currentVar), 10)
				currentVar++
			case bindAt:
				rebound = append(rebound, '@', 'p')
				rebound = strconv.AppendInt(rebound, int64(currentVar), 10)
				currentVar++
			default:
				rebound = append(rebound, '?')
			}

			if i != last {
				rebound = append(rebound, b)
			} else if !unicode.IsOneOf(namedBindRunes, rune(b)) {
				rebound = append(rebound, b)
			}
		default:
			rebound = append(rebound, b)
		}
	}

	return string(rebound), names, nil
}

func fixBound(bound string, loop int) string {
	loc := valuesRegexp.FindStringIndex(bound)
	if len(loc) < 2 {
		return bound
	}

	openingBracketIndex := loc[1] - 1
	index := findMatchingClosingBracketIndex(bound[openingBracketIndex:])
	if index == 0 {
		return bound
	}

	closingBracketIndex := openingBracketIndex + index + 1

	var buffer bytes.Buffer
	buffer.WriteString(bound[:closingBracketIndex])
	for i := 0; i < loop-1; i++ {
		buffer.WriteByte(',')
		buffer.WriteString(bound[openingBracketIndex:closingBracketIndex])
	}
	buffer.WriteString(bound[closingBracketIndex:])

	return buffer.String()
}

func findMatchingClosingBracketIndex(s string) int {
	count := 0
	for i, ch := range s {
		if ch == '(' {
			count++
		}

		if ch == ')' {
			count--
			if count == 0 {
				return i
			}
		}
	}

	return 0
}

func rebindQuery(bindType bindType, query string) string {
	switch bindType {
	case bindDollar, bindNamed, bindAt:
	default:
		return query
	}

	rebound := make([]byte, 0, len(query)+10)
	index := 0

	for {
		position := indexByte(query, '?')
		if position == -1 {
			break
		}

		rebound = append(rebound, query[:position]...)
		switch bindType {
		case bindDollar:
			rebound = append(rebound, '$')
		case bindNamed:
			rebound = append(rebound, ':', 'a', 'r', 'g')
		case bindAt:
			rebound = append(rebound, '@', 'p')
		}

		index++
		rebound = strconv.AppendInt(rebound, int64(index), 10)
		query = query[position+1:]
	}

	return string(append(rebound, query...))
}

var namedBindRunes = []*unicode.RangeTable{unicode.Letter, unicode.Digit}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}

	return -1
}
