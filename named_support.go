package sqlc

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"unicode"
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

func bindNamedMapper(driverName, query string, arg any, mapper *structMapper) (bound string, args []any, err error) {
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

func bindStruct(bindType bindType, query string, arg any, mapper *structMapper) (bound string, args []any, err error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", nil, err
	}

	args, err = bindAnyArgs(names, arg, mapper)
	if err != nil {
		return "", nil, err
	}

	return bound, args, nil
}

func bindMap(bindType bindType, query string, arg map[string]any) (bound string, args []any, err error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", nil, err
	}

	args, err = bindMapArgs(names, arg)

	return bound, args, err
}

func bindArray(bindType bindType, query string, arg any, mapper *structMapper) (bound string, args []any, err error) {
	bound, names, err := compileNamedQuery([]byte(query), bindQuestion)
	if err != nil {
		return "", nil, err
	}

	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", nil, fmt.Errorf("length of array is 0: %#v", arg)
	}

	args = make([]any, 0, len(names)*arrayLen)
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

func bindAnyArgs(names []string, arg any, mapper *structMapper) ([]any, error) {
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

func bindArgs(names []string, arg any, mapper *structMapper) ([]any, error) {
	args := make([]any, 0, len(names))
	v := reflect.ValueOf(arg)
	if !v.IsValid() {
		return nil, fmt.Errorf("can not bind nil argument")
	}

	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, fmt.Errorf("can not bind nil argument")
		}

		v = v.Elem()
	}

	err := effectiveMapper(mapper).TraversalsByNameFunc(v.Type(), names, func(i int, traversal []int) error {
		if len(traversal) == 0 {
			return fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}

		args = append(args, fieldByIndexesReadOnly(v, traversal).Interface())

		return nil
	})

	return args, err
}

func compileNamedQuery(query []byte, bindType bindType) (bound string, names []string, err error) {
	names = make([]string, 0, 10)
	rebound := make([]byte, 0, len(query))

	inName := false
	currentVar := 1
	name := make([]byte, 0, 10)

	for i, b := range query {
		if !inName {
			started := startNamedParam(query, i, b, &rebound)
			if started {
				inName = true
				name = name[:0]

				continue
			}

			rebound = append(rebound, b)

			continue
		}

		if escapedDoubleColon(query, i, b) {
			rebound = append(rebound, ':')
			inName = false

			continue
		}
		if b == ':' {
			return "", nil, fmt.Errorf("unexpected `:` while reading named param at %d", i)
		}

		keepReading, cancelled := consumeParamPrefix(i, b, name, &rebound)
		if cancelled {
			inName = false

			continue
		}
		if keepReading && shouldContinueName(query, i, b) {
			name = append(name, b)

			continue
		}

		finishNamedParam(query, i, b, bindType, name, &rebound, &names, &currentVar)
		inName = false
	}

	bound = string(rebound)

	return bound, names, nil
}

func startNamedParam(query []byte, index int, b byte, rebound *[]byte) bool {
	return b == ':'
}

func escapedDoubleColon(query []byte, index int, b byte) bool {
	return b == ':' && index > 0 && query[index-1] == ':'
}

func consumeParamPrefix(index int, b byte, name []byte, rebound *[]byte) (keepReading bool, cancelled bool) {
	if index > 0 && b == '=' && len(name) == 0 {
		*rebound = append(*rebound, ':', '=')

		return false, true
	}

	return isNamedParamRune(b), false
}

func shouldContinueName(query []byte, index int, b byte) bool {
	return index != len(query)-1 && isNamedParamRune(b)
}

func finishNamedParam(query []byte, index int, b byte, bindType bindType, name []byte, rebound *[]byte, names *[]string, currentVar *int) {
	if index == len(query)-1 && unicode.IsOneOf(namedBindRunes, rune(b)) {
		name = append(name, b)
	}

	*names = append(*names, string(name))
	*currentVar = appendBindVar(rebound, bindType, name, *currentVar)
	appendTrailingByte(query, index, b, rebound)
}

func appendBindVar(rebound *[]byte, bindType bindType, name []byte, currentVar int) int {
	switch bindType {
	case bindNamed:
		*rebound = append(*rebound, ':')
		*rebound = append(*rebound, name...)
	case bindDollar:
		*rebound = append(*rebound, '$')
		*rebound = strconv.AppendInt(*rebound, int64(currentVar), 10)
		currentVar++
	case bindAt:
		*rebound = append(*rebound, '@', 'p')
		*rebound = strconv.AppendInt(*rebound, int64(currentVar), 10)
		currentVar++
	default:
		*rebound = append(*rebound, '?')
	}

	return currentVar
}

func appendTrailingByte(query []byte, index int, b byte, rebound *[]byte) {
	if index != len(query)-1 {
		*rebound = append(*rebound, b)

		return
	}

	if !unicode.IsOneOf(namedBindRunes, rune(b)) {
		*rebound = append(*rebound, b)
	}
}

func isNamedParamRune(b byte) bool {
	return unicode.IsOneOf(namedBindRunes, rune(b)) || b == '_' || b == '.'
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
