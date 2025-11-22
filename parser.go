package sqlc

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ParseWhere parses a SQL WHERE clause string into an Expression.
// It supports:
//   - Comparison operators: =, !=, >, <, >=, <=
//   - IN and NOT IN with value lists: "id IN (1, 2, 3)"
//   - LIKE for pattern matching: "name LIKE '%john%'"
//   - IS NULL and IS NOT NULL
//   - Logical operators: AND, OR, NOT (with proper precedence: NOT > AND > OR)
//   - Parentheses for grouping: "(age > 18 AND age < 65) OR status = 'active'"
//   - String literals: 'foo', "bar"
//   - Numeric literals: 1, 3.14, -5
//   - Column names: id, users.name, table.column
//   - Single-argument SQL functions (can be nested):
//   - String: UPPER, LOWER, TRIM, LTRIM, RTRIM, REVERSE, LENGTH, CHAR_LENGTH
//   - Numeric: ABS, CEIL, FLOOR, ROUND, SQRT, SIGN
//   - Aggregate: COUNT, SUM, AVG, MIN, MAX
//
// Function examples:
//
//	expr, err := ParseWhere("UPPER(name) = 'JOHN'")
//	// Returns: Col("name").Upper().Eq("JOHN")
//
//	expr, err := ParseWhere("LENGTH(email) > 10 AND TRIM(name) = 'John'")
//	// Returns: And(Col("email").Length().Gt(10), Col("name").Trim().Eq("John"))
//
//	expr, err := ParseWhere("UPPER(TRIM(name)) = 'JOHN'")
//	// Returns: Col("name").Trim().Upper().Eq("JOHN") (nested functions)
//
// Limitations:
//   - Only single-argument functions are supported (no ROUND(x, 2), CONCAT(a, b), etc.)
//   - Multi-argument functions require the programmatic Expression API
//   - Date/time functions (YEAR, DATE, etc.) are not yet supported
//
// Basic examples:
//
//	expr, err := ParseWhere("id = 1 AND name = 'foo'")
//	// Returns: And(Col("id").Eq(1), Col("name").Eq("foo"))
//
//	expr, err := ParseWhere("status IN ('active', 'pending') AND age >= 18")
//	// Returns: And(Col("status").In("active", "pending"), Col("age").Gte(18))
func ParseWhere(input string) (*Expression, error) {
	lexer := newLexer(input)
	tokens, err := lexer.tokenize()
	if err != nil {
		return nil, err
	}

	parser := &parser{
		tokens: tokens,
		pos:    0,
	}

	return parser.parseExpression()
}

// tokenType represents the type of a token
type tokenType int

const (
	tokenEOF tokenType = iota
	tokenIdentifier
	tokenString
	tokenNumber
	tokenOperator
	tokenLeftParen
	tokenRightParen
	tokenComma
	tokenAnd
	tokenOr
	tokenNot
	tokenIn
	tokenNotIn
	tokenLike
	tokenIs
	tokenNull
)

// token represents a lexical token
type token struct {
	typ   tokenType
	value string
}

// lexer tokenizes the input string
type lexer struct {
	input string
	pos   int
}

func newLexer(input string) *lexer {
	return &lexer{input: input, pos: 0}
}

func (l *lexer) tokenize() ([]token, error) {
	var tokens []token

	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}

		tok, err := l.nextToken()
		if err != nil {
			return nil, err
		}

		tokens = append(tokens, tok)
	}

	tokens = append(tokens, token{typ: tokenEOF})

	return tokens, nil
}

// nextToken reads and returns the next token from the input
func (l *lexer) nextToken() (token, error) {
	ch := l.input[l.pos]

	// Handle parentheses
	if ch == '(' {
		l.pos++

		return token{typ: tokenLeftParen, value: "("}, nil
	}
	if ch == ')' {
		l.pos++

		return token{typ: tokenRightParen, value: ")"}, nil
	}

	// Handle comma
	if ch == ',' {
		l.pos++

		return token{typ: tokenComma, value: ","}, nil
	}

	// Handle string literals
	if ch == '\'' || ch == '"' {
		str, err := l.readString()
		if err != nil {
			return token{}, err
		}

		return token{typ: tokenString, value: str}, nil
	}

	// Handle numbers
	if unicode.IsDigit(rune(ch)) || (ch == '-' && l.pos+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.pos+1]))) {
		num := l.readNumber()

		return token{typ: tokenNumber, value: num}, nil
	}

	// Handle operators
	if op := l.readOperator(); op != "" {
		return token{typ: tokenOperator, value: op}, nil
	}

	// Handle identifiers and keywords
	if unicode.IsLetter(rune(ch)) || ch == '_' || ch == '`' {
		ident := l.readIdentifier()

		return l.identifierToToken(ident), nil
	}

	return token{}, fmt.Errorf("unexpected character at position %d: %c", l.pos, ch)
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *lexer) readString() (string, error) {
	quote := l.input[l.pos]
	l.pos++ // skip opening quote

	start := l.pos
	for l.pos < len(l.input) {
		if l.input[l.pos] == quote {
			str := l.input[start:l.pos]
			l.pos++ // skip closing quote

			return str, nil
		}
		// Handle escaped quotes
		if l.input[l.pos] == '\\' && l.pos+1 < len(l.input) {
			l.pos += 2

			continue
		}
		l.pos++
	}

	return "", fmt.Errorf("unterminated string starting at position %d", start-1)
}

func (l *lexer) readNumber() string {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && (unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '.') {
		l.pos++
	}

	return l.input[start:l.pos]
}

func (l *lexer) readOperator() string {
	ch := l.input[l.pos]

	// Check for two-character operators
	if l.pos+1 < len(l.input) {
		twoChar := l.input[l.pos : l.pos+2]
		switch twoChar {
		case "!=", ">=", "<=", "<>":
			l.pos += 2
			if twoChar == "<>" {
				return "!="
			}

			return twoChar
		}
	}

	// Single character operators
	switch ch {
	case '=', '>', '<':
		l.pos++

		return string(ch)
	}

	return ""
}

func (l *lexer) readIdentifier() string {
	// Handle backtick-quoted identifiers
	if l.input[l.pos] == '`' {
		l.pos++ // skip opening backtick
		start := l.pos
		for l.pos < len(l.input) && l.input[l.pos] != '`' {
			l.pos++
		}
		ident := l.input[start:l.pos]
		if l.pos < len(l.input) {
			l.pos++ // skip closing backtick
		}

		return ident
	}

	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' || ch == '.' {
			l.pos++
		} else {
			break
		}
	}

	return l.input[start:l.pos]
}

func (l *lexer) identifierToToken(ident string) token {
	upper := strings.ToUpper(ident)
	switch upper {
	case "AND":
		return token{typ: tokenAnd, value: ident}
	case "OR":
		return token{typ: tokenOr, value: ident}
	case "NOT":
		return token{typ: tokenNot, value: ident}
	case "IN":
		return token{typ: tokenIn, value: ident}
	case "LIKE":
		return token{typ: tokenLike, value: ident}
	case "IS":
		return token{typ: tokenIs, value: ident}
	case "NULL":
		return token{typ: tokenNull, value: ident}
	default:
		return token{typ: tokenIdentifier, value: ident}
	}
}

// parser implements a recursive descent parser for SQL WHERE clauses
type parser struct {
	tokens []token
	pos    int
}

func (p *parser) current() token {
	if p.pos >= len(p.tokens) {
		return token{typ: tokenEOF}
	}

	return p.tokens[p.pos]
}

func (p *parser) advance() {
	if p.pos < len(p.tokens) {
		p.pos++
	}
}

// parseExpression is the entry point for parsing
// Precedence: OR (lowest) > AND > NOT > comparison (highest)
func (p *parser) parseExpression() (*Expression, error) {
	return p.parseOr()
}

// parseOr handles OR expressions (lowest precedence)
func (p *parser) parseOr() (*Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	var expressions []*Expression
	expressions = append(expressions, left)

	for p.current().typ == tokenOr {
		p.advance() // consume OR
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, right)
	}

	if len(expressions) == 1 {
		return expressions[0], nil
	}

	return Or(expressions...), nil
}

// parseAnd handles AND expressions
func (p *parser) parseAnd() (*Expression, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	var expressions []*Expression
	expressions = append(expressions, left)

	for p.current().typ == tokenAnd {
		p.advance() // consume AND
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, right)
	}

	if len(expressions) == 1 {
		return expressions[0], nil
	}

	return And(expressions...), nil
}

// parseNot handles NOT expressions
func (p *parser) parseNot() (*Expression, error) {
	if p.current().typ == tokenNot {
		p.advance()               // consume NOT
		expr, err := p.parseNot() // NOT is right-associative
		if err != nil {
			return nil, err
		}

		return Not(expr), nil
	}

	return p.parseComparison()
}

// parseComparison handles comparison operations
func (p *parser) parseComparison() (*Expression, error) {
	// Handle parentheses for grouped expressions
	if p.current().typ == tokenLeftParen {
		p.advance() // consume (
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if p.current().typ != tokenRightParen {
			return nil, fmt.Errorf("expected closing parenthesis, got %v", p.current())
		}
		p.advance() // consume )

		return expr, nil
	}

	// Parse left side (column or function call)
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	// Check for IS NULL / IS NOT NULL
	if p.current().typ == tokenIs {
		return p.handleIsNull(left)
	}

	// Check for IN / NOT IN
	if p.current().typ == tokenIn || p.current().typ == tokenNot {
		return p.handleInOperator(left)
	}

	// Check for LIKE
	if p.current().typ == tokenLike {
		return p.handleLike(left)
	}

	// Handle regular comparison operators
	return p.handleComparisonOperator(left)
}

// handleIsNull handles IS NULL and IS NOT NULL
func (p *parser) handleIsNull(left *Expression) (*Expression, error) {
	p.advance() // consume IS
	isNot := false
	if p.current().typ == tokenNot {
		isNot = true
		p.advance() // consume NOT
	}
	if p.current().typ != tokenNull {
		return nil, fmt.Errorf("expected NULL after IS [NOT], got %v", p.current())
	}
	p.advance() // consume NULL

	if isNot {
		return left.IsNotNull(), nil
	}

	return left.IsNull(), nil
}

// handleInOperator handles IN and NOT IN operators
func (p *parser) handleInOperator(left *Expression) (*Expression, error) {
	isNotIn := false
	if p.current().typ == tokenNot {
		isNotIn = true
		p.advance() // consume NOT
		if p.current().typ != tokenIn {
			return nil, fmt.Errorf("expected IN after NOT, got %v", p.current())
		}
	}
	p.advance() // consume IN

	if p.current().typ != tokenLeftParen {
		return nil, fmt.Errorf("expected ( after IN, got %v", p.current())
	}
	p.advance() // consume (

	values, err := p.parseValueList()
	if err != nil {
		return nil, err
	}

	if p.current().typ != tokenRightParen {
		return nil, fmt.Errorf("expected ) after value list, got %v", p.current())
	}
	p.advance() // consume )

	if isNotIn {
		return left.NotIn(values...), nil
	}

	return left.In(values...), nil
}

// handleLike handles LIKE operator
func (p *parser) handleLike(left *Expression) (*Expression, error) {
	p.advance() // consume LIKE
	value, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	strValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE requires string value, got %T", value)
	}

	return left.Like(strValue), nil
}

// handleComparisonOperator handles =, !=, >, <, >=, <=
func (p *parser) handleComparisonOperator(left *Expression) (*Expression, error) {
	// Expect comparison operator
	if p.current().typ != tokenOperator {
		return nil, fmt.Errorf("expected operator, got %v at position %d", p.current(), p.pos)
	}

	operator := p.current().value
	p.advance()

	// Parse value
	value, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	// Create appropriate expression based on operator
	switch operator {
	case "=":
		return left.Eq(value), nil
	case "!=":
		return left.NotEq(value), nil
	case ">":
		return left.Gt(value), nil
	case ">=":
		return left.Gte(value), nil
	case "<":
		return left.Lt(value), nil
	case "<=":
		return left.Lte(value), nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator)
	}
}

// parsePrimary parses a primary expression (column reference or function call)
func (p *parser) parsePrimary() (*Expression, error) {
	if p.current().typ != tokenIdentifier {
		return nil, fmt.Errorf("expected identifier, got %v at position %d", p.current(), p.pos)
	}

	ident := p.current().value
	p.advance()

	// Check if this is a function call
	if p.current().typ == tokenLeftParen {
		return p.parseFunctionCall(ident)
	}

	// Plain column reference
	return Col(ident), nil
}

// parseFunctionCall parses a function call with a single argument
func (p *parser) parseFunctionCall(funcName string) (*Expression, error) {
	p.advance() // consume (

	// Parse the argument (which can be a column or another function call)
	arg, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	if p.current().typ != tokenRightParen {
		return nil, fmt.Errorf("expected closing parenthesis for function %s, got %v", funcName, p.current())
	}
	p.advance() // consume )

	// Map function name to Expression method
	return p.applyFunction(funcName, arg)
}

// applyFunction applies a function to an expression
func (p *parser) applyFunction(funcName string, arg *Expression) (*Expression, error) {
	upper := strings.ToUpper(funcName)

	switch upper {
	// String functions
	case "UPPER":
		return arg.Upper(), nil
	case "LOWER":
		return arg.Lower(), nil
	case "TRIM":
		return arg.Trim(), nil
	case "LTRIM":
		return arg.Ltrim(), nil
	case "RTRIM":
		return arg.Rtrim(), nil
	case "REVERSE":
		return arg.Reverse(), nil
	case "LENGTH":
		return arg.Length(), nil
	case "CHAR_LENGTH":
		return arg.CharLength(), nil

	// Numeric functions
	case "ABS":
		return arg.Abs(), nil
	case "CEIL":
		return arg.Ceil(), nil
	case "FLOOR":
		return arg.Floor(), nil
	case "ROUND":
		return arg.Round(), nil
	case "SQRT":
		return arg.Sqrt(), nil
	case "SIGN":
		return arg.Sign(), nil

	// Aggregate functions
	case "COUNT":
		return arg.Count(), nil
	case "SUM":
		return arg.Sum(), nil
	case "AVG":
		return arg.Avg(), nil
	case "MIN":
		return arg.Min(), nil
	case "MAX":
		return arg.Max(), nil

	default:
		return nil, fmt.Errorf("unsupported function: %s", funcName)
	}
}

// parseValue parses a single value (string or number)
func (p *parser) parseValue() (any, error) {
	tok := p.current()

	switch tok.typ {
	case tokenString:
		p.advance()

		return tok.value, nil
	case tokenNumber:
		p.advance()

		return p.parseNumberValue(tok.value)
	case tokenNull:
		p.advance()

		return nil, nil
	default:
		return nil, fmt.Errorf("expected value, got %v at position %d", tok, p.pos)
	}
}

// parseValueList parses a comma-separated list of values
func (p *parser) parseValueList() ([]any, error) {
	var values []any

	for p.current().typ != tokenRightParen {
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, value)

		if p.current().typ == tokenComma {
			p.advance() // consume comma

			continue
		}

		break
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("expected at least one value in list")
	}

	return values, nil
}

// parseNumberValue converts a number string to int64 or float64
func (p *parser) parseNumberValue(s string) (any, error) {
	// Try to parse as integer first
	if !strings.Contains(s, ".") {
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return i, nil
		}
	}

	// Parse as float
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number: %s", s)
	}

	return f, nil
}
