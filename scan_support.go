package sqlc

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	defaultMapper    = newStructMapperFunc(dbStructTag, strings.ToLower)
)

type scanRows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type mapperCache struct {
	mu      sync.Mutex
	mapper  *structMapper
	tagName string
}

func (c *mapperCache) get(tagName string) *structMapper {
	c.mu.Lock()
	defer c.mu.Unlock()

	if tagName == "" {
		return defaultMapper
	}

	if c.mapper == nil || c.tagName != tagName {
		c.mapper = newStructMapperFunc(tagName, strings.ToLower)
		c.tagName = tagName
	}

	return c.mapper
}

func effectiveMapper(mapper *structMapper) *structMapper {
	if mapper != nil {
		return mapper
	}

	return defaultMapper
}

func structScan(rows scanRows, dest any, mapper *structMapper) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	value = value.Elem()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fields := effectiveMapper(mapper).TraversalsByName(value.Type(), columns)
	if index, missingErr := missingFields(fields); missingErr != nil {
		return fmt.Errorf("missing destination name %s in %T", columns[index], dest)
	}

	values := make([]any, len(columns))
	if err = fieldsByTraversal(value, fields, values, true); err != nil {
		return err
	}

	if err = rows.Scan(values...); err != nil {
		return err
	}

	return rows.Err()
}

func getContext(ctxRows *sql.Rows, dest any, mapper *structMapper) error {
	row := scanRow{rows: ctxRows, mapper: effectiveMapper(mapper)}
	return row.scanAny(dest, false)
}

func selectContext(rows *sql.Rows, dest any, mapper *structMapper) error {
	defer rows.Close()
	return scanAll(rows, dest, false, mapper)
}

func scanAll(rows scanRows, dest any, structOnly bool, mapper *structMapper) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	direct := reflect.Indirect(value)
	sliceType, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}

	isPtr := sliceType.Elem().Kind() == reflect.Ptr
	base := derefType(sliceType.Elem())
	scannable := isScannable(base)
	if structOnly && scannable {
		return structOnlyError(base)
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if scannable && len(columns) > 1 {
		return fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns))
	}

	if !scannable {
		fields := effectiveMapper(mapper).TraversalsByName(base, columns)
		if index, missingErr := missingFields(fields); missingErr != nil {
			return fmt.Errorf("missing destination name %s in %T", columns[index], dest)
		}

		values := make([]any, len(columns))
		for rows.Next() {
			itemPtr := reflect.New(base)
			item := reflect.Indirect(itemPtr)

			if err = fieldsByTraversal(item, fields, values, true); err != nil {
				return err
			}

			if err = rows.Scan(values...); err != nil {
				return err
			}

			if isPtr {
				direct.Set(reflect.Append(direct, itemPtr))
			} else {
				direct.Set(reflect.Append(direct, item))
			}
		}

		return rows.Err()
	}

	for rows.Next() {
		itemPtr := reflect.New(base)
		if err = rows.Scan(itemPtr.Interface()); err != nil {
			return err
		}

		if isPtr {
			direct.Set(reflect.Append(direct, itemPtr))
		} else {
			direct.Set(reflect.Append(direct, reflect.Indirect(itemPtr)))
		}
	}

	return rows.Err()
}

type scanRow struct {
	rows   *sql.Rows
	err    error
	mapper *structMapper
}

func (r *scanRow) Columns() ([]string, error) {
	if r.err != nil {
		return []string{}, r.err
	}

	return r.rows.Columns()
}

func (r *scanRow) Err() error {
	return r.err
}

func (r *scanRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	defer r.rows.Close()
	for _, destination := range dest {
		if _, ok := destination.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}

		return sql.ErrNoRows
	}

	if err := r.rows.Scan(dest...); err != nil {
		return err
	}

	if err := r.rows.Close(); err != nil {
		return err
	}

	return nil
}

func (r *scanRow) scanAny(dest any, structOnly bool) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		r.err = sql.ErrNoRows
		return r.err
	}

	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	base := derefType(value.Type())
	scannable := isScannable(base)
	if structOnly && scannable {
		return structOnlyError(base)
	}

	columns, err := r.Columns()
	if err != nil {
		return err
	}

	if scannable && len(columns) > 1 {
		return fmt.Errorf("scannable dest type %s with >1 columns (%d) in result", base.Kind(), len(columns))
	}

	if scannable {
		return r.Scan(dest)
	}

	fields := effectiveMapper(r.mapper).TraversalsByName(value.Type(), columns)
	if index, missingErr := missingFields(fields); missingErr != nil {
		return fmt.Errorf("missing destination name %s in %T", columns[index], dest)
	}

	values := make([]any, len(columns))
	if err = fieldsByTraversal(value, fields, values, true); err != nil {
		return err
	}

	return r.Scan(values...)
}

func isScannable(t reflect.Type) bool {
	if reflect.PtrTo(t).Implements(scannerInterface) {
		return true
	}

	if t.Kind() != reflect.Struct {
		return true
	}

	return defaultMapper.TypeMap(t).fieldCount == 0
}

func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PtrTo(t).Implements(scannerInterface)
	if !isStruct {
		return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
	}
	if isScanner {
		return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements scanner", t.Name())
	}

	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = derefType(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}

	return t, nil
}

func fieldsByTraversal(v reflect.Value, traversals [][]int, values []any, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(any)
			continue
		}

		field := fieldByIndexes(v, traversal)
		if ptrs {
			values[i] = field.Addr().Interface()
		} else {
			values[i] = field.Interface()
		}
	}

	return nil
}

func missingFields(traversals [][]int) (int, error) {
	for i, traversal := range traversals {
		if len(traversal) == 0 {
			return i, errors.New("missing field")
		}
	}

	return 0, nil
}
