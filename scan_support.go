package sqlc

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
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

func effectiveMapper(mapper *structMapper) *structMapper {
	if mapper != nil {
		return mapper
	}

	return defaultMapper
}

func closeOnReturn(err error, closeFn func() error) error {
	if closeErr := closeFn(); closeErr != nil && err == nil {
		return closeErr
	}

	return err
}

func getContext(ctxRows *sql.Rows, dest any, mapper *structMapper) error {
	row := scanRow{rows: ctxRows, mapper: effectiveMapper(mapper)}

	return row.scanAny(dest, false)
}

func selectContext(rows *sql.Rows, dest any, mapper *structMapper) (err error) {
	defer func() {
		err = closeOnReturn(err, rows.Close)
	}()

	return scanAll(rows, dest, false, mapper)
}

func scanAll(rows scanRows, dest any, structOnly bool, mapper *structMapper) error {
	config, err := scanAllConfig(rows, dest, structOnly)
	if err != nil {
		return err
	}

	if config.scannable {
		return scanAllScannable(rows, config.direct, config.base, config.isPtr)
	}

	return scanAllStructs(rows, dest, config, mapper)
}

type scanAllSetup struct {
	direct    reflect.Value
	base      reflect.Type
	isPtr     bool
	scannable bool
	columns   []string
}

func scanAllConfig(rows scanRows, dest any, structOnly bool) (setup scanAllSetup, err error) {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		err = errors.New("must pass a pointer, not a value, to StructScan destination")

		return
	}
	if value.IsNil() {
		err = errors.New("nil pointer passed to StructScan destination")

		return
	}

	setup.direct = reflect.Indirect(value)
	sliceType, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return setup, err
	}

	setup.isPtr = sliceType.Elem().Kind() == reflect.Ptr
	setup.base = derefType(sliceType.Elem())
	setup.scannable = isScannable(setup.base)
	if structOnly && setup.scannable {
		err = structOnlyError(setup.base)

		return
	}

	setup.columns, err = rows.Columns()
	if err != nil {
		return setup, err
	}

	if setup.scannable && len(setup.columns) > 1 {
		err = fmt.Errorf("non-struct dest type %s with >1 columns (%d)", setup.base.Kind(), len(setup.columns))
	}

	return
}

func scanAllStructs(rows scanRows, dest any, setup scanAllSetup, mapper *structMapper) error {
	fields := effectiveMapper(mapper).TraversalsByName(setup.base, setup.columns)
	if index, missingErr := missingFields(fields); missingErr != nil {
		return fmt.Errorf("missing destination name %s in %T", setup.columns[index], dest)
	}

	values := make([]any, len(setup.columns))
	for rows.Next() {
		itemPtr := reflect.New(setup.base)
		item := reflect.Indirect(itemPtr)

		if err := fieldsByTraversal(item, fields, values, true); err != nil {
			return err
		}

		if err := rows.Scan(values...); err != nil {
			return err
		}

		appendScanValue(setup.direct, itemPtr, item, setup.isPtr)
	}

	return rows.Err()
}

func scanAllScannable(rows scanRows, direct reflect.Value, base reflect.Type, isPtr bool) error {
	for rows.Next() {
		itemPtr := reflect.New(base)
		if err := rows.Scan(itemPtr.Interface()); err != nil {
			return err
		}

		appendScanValue(direct, itemPtr, reflect.Indirect(itemPtr), isPtr)
	}

	return rows.Err()
}

func appendScanValue(dest reflect.Value, itemPtr reflect.Value, item reflect.Value, isPtr bool) {
	if isPtr {
		dest.Set(reflect.Append(dest, itemPtr))

		return
	}

	dest.Set(reflect.Append(dest, item))
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

func (r *scanRow) Scan(dest ...any) (err error) {
	if r.err != nil {
		return r.err
	}

	defer func() {
		err = closeOnReturn(err, r.rows.Close)
	}()

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

	if scanErr := r.rows.Scan(dest...); scanErr != nil {
		return scanErr
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
	if err := fieldsByTraversal(value, fields, values, true); err != nil {
		return err
	}

	return r.Scan(values...)
}

func isScannable(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(scannerInterface) {
		return true
	}

	if t.Kind() != reflect.Struct {
		return true
	}

	return defaultMapper.TypeMap(t).fieldCount == 0
}

func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PointerTo(t).Implements(scannerInterface)
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
