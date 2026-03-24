package sqlc

import (
	"reflect"
	"strings"
	"sync"
)

type structMapper struct {
	mu      sync.Mutex
	cache   map[reflect.Type]*structMap
	tagName string
	mapFunc func(string) string
}

type structMap struct {
	names      map[string][]int
	fieldCount int
}

type mappedField struct {
	fieldType reflect.Type
	index     []int
	path      string
	name      string
	embedded  bool
	parent    *mappedField
}

type mappingQueueItem struct {
	typ        reflect.Type
	field      *mappedField
	parentPath string
}

func newStructMapperFunc(tagName string, mapFunc func(string) string) *structMapper {
	return &structMapper{
		cache:   make(map[reflect.Type]*structMap),
		tagName: tagName,
		mapFunc: mapFunc,
	}
}

func (m *structMapper) TypeMap(t reflect.Type) *structMap {
	t = derefType(t)

	m.mu.Lock()
	defer m.mu.Unlock()

	if tm, ok := m.cache[t]; ok {
		return tm
	}

	tm := buildStructMap(t, m.tagName, m.mapFunc)
	m.cache[t] = tm

	return tm
}

func (m *structMapper) TraversalsByName(t reflect.Type, names []string) [][]int {
	t = derefType(t)
	tm := m.TypeMap(t)
	traversals := make([][]int, 0, len(names))

	for _, name := range names {
		traversal, ok := tm.names[name]
		if ok {
			traversals = append(traversals, traversal)
		} else {
			traversals = append(traversals, []int{})
		}
	}

	return traversals
}

func (m *structMapper) TraversalsByNameFunc(t reflect.Type, names []string, fn func(int, []int) error) error {
	t = derefType(t)
	tm := m.TypeMap(t)

	for i, name := range names {
		traversal, ok := tm.names[name]
		if !ok {
			if err := fn(i, nil); err != nil {
				return err
			}

			continue
		}

		if err := fn(i, traversal); err != nil {
			return err
		}
	}

	return nil
}

func derefType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}

	return t
}

func fieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	for _, index := range indexes {
		v = reflect.Indirect(v).Field(index)

		if v.Kind() == reflect.Ptr && v.IsNil() {
			v.Set(reflect.New(derefType(v.Type())))
		}

		if v.Kind() == reflect.Map && v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
	}

	return v
}

func fieldByIndexesReadOnly(v reflect.Value, indexes []int) reflect.Value {
	for _, index := range indexes {
		v = reflect.Indirect(v).Field(index)
	}

	return v
}

func buildStructMap(t reflect.Type, tagName string, mapFunc func(string) string) *structMap {
	fields := collectMappedFields(derefType(t), tagName, mapFunc)

	return newTypeMap(fields)
}

func collectMappedFields(root reflect.Type, tagName string, mapFunc func(string) string) []*mappedField {
	fields := make([]*mappedField, 0)
	queue := []mappingQueueItem{{typ: root}}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if shouldSkipMappingItem(item) {
			continue
		}

		mapped, nested := mapFieldsForType(item, tagName, mapFunc)
		fields = append(fields, mapped...)
		queue = append(queue, nested...)
	}

	return fields
}

func shouldSkipMappingItem(item mappingQueueItem) bool {
	if item.typ.Kind() != reflect.Struct {
		return true
	}

	return hasRecursiveMapping(item)
}

func hasRecursiveMapping(item mappingQueueItem) bool {
	if item.field == nil {
		return false
	}

	for parent := item.field.parent; parent != nil; parent = parent.parent {
		if item.field.fieldType == parent.fieldType {
			return true
		}
	}

	return false
}

func mapFieldsForType(item mappingQueueItem, tagName string, mapFunc func(string) string) (fields []*mappedField, nested []mappingQueueItem) {
	fields = make([]*mappedField, 0, item.typ.NumField())
	nested = make([]mappingQueueItem, 0, item.typ.NumField())

	for pos := 0; pos < item.typ.NumField(); pos++ {
		field := item.typ.Field(pos)
		tagValue, name := parseMappedName(field, tagName, mapFunc)
		if shouldSkipField(field, name) {
			continue
		}

		mapped := newMappedField(item, field, pos, name)
		fields = append(fields, mapped)
		nested = append(nested, nestedMappingItems(item.parentPath, field, tagValue, mapped)...)
	}

	return fields, nested
}

func shouldSkipField(field reflect.StructField, name string) bool {
	if name == "-" {
		return true
	}

	return field.PkgPath != "" && !field.Anonymous
}

func newMappedField(item mappingQueueItem, field reflect.StructField, pos int, name string) *mappedField {
	mapped := &mappedField{
		fieldType: field.Type,
		index:     appendIndex(nil, pos),
		name:      name,
		parent:    item.field,
	}
	if item.field != nil {
		mapped.index = appendIndex(item.field.index, pos)
	}

	if item.parentPath == "" {
		mapped.path = mapped.name
	} else {
		mapped.path = item.parentPath + "." + mapped.name
	}

	return mapped
}

func nestedMappingItems(parentPath string, field reflect.StructField, tagValue string, mapped *mappedField) []mappingQueueItem {
	fieldType := derefType(field.Type)
	if field.Anonymous {
		mapped.embedded = true
		if tagValue != "" {
			parentPath = mapped.path
		}

		return []mappingQueueItem{{
			typ:        fieldType,
			field:      mapped,
			parentPath: parentPath,
		}}
	}

	if fieldType.Kind() != reflect.Struct {
		return nil
	}

	return []mappingQueueItem{{
		typ:        fieldType,
		field:      mapped,
		parentPath: mapped.path,
	}}
}

func newTypeMap(fields []*mappedField) *structMap {
	paths := make(map[string]*mappedField)
	names := make(map[string][]int)

	for _, field := range fields {
		existing, ok := paths[field.path]
		if ok && !existing.embedded {
			continue
		}

		paths[field.path] = field
		if field.name != "" && !field.embedded {
			names[field.path] = field.index
		}
	}

	return &structMap{
		names:      names,
		fieldCount: len(fields),
	}
}

func appendIndex(indexes []int, index int) []int {
	out := make([]int, len(indexes)+1)
	copy(out, indexes)
	out[len(out)-1] = index

	return out
}

func parseMappedName(field reflect.StructField, tagName string, mapFunc func(string) string) (tagValue, name string) {
	name = field.Name
	if mapFunc != nil {
		name = mapFunc(name)
	}

	if tagName == "" {
		return "", name
	}

	if !strings.Contains(string(field.Tag), tagName+":") {
		return "", name
	}

	tagValue = field.Tag.Get(tagName)
	parts := strings.Split(tagValue, ",")
	name = parts[0]

	return tagValue, name
}
