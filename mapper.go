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
	traversals := make([][]int, 0, len(names))

	_ = m.TraversalsByNameFunc(t, names, func(_ int, traversal []int) error {
		if traversal == nil {
			traversals = append(traversals, []int{})
		} else {
			traversals = append(traversals, traversal)
		}

		return nil
	})

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
	fields := make([]*mappedField, 0)
	queue := []mappingQueueItem{{typ: derefType(t)}}

queueLoop:
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		for parent := item.field; parent != nil; parent = parent.parent {
			if item.field != nil && parent != item.field && item.field.fieldType == parent.fieldType {
				continue queueLoop
			}
		}

		if item.typ.Kind() != reflect.Struct {
			continue
		}

		for pos := 0; pos < item.typ.NumField(); pos++ {
			field := item.typ.Field(pos)
			tagValue, name := parseMappedName(field, tagName, mapFunc)
			if name == "-" {
				continue
			}

			if len(field.PkgPath) != 0 && !field.Anonymous {
				continue
			}

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

			fields = append(fields, mapped)

			fieldType := derefType(field.Type)
			if field.Anonymous {
				parentPath := item.parentPath
				if tagValue != "" {
					parentPath = mapped.path
				}

				mapped.embedded = true
				queue = append(queue, mappingQueueItem{
					typ:        fieldType,
					field:      mapped,
					parentPath: parentPath,
				})
				continue
			}

			if fieldType.Kind() == reflect.Struct {
				queue = append(queue, mappingQueueItem{
					typ:        fieldType,
					field:      mapped,
					parentPath: mapped.path,
				})
			}
		}
	}

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
