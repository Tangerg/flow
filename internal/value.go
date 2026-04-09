package workflow

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cast"
)

type ValueType string

const (
	ValueTypeString ValueType = "string"
	ValueTypeNumber ValueType = "number"
	ValueTypeBool   ValueType = "bool"
	ValueTypeArray  ValueType = "array"
	ValueTypeObject ValueType = "object"
)

type Value interface {
	Type() ValueType
	Raw() any
	Clone() Value
	isValue()
}

func InferValue(val any) (Value, error) {
	switch v := val.(type) {
	case string:
		return NewStringValue(v), nil
	case bool:
		return NewBoolValue(v), nil
	case []any:
		items := make([]Value, 0, len(v))
		for i, item := range v {
			value, err := InferValue(item)
			if err != nil {
				return nil, fmt.Errorf("failed to infer array item at index %d: %w", i, err)
			}
			items = append(items, value)
		}
		return NewArrayValue(items...), nil
	case map[string]any:
		fields := make(map[string]Value, len(v))
		for key, item := range v {
			value, err := InferValue(item)
			if err != nil {
				return nil, fmt.Errorf("failed to infer object field '%s': %w", key, err)
			}
			fields[key] = value
		}
		return NewObjectValue(fields), nil
	default:
		num, err := cast.ToFloat64E(val)
		if err != nil {
			return nil, fmt.Errorf("unsupported type: %T", val)
		}
		return NewNumberValue(num), nil
	}
}

func getPathRecursive(current Value, subPath string) (Value, error) {
	switch v := current.(type) {
	case *ArrayValue:
		return v.GetPath(subPath)
	case *ObjectValue:
		return v.GetPath(subPath)
	default:
		return nil, fmt.Errorf("cannot access path '%s' on type %s", subPath, current.Type())
	}
}

type StringValue struct {
	val string
}

func NewStringValue(val string) *StringValue {
	return &StringValue{val: val}
}

func (v *StringValue) Type() ValueType { return ValueTypeString }
func (v *StringValue) Raw() any        { return v.val }
func (v *StringValue) String() string  { return v.val }
func (v *StringValue) Clone() Value    { return &StringValue{val: v.val} }
func (v *StringValue) isValue()        {}
func (v *StringValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.val)
}
func (v *StringValue) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &v.val)
}

type NumberValue struct {
	val float64
}

func NewNumberValue(val float64) *NumberValue {
	return &NumberValue{val: val}
}

func (v *NumberValue) Type() ValueType { return ValueTypeNumber }
func (v *NumberValue) Raw() any        { return v.val }
func (v *NumberValue) String() string {
	return fmt.Sprintf("%v", v.val)
}
func (v *NumberValue) Clone() Value { return &NumberValue{val: v.val} }
func (v *NumberValue) isValue()     {}
func (v *NumberValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.val)
}
func (v *NumberValue) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &v.val)
}

type BoolValue struct {
	val bool
}

func NewBoolValue(val bool) *BoolValue {
	return &BoolValue{val: val}
}

func (v *BoolValue) Type() ValueType { return ValueTypeBool }
func (v *BoolValue) Raw() any        { return v.val }
func (v *BoolValue) String() string {
	return fmt.Sprintf("%v", v.val)
}
func (v *BoolValue) Clone() Value { return &BoolValue{val: v.val} }
func (v *BoolValue) isValue()     {}
func (v *BoolValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.val)
}
func (v *BoolValue) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &v.val)
}

type ArrayValue struct {
	items []Value
}

func NewArrayValue(items ...Value) *ArrayValue {
	return &ArrayValue{items: items}
}

func (v *ArrayValue) Type() ValueType { return ValueTypeArray }

func (v *ArrayValue) Raw() any {
	values := make([]any, 0, len(v.items))
	for _, item := range v.items {
		values = append(values, item.Raw())
	}
	return values
}

func (v *ArrayValue) String() string {
	data, _ := v.MarshalJSON()
	return string(data)
}

func (v *ArrayValue) Clone() Value {
	clonedItems := make([]Value, len(v.items))
	for i, item := range v.items {
		clonedItems[i] = item.Clone()
	}
	return &ArrayValue{items: clonedItems}
}

func (v *ArrayValue) isValue() {}

func (v *ArrayValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Raw())
}

func (v *ArrayValue) UnmarshalJSON(data []byte) error {
	var values []any
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}

	v.items = make([]Value, 0, len(values))
	for _, item := range values {
		value, err := InferValue(item)
		if err != nil {
			return fmt.Errorf("failed to infer array item type: %w", err)
		}
		v.items = append(v.items, value)
	}
	return nil
}

func (v *ArrayValue) Get(index int) (Value, bool) {
	if index < 0 || index >= len(v.items) {
		return nil, false
	}
	return v.items[index], true
}

func (v *ArrayValue) Len() int {
	return len(v.items)
}

func (v *ArrayValue) GetPath(path string) (Value, error) {
	if path == "" {
		return v, nil
	}

	parts := strings.SplitN(path, ".", 2)

	index, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid array index: %s", parts[0])
	}

	item, ok := v.Get(index)
	if !ok {
		return nil, fmt.Errorf("array index out of range: %d", index)
	}

	if len(parts) == 1 {
		return item, nil
	}

	return getPathRecursive(item, parts[1])
}

type ObjectValue struct {
	fields map[string]Value
}

func NewObjectValue(fields map[string]Value) *ObjectValue {
	if fields == nil {
		fields = make(map[string]Value)
	}
	return &ObjectValue{fields: fields}
}

func (v *ObjectValue) Type() ValueType { return ValueTypeObject }

func (v *ObjectValue) Raw() any {
	result := make(map[string]any, len(v.fields))
	for key, field := range v.fields {
		result[key] = field.Raw()
	}
	return result
}

func (v *ObjectValue) String() string {
	data, _ := v.MarshalJSON()
	return string(data)
}

func (v *ObjectValue) Clone() Value {
	clonedFields := make(map[string]Value, len(v.fields))
	for key, field := range v.fields {
		clonedFields[key] = field.Clone()
	}
	return &ObjectValue{fields: clonedFields}
}

func (v *ObjectValue) isValue() {}

func (v *ObjectValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Raw())
}

func (v *ObjectValue) UnmarshalJSON(data []byte) error {
	var values map[string]any
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}

	v.fields = make(map[string]Value, len(values))
	for key, item := range values {
		value, err := InferValue(item)
		if err != nil {
			return fmt.Errorf("failed to infer object field '%s' type: %w", key, err)
		}
		v.fields[key] = value
	}
	return nil
}

func (v *ObjectValue) Get(key string) (Value, bool) {
	field, ok := v.fields[key]
	return field, ok
}

func (v *ObjectValue) Keys() []string {
	keys := make([]string, 0, len(v.fields))
	for k := range v.fields {
		keys = append(keys, k)
	}
	return keys
}

func (v *ObjectValue) GetPath(path string) (Value, error) {
	if path == "" {
		return v, nil
	}

	parts := strings.SplitN(path, ".", 2)

	field, ok := v.Get(parts[0])
	if !ok {
		return nil, fmt.Errorf("field not found: %s", parts[0])
	}

	if len(parts) == 1 {
		return field, nil
	}

	return getPathRecursive(field, parts[1])
}
