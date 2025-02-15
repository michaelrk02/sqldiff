package internal

import (
	"fmt"
	"reflect"
	"strconv"
)

type Value struct {
	Data interface{}
}

func NewValue(data interface{}) Value {
	return Value{Data: data}
}

func (v Value) IsBefore(other Value) bool {
	switch x := v.Data.(type) {
	case int:
		return x < other.Data.(int)
	case float64:
		return x < other.Data.(float64)
	case []uint8:
		return string(x) < string(other.Data.([]uint8))
	}
	panic(fmt.Sprintf("invalid data type: %s", reflect.TypeOf(v.Data)))
}

func (v Value) Equals(other Value) bool {
	if v.Data == nil && other.Data == nil {
		return true
	}
	if v.Data == nil && other.Data != nil ||
		v.Data != nil && other.Data == nil {
		return false
	}

	switch x := v.Data.(type) {
	case int64:
		return other.EqualsInt(x)
	case *int64:
		return other.EqualsIntPtr(x)
	case float64:
		return other.EqualsFloat(x)
	case *float64:
		return other.EqualsFloatPtr(x)
	case []uint8:
		return other.EqualsString(x)
	case *[]uint8:
		return other.EqualsStringPtr(x)
	}

	panic(fmt.Sprintf("invalid data type: %s", reflect.TypeOf(v.Data)))
}

func (v Value) String() string {
	if v.Data == nil {
		return "NULL"
	}

	switch x := v.Data.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case *int64:
		return strconv.FormatInt(*x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case *float64:
		return strconv.FormatFloat(*x, 'g', -1, 64)
	case []uint8:
		return Escape(string(x))
	case *[]uint8:
		return Escape(string(*x))
	}

	panic(fmt.Sprintf("invalid type: %s", reflect.TypeOf(v.Data)))
}

func (v Value) EqualsInt(y int64) bool {
	if x, ok := v.Data.(int64); ok {
		return x == y
	}
	return false
}

func (v Value) EqualsIntPtr(y *int64) bool {
	if x, ok := v.Data.(*int64); ok {
		if x == nil && y == nil {
			return true
		}
		if x != nil && y != nil {
			return *x == *y
		}
	}
	return false
}

func (v Value) EqualsFloat(y float64) bool {
	if x, ok := v.Data.(float64); ok {
		return x == y
	}
	return false
}

func (v Value) EqualsFloatPtr(y *float64) bool {
	if x, ok := v.Data.(*float64); ok {
		if x == nil && y == nil {
			return true
		}
		if x != nil && y != nil {
			return *x == *y
		}
	}
	return false
}

func (v Value) EqualsString(y []uint8) bool {
	if x, ok := v.Data.([]uint8); ok {
		return string(x) == string(y)
	}
	return false
}

func (v Value) EqualsStringPtr(y *[]uint8) bool {
	if x, ok := v.Data.(*[]uint8); ok {
		if x == nil && y == nil {
			return true
		}
		if x != nil && y != nil {
			return string(*x) == string(*y)
		}
	}
	return false
}
