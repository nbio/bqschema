package bqschema

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"google.golang.org/api/bigquery/v2"
)

var (
	ErrArrayOfArray = errors.New("Array of Arrays not allowed")
	ErrNotStruct    = errors.New("Can not convert non structs")

	// Legacy errors below. Exported names kept for compatibility.

	ArrayOfArray = ErrArrayOfArray
	NotStruct    = ErrNotStruct
	UnknownType  = errors.New("Unknown type") // no longer used
)

// ToSchema converts the passed type to a BigQuery table schema.
func ToSchema(src interface{}) (*bigquery.TableSchema, error) {
	value := reflect.ValueOf(src)
	t := value.Type()

	schema := &bigquery.TableSchema{}

	if t.Kind() != reflect.Struct {
		return schema, ErrNotStruct
	}
	schema.Fields = make([]*bigquery.TableFieldSchema, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" { // unexported
			continue
		}

		v := pointerGuard(value.Field(i))

		name := sf.Name
		mode := "required"

		switch jsonTag := sf.Tag.Get("json"); jsonTag {
		case "":
		case "-":
			continue
		default:
			jt := strings.Split(jsonTag, ",")
			name = jt[0]
			if len(jt) > 1 && jt[1] == "omitempty" {
				mode = "nullable"
			}
		}

		kind := v.Kind()
		t, isSimple := simpleType(kind)

		tfs := &bigquery.TableFieldSchema{
			Mode: mode,
			Name: name,
			Type: t,
		}
		schema.Fields = append(schema.Fields, tfs)

		if isSimple {
			continue
		}

		switch kind {
		case reflect.Struct:
			mode, tfs.Mode = tfs.Mode, "nullable" // preserve previous value
			t, fields, err := structConversion(v.Interface())
			if err != nil {
				return schema, err
			}
			tfs.Type = t
			if t == "string" {
				tfs.Mode = mode
			}
			tfs.Fields = fields
		case reflect.Array, reflect.Slice:
			tfs.Mode = "repeated"
			subKind := pointerGuard(v.Type().Elem()).Kind()
			if t, isSimple := simpleType(subKind); isSimple {
				schema.Fields[i].Type = t
				continue
			}
			if subKind != reflect.Struct {
				return schema, ErrArrayOfArray
			}
			subStruct := reflect.Zero(pointerGuard(v.Type().Elem()).Type()).Interface()
			t, fields, err := structConversion(subStruct)
			if err != nil {
				return schema, err
			}
			schema.Fields[i].Type = t
			schema.Fields[i].Fields = fields
		default:
			return schema, &ErrInconvertibleType{sf.Type.String()}
		}
	}
	return schema, nil
}

// MustToSchema panics if conversion to a schema encounters an error.
func MustToSchema(src interface{}) *bigquery.TableSchema {
	schema, err := ToSchema(src)
	if err != nil {
		panic(err)
	}
	return schema
}

func simpleType(kind reflect.Kind) (string, bool) {
	switch kind {
	case reflect.Bool:
		return "boolean", true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer", true
	case reflect.Float32, reflect.Float64:
		return "float", true
	case reflect.String:
		return "string", true
	default:
		return "", false
	}
}

func structConversion(src interface{}) (string, []*bigquery.TableFieldSchema, error) {
	v := reflect.ValueOf(src)
	if v.Type().Name() == "Key" && strings.Contains(v.Type().PkgPath(), "appengine") {
		return "string", nil, nil
	} else if v.Type().ConvertibleTo(reflect.TypeOf(time.Time{})) {
		return "timestamp", nil, nil
	} else {
		schema, err := ToSchema(src)
		return "record", schema.Fields, err
	}
}

func pointerGuard(i interface{}) reflect.Value {
	v, ok := i.(reflect.Value)
	if !ok {
		if t, ok := i.(reflect.Type); ok {
			v = reflect.Indirect(reflect.New(t))
		}
	}

	if v.Kind() == reflect.Ptr {
		v = reflect.Indirect(reflect.New(v.Type().Elem()))
	}
	return v
}

// ErrInconvertibleType reports a type that cannot be converted to a BigQuery schema.
type ErrInconvertibleType struct {
	TypeName string
}

func (e *ErrInconvertibleType) Error() string {
	return fmt.Sprintf("inconvertible type: %s", e.TypeName)
}
