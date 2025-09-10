package library

import (
	gob "encoding/gob"
	"fmt"
	"io"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"
)

var mu sync.Mutex
var errorCount int // For test capital
var checked map[reflect.Type]bool

func Register(value any) {
	checkValue(value)
	gob.Register(value)
}

func RegisterName(name string, value any) {
	checkValue(value)
	gob.RegisterName(name, value)
}

// Allows us to be able to send data across
// rpc calls and decode it easily even for non-capitalized field
type GEncoder struct {
	gob     *gob.Encoder
	version int
}

func NewEncoder(w io.Writer) *GEncoder {
	genco := &GEncoder{
		gob:     gob.NewEncoder(w),
		version: 1,
	}
	return genco
}

func (genco *GEncoder) Encode(e any) error {
	checkValue(e)
	return genco.gob.Encode(e)
}

func (genco *GEncoder) EncodeValue(value reflect.Value) error {
	checkValue(value)
	return genco.gob.Encode(value)
}

func checkValue(t any) {
	checkType(reflect.TypeOf(t))
}

func checkType(t reflect.Type) {
	k := t.Kind()

	// IDEA : check the value of the element parsed
	// use a memo cache to detect if the type  was seen prior
	mu.Lock()
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}

	if checked[t] {
		mu.Unlock()
		return
	}

	checked[t] = true
	mu.Unlock()
	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			rune, _ := utf8.DecodeRuneInString(field.Name)
			if unicode.IsLower(rune) {
				fmt.Printf(`gob error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n`,
					field.Name, t.Name())
				mu.Lock()
				errorCount += 1
				mu.Unlock()
			}
			checkType(field.Type)
		}
	case reflect.Map:
		checkType(t.Key())
		checkType(t.Elem())
		return
	case reflect.Array, reflect.Slice, reflect.Ptr:
		checkType(t)
		return
	default:
		return
	}

}

type GDecoder struct {
	gob     *gob.Decoder
	version int
}

func NewDecoder(w io.Reader) *GDecoder {
	gdeco := &GDecoder{
		gob:     gob.NewDecoder(w),
		version: 1,
	}
	return gdeco
}

func (gdeco *GDecoder) Decode(e any) error {
	checkValue(e)
	checkDefault(e)
	return gdeco.gob.Decode(e)
}

func checkDefault(value any) {
	if value == nil {
		return
	}
	checkDefaultHelper(reflect.ValueOf(value), 1, "")
}

func checkDefaultHelper(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}

	t := value.Type()
	k := value.Kind()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			vv := value.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefaultHelper(vv, depth+1, name1)
		}

	case reflect.Ptr:
		if value.IsNil() {
			return
		}
		checkDefaultHelper(value.Elem(), depth+1, name)
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.String:
		if !reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name()
				}
				// this warning typically arises if code re-uses the same RPC reply
				// variable for multiple RPC calls, or if code restores persisted
				// state into variable that already have non-default values.
				fmt.Printf("gob warning: Decoding into a non-default variable/field %v may not work\n",
					what)
			}
			errorCount += 1
			mu.Unlock()
		}
	}

}
