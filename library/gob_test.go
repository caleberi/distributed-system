package library

import (
	"bytes"
	"testing"
)

type T1 struct {
	T1int0    int
	T1int1    int
	T1string0 string
	T1string1 string
}

type T2 struct {
	T2slice []T1
	T2map   map[int]*T1
	T2t3    any
}

type T3 struct {
	T3int999 int
}

func TestGOB(t *testing.T) {
	ec := errorCount // count stub (if we violate relection criteria)

	buffer := &bytes.Buffer{} //  a place to store all encoded information

	{
		x1 := 10
		x2 := 20
		t1 := T1{}
		t2 := T2{}

		t1.T1int0 = x1
		t1.T1int1 = x2
		t1.T1string0 = "testing@T1"
		t1.T1string1 = "6.5840"

		t2.T2slice = []T1{t1, {}}
		Register(map[int]*T1{}) // register type for encoding
		t2.T2map = map[int]*T1{
			23: &t1,
			99: {
				T1string1: "y",
			},
		}
		Register(map[string]int{})
		t2.T2t3 = map[string]int{
			"34": 23,
			"21": 34,
		}

		encoder := NewEncoder(buffer)

		for _, err := range []error{
			encoder.Encode(x1),
			encoder.Encode(x2),
			encoder.Encode(t1),
			encoder.Encode(t2)} {
			if err != nil {
				t.Fatalf("Encoding failed :  %v", err)
			}
		}
	}

	data := buffer.Bytes()

	{
		var x1 int
		var x2 int
		var t1 T1
		var t2 T2

		r := bytes.NewBuffer(data) //  temp for decoding before comparsion
		decoder := NewDecoder(r)
		// decode  the information
		for _, err := range []error{
			decoder.Decode(&x1),
			decoder.Decode(&x2),
			decoder.Decode(&t1),
			decoder.Decode(&t2)} {
			if err != nil {
				t.Fatalf("Decoding failed :  %v", err)
			}
		}

		testcases := []struct {
			name     string
			input    any
			expected any
		}{
			{
				name:     "x1",
				input:    x1,
				expected: 10,
			},
			{
				name:     "x2",
				input:    x2,
				expected: 20,
			},
			{
				name:     "t1.T1int0",
				input:    t1.T1int0,
				expected: 10,
			},
			{
				name:     "t1.T1int1",
				input:    t1.T1int1,
				expected: 20,
			},
			{
				name:     "t1.T1int0(1)",
				input:    "",
				expected: "",
			},
			{
				name:     "t1.T1int1(2)",
				input:    t1.T1string1,
				expected: "6.5840",
			},
			{
				name:     "t1.T1int1(3)",
				input:    len(t2.T2slice),
				expected: 2,
			},
			{
				name:     "t1.T1int1(4)",
				input:    t2.T2slice[1].T1int1,
				expected: 0,
			},
			{
				name:     "t2.T2map",
				input:    len(t2.T2map),
				expected: 2,
			},
			{
				name:     "t2.T2map[99].T1string1",
				input:    t2.T2map[99].T1string1,
				expected: "y",
			},
		}

		for _, suite := range testcases {
			t.Run(suite.name, func(t *testing.T) {
				if suite.expected != suite.input {
					t.Fatalf("[%s]: got %v expected %v\n", suite.name, suite.expected, suite.input)
				}
			})
		}
	}

	if errorCount != ec {
		t.Fatalf("there were errors, but should not have been")
	}
}

func TestDefualtValue(t *testing.T) {
	e0 := errorCount

	type Point struct {
		X, Y int
	}

	w := &bytes.Buffer{}

	encoder := NewEncoder(w)

	dd := Point{} //send empty data
	encoder.Encode(dd)

	data := w.Bytes()

	// and receive it into memory that already
	// holds non-default values.

	reply := Point{
		X: 99,
	}

	r := bytes.NewBuffer(data)
	decoder2 := NewDecoder(r)

	decoder2.Decode(&reply)

	expectedErrCount := e0 + 1
	if errorCount != expectedErrCount {
		t.Fatalf("failed to warn about decoding into a non-default value")
	}

}

func TestDecodingInArray(t *testing.T) {
	type User struct {
		Address string
		Name    string
		age     uint
	}

	w := bytes.NewBuffer([]byte{})

	encoder := NewEncoder(w)

	users := []User{
		{
			Address: "23,abc street",
			Name:    "test12",
			age:     20,
		},
		{
			Address: "234,taiwo str",
			Name:    "test10",
		},
	}

	encoder.Encode(users)

	decoder := NewDecoder(w)

	destination := []User{}

	decoder.Decode(destination)

	for i, user := range destination {
		if !(user.age == 0 || user.Address == users[i].Address || user.Name == users[i].Name) {
			t.Fatalf("error: decoding is not correct")
		}
	}

}
