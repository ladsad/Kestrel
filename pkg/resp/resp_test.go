package resp

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected []byte
	}{
		{"SimpleString", NewSimpleString("OK"), []byte("+OK\r\n")},
		{"Error", NewError("ERR unknown command 'foobar'"), []byte("-ERR unknown command 'foobar'\r\n")},
		{"Integer", NewInteger(1000), []byte(":1000\r\n")},
		{"BulkString", NewBulkString([]byte("hello")), []byte("$5\r\nhello\r\n")},
		{"NullBulkString", NewNullBulkString(), []byte("$-1\r\n")},
		{"EmptyBulkString", NewBulkString([]byte("")), []byte("$0\r\n\r\n")},
		{"Array", NewArray([]Value{NewBulkString([]byte("echo")), NewBulkString([]byte("hello world"))}), []byte("*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n")},
		{"NullArray", NewNullArray(), []byte("*-1\r\n")},
		{"EmptyArray", NewArray([]Value{}), []byte("*0\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.value.Marshal()
			if !bytes.Equal(actual, tt.expected) {
				t.Errorf("Marshal() = %q, want %q", actual, tt.expected)
			}
		})
	}
}

func TestReader(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected Value
		wantErr  bool
	}{
		{"SimpleString", []byte("+OK\r\n"), NewSimpleString("OK"), false},
		{"Error", []byte("-Error message\r\n"), NewError("Error message"), false},
		{"Integer", []byte(":1000\r\n"), NewInteger(1000), false},
		{"IntegerNegative", []byte(":-1000\r\n"), NewInteger(-1000), false},
		{"BulkString", []byte("$5\r\nhello\r\n"), NewBulkString([]byte("hello")), false},
		{"NullBulkString", []byte("$-1\r\n"), NewNullBulkString(), false},
		{"EmptyBulkString", []byte("$0\r\n\r\n"), NewBulkString([]byte("")), false},
		{"Array", []byte("*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n"), NewArray([]Value{NewBulkString([]byte("echo")), NewBulkString([]byte("hello world"))}), false},
		{"NullArray", []byte("*-1\r\n"), NewNullArray(), false},
		{"EmptyArray", []byte("*0\r\n"), NewArray([]Value{}), false},
		{"InvalidLineEnding", []byte("+OK\n"), Value{}, true},
		{"IncompleteBulkString", []byte("$5\r\nhel\r\n"), Value{}, true},
		{"InvalidType", []byte("!10\r\n"), Value{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(bytes.NewReader(tt.input))
			actual, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Read() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
