package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	TypeSimpleString = '+'
	TypeError        = '-'
	TypeInteger      = ':'
	TypeBulkString   = '$'
	TypeArray        = '*'
)

type Value struct {
	Type   byte
	Str    string
	Num    int64
	Bulk   []byte
	IsNull bool
	Array  []Value
}

func NewSimpleString(s string) Value {
	return Value{Type: TypeSimpleString, Str: s}
}

func NewError(err string) Value {
	return Value{Type: TypeError, Str: err}
}

func NewInteger(n int64) Value {
	return Value{Type: TypeInteger, Num: n}
}

func NewBulkString(b []byte) Value {
	return Value{Type: TypeBulkString, Bulk: b}
}

func NewNullBulkString() Value {
	return Value{Type: TypeBulkString, IsNull: true}
}

func NewArray(a []Value) Value {
	return Value{Type: TypeArray, Array: a}
}

func NewNullArray() Value {
	return Value{Type: TypeArray, IsNull: true}
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)
	return err
}

func (w *Writer) WriteRaw(b []byte) error {
	_, err := w.writer.Write(b)
	return err
}

func (v Value) Marshal() []byte {
	switch v.Type {
	case TypeSimpleString:
		return []byte(fmt.Sprintf("+%s\r\n", v.Str))
	case TypeError:
		return []byte(fmt.Sprintf("-%s\r\n", v.Str))
	case TypeInteger:
		return []byte(fmt.Sprintf(":%d\r\n", v.Num))
	case TypeBulkString:
		if v.IsNull {
			return []byte("$-1\r\n")
		}
		res := []byte(fmt.Sprintf("$%d\r\n", len(v.Bulk)))
		res = append(res, v.Bulk...)
		res = append(res, '\r', '\n')
		return res
	case TypeArray:
		if v.IsNull {
			return []byte("*-1\r\n")
		}
		res := []byte(fmt.Sprintf("*%d\r\n", len(v.Array)))
		for _, item := range v.Array {
			res = append(res, item.Marshal()...)
		}
		return res
	default:
		return []byte{} // Unknown type
	}
}

type Reader struct {
	reader *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(rd)}
}

func (r *Reader) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case TypeSimpleString:
		return r.readSimpleString()
	case TypeError:
		return r.readError()
	case TypeInteger:
		return r.readInteger()
	case TypeBulkString:
		return r.readBulkString()
	case TypeArray:
		return r.readArray()
	default:
		return Value{}, fmt.Errorf("unknown type: %q", _type)
	}
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("invalid line ending")
	}
	return line[:len(line)-2], nil
}

func (r *Reader) readSimpleString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return NewSimpleString(string(line)), nil
}

func (r *Reader) readError() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return NewError(string(line)), nil
}

func (r *Reader) readInteger() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, err
	}
	return NewInteger(n), nil
}

func (r *Reader) readBulkString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, err
	}
	if length == -1 {
		return NewNullBulkString(), nil
	}
	if length < 0 {
		return Value{}, errors.New("invalid bulk string length")
	}

	bulk := make([]byte, length)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return Value{}, err
	}

	// Read \r\n
	crlf := make([]byte, 2)
	_, err = io.ReadFull(r.reader, crlf)
	if err != nil {
		return Value{}, err
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return Value{}, errors.New("invalid bulk string ending")
	}

	return NewBulkString(bulk), nil
}

func (r *Reader) readArray() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	length, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, err
	}
	if length == -1 {
		return NewNullArray(), nil
	}
	if length < 0 {
		return Value{}, errors.New("invalid array length")
	}

	array := make([]Value, length)
	for i := int64(0); i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return Value{}, err
		}
		array[i] = val
	}

	return NewArray(array), nil
}
