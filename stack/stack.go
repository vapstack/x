package stack

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"runtime"
	"strconv"
	"strings"
)

func Trace(max, skip int) Frames {
	if max == 0 {
		return Frames{}
	}
	ptrs := make([]uintptr, max)
	count := runtime.Callers(skip+2, ptrs)
	if count == 0 {
		return Frames{}
	}
	stack := make(Frames, 0, count)
	frames := runtime.CallersFrames(ptrs[:count])
	for {
		f, more := frames.Next()
		if !strings.HasPrefix(f.Function, "runtime.") && f.Line > 0 {
			stack = append(stack, f)
		}
		if !more {
			break
		}
	}
	return stack
}

type Frames []runtime.Frame

func (s Frames) String() string {
	return strings.Join(s.ToStrings(), "\n")
}

func (s Frames) ToStrings() []string {
	out := make([]string, len(s))
	// "%v:%v - %v"
	for i, f := range s {
		var b strings.Builder
		b.Grow(len(f.File) + len(f.Function) + 32)
		b.WriteString(f.File)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(f.Line))
		b.WriteString(" - ")
		b.WriteString(shortFuncName(f.Function))
		out[i] = b.String()
	}
	return out
}

func (s Frames) MarshalJSON() ([]byte, error) {
	n := len(s)
	if n == 0 {
		return []byte("[]"), nil
	}
	total := 2

	for i, f := range s {
		if i > 0 {
			total++
		}
		total += 30
		total += len(f.File)
		total += len(f.Function)
		total += len(strconv.Itoa(f.Line))
	}

	b := make([]byte, 0, total)

	b = append(b, '[')
	for i, f := range s {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"file":`...)
		b = appendJSONString(b, f.File)
		b = append(b, `,"line":`...)
		b = strconv.AppendInt(b, int64(f.Line), 10)
		b = append(b, `,"func":`...)
		b = appendJSONString(b, shortFuncName(f.Function))
		b = append(b, '}')
	}
	b = append(b, ']')

	return b, nil
}

func appendJSONString(dst []byte, s string) []byte {
	b, _ := json.Marshal(s)
	return append(dst, b...)
}

func (s *Frames) UnmarshalJSON(data []byte) error {

	var in []frame
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	stack := make(Frames, len(in))
	for i, f := range in {
		stack[i] = runtime.Frame{
			File:     f.F,
			Line:     f.L,
			Function: f.N,
		}
	}
	*s = stack
	return nil
}

func (s Frames) GobEncode() ([]byte, error) {
	out := make([]frame, len(s))
	for i, f := range s {
		out[i] = frame{
			F: f.File,
			L: f.Line,
			N: shortFuncName(f.Function),
		}
	}
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(out)
	return buf.Bytes(), err
}

func (s *Frames) GobDecode(data []byte) error {
	var in []frame
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&in); err != nil {
		return err
	}
	stack := make(Frames, len(in))
	for i, f := range in {
		stack[i] = runtime.Frame{
			File:     f.F,
			Line:     f.L,
			Function: f.N,
		}
	}
	*s = stack
	return nil
}

func (s Frames) MarshalBinary() ([]byte, error) {
	return s.GobEncode()
}

func (s *Frames) UnmarshalBinary(data []byte) error {
	return s.GobDecode(data)
}

type frame struct {
	F string `json:"file"`
	L int    `json:"line"`
	N string `json:"func"`
}

func shortFuncName(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		s = s[i+1:]
	}
	// strings.ReplaceAll(s, "...", "")
	return s
}
