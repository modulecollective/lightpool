package wire

// The original token-based pipeline is intentionally independent of the new
// structural guard. Compare full acceptance and values, including typed rules.
import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

func referenceDecodeBody(b []byte) ([]Message, error) {
	if len(b) > MaxBody || !utf8.Valid(b) || !validStrings(b) || !referenceBoundedJSON(b) {
		return nil, errors.New("invalid body text")
	}
	var messages []Message
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&messages); err != nil {
		return nil, err
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return nil, errors.New("trailing body content")
	}
	if len(messages) == 0 || len(messages) > MaxBatch {
		return nil, errors.New("invalid batch count")
	}
	for _, m := range messages {
		if err := m.Validate(); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func referenceBoundedJSON(b []byte) bool {
	type level struct {
		array bool
		count int
	}
	stack := make([]level, 0, 8)
	d := json.NewDecoder(bytes.NewReader(b))
	for {
		token, err := d.Token()
		if err == io.EOF {
			return len(stack) == 0
		}
		if err != nil {
			return false
		}
		delim, isDelim := token.(json.Delim)
		closing := isDelim && (delim == ']' || delim == '}')
		if !closing && len(stack) > 0 && stack[len(stack)-1].array {
			l := &stack[len(stack)-1]
			l.count++
			limit := MaxNodes
			if len(stack) == 1 {
				limit = MaxBatch
			}
			if l.count > limit {
				return false
			}
		}
		if isDelim {
			switch delim {
			case '[', '{':
				if len(stack) >= 16 {
					return false
				}
				stack = append(stack, level{array: delim == '['})
			case ']', '}':
				if len(stack) == 0 {
					return false
				}
				stack = stack[:len(stack)-1]
			}
		}
	}
}
