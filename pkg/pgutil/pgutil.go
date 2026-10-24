package pgutil

import (
	"fmt"
	"strings"
)

// PGStringSlice scans a PostgreSQL text array into a Go string slice pointer.
type PGStringSlice struct {
	Dest *[]string
}

// Scan implements database/sql.Scanner for PGStringSlice.
func (s *PGStringSlice) Scan(src any) error {
	vals, err := ParsePGTextArray(src)
	if err != nil {
		return err
	}
	if s.Dest != nil {
		*s.Dest = vals
	}
	return nil
}

// FormatPGTextArray encodes a slice of strings into a PostgreSQL text array literal format (e.g. {"a","b"}).
func FormatPGTextArray(elems []string) string {
	if len(elems) == 0 {
		return "{}"
	}
	var sb strings.Builder
	sb.WriteByte('{')
	for i, s := range elems {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		for _, r := range s {
			if r == '\\' || r == '"' {
				sb.WriteByte('\\')
			}
			sb.WriteRune(r)
		}
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}

func extractArrayString(src any) (content string, empty bool, err error) {
	if src == nil {
		return "", true, nil
	}
	var str string
	switch v := src.(type) {
	case string:
		str = v
	case []byte:
		str = string(v)
	default:
		return "", false, fmt.Errorf("cannot scan type %T into pgStringSlice", src)
	}
	str = strings.TrimSpace(str)
	if str == "" || str == "{}" || str == "NULL" {
		return "", true, nil
	}
	if !strings.HasPrefix(str, "{") || !strings.HasSuffix(str, "}") {
		return "", false, fmt.Errorf("malformed postgres array: %q", str)
	}
	return str[1 : len(str)-1], false, nil
}

func parsePGArrayContent(content string) ([]string, error) {
	if len(content) == 0 {
		return []string{}, nil
	}
	var res []string
	var cur strings.Builder
	inQuotes, escaped, hasItem := false, false, false

	for i := 0; i < len(content); i++ {
		ch := content[i]
		if escaped {
			cur.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '"' {
			inQuotes = !inQuotes
			hasItem = true
			continue
		}
		if ch == ',' && !inQuotes {
			res = append(res, cur.String())
			cur.Reset()
			hasItem = false
			continue
		}
		cur.WriteByte(ch)
		hasItem = true
	}
	if inQuotes || escaped {
		return nil, fmt.Errorf("unclosed quote or escape in postgres array")
	}
	if hasItem || len(res) > 0 {
		res = append(res, cur.String())
	}
	return res, nil
}

// ParsePGTextArray parses a PostgreSQL text array representation (string, []byte, or []string)
// into a slice of strings.
func ParsePGTextArray(src any) ([]string, error) {
	if slice, ok := src.([]string); ok {
		return slice, nil
	}
	content, empty, err := extractArrayString(src)
	if err != nil || empty {
		return []string{}, err
	}
	return parsePGArrayContent(content)
}
