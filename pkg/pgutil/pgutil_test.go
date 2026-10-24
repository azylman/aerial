package pgutil

import (
	"reflect"
	"testing"
)

func TestPGStringSlice_Scan_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		destNil   bool
		input     any
		wantSlice []string
		wantErr   bool
	}{
		{
			name:      "nil input",
			destNil:   false,
			input:     nil,
			wantSlice: []string{},
			wantErr:   false,
		},
		{
			name:      "empty array string",
			destNil:   false,
			input:     "{}",
			wantSlice: []string{},
			wantErr:   false,
		},
		{
			name:      "null string",
			destNil:   false,
			input:     "NULL",
			wantSlice: []string{},
			wantErr:   false,
		},
		{
			name:      "bytes slice valid array",
			destNil:   false,
			input:     []byte(`{"foo","bar"}`),
			wantSlice: []string{"foo", "bar"},
			wantErr:   false,
		},
		{
			name:      "native string slice pass through",
			destNil:   false,
			input:     []string{"alpha", "beta"},
			wantSlice: []string{"alpha", "beta"},
			wantErr:   false,
		},
		{
			name:      "dest nil scan success",
			destNil:   true,
			input:     `{"only","test"}`,
			wantSlice: nil,
			wantErr:   false,
		},
		{
			name:      "unsupported integer type",
			destNil:   false,
			input:     12345,
			wantSlice: nil,
			wantErr:   true,
		},
		{
			name:      "malformed missing brackets",
			destNil:   false,
			input:     "raw_string_without_braces",
			wantSlice: nil,
			wantErr:   true,
		},
		{
			name:      "unclosed quote error",
			destNil:   false,
			input:     `{"unclosed}`,
			wantSlice: nil,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var target []string
			var scanner *PGStringSlice
			if tc.destNil {
				scanner = &PGStringSlice{Dest: nil}
			} else {
				scanner = &PGStringSlice{Dest: &target}
			}

			err := scanner.Scan(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Scan(%v) error = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr && !tc.destNil {
				if !reflect.DeepEqual(target, tc.wantSlice) {
					t.Errorf("Scan() got = %v, want = %v", target, tc.wantSlice)
				}
			}
		})
	}
}

func TestFormatPGTextArray_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "empty slice",
			input:    []string{},
			expected: "{}",
		},
		{
			name:     "nil slice",
			input:    nil,
			expected: "{}",
		},
		{
			name:     "single element",
			input:    []string{"single"},
			expected: `{"single"}`,
		},
		{
			name:     "multiple elements",
			input:    []string{"apple", "banana"},
			expected: `{"apple","banana"}`,
		},
		{
			name:     "escaping quotes and backslashes",
			input:    []string{`say "hello"`, `path\to\file`},
			expected: `{"say \"hello\"","path\\to\\file"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := FormatPGTextArray(tc.input)
			if got != tc.expected {
				t.Errorf("FormatPGTextArray(%v) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParsePGTextArray_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     any
		expected  []string
		expectErr bool
	}{
		{
			name:      "already string slice",
			input:     []string{"first", "second"},
			expected:  []string{"first", "second"},
			expectErr: false,
		},
		{
			name:      "empty brackets string",
			input:     "{}",
			expected:  []string{},
			expectErr: false,
		},
		{
			name:      "whitespace only string",
			input:     "   ",
			expected:  []string{},
			expectErr: false,
		},
		{
			name:      "empty brackets bytes",
			input:     []byte("{}"),
			expected:  []string{},
			expectErr: false,
		},
		{
			name:      "escaped quotes and backslashes in content",
			input:     `{"say \"hi\"","path\\file"}`,
			expected:  []string{`say "hi"`, `path\file`},
			expectErr: false,
		},
		{
			name:      "single item without quotes",
			input:     "{hello}",
			expected:  []string{"hello"},
			expectErr: false,
		},
		{
			name:      "trailing backslash escape error",
			input:     `{"escaped\`,
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParsePGTextArray(tc.input)
			if (err != nil) != tc.expectErr {
				t.Fatalf("ParsePGTextArray(%v) err = %v, expectErr = %v", tc.input, err, tc.expectErr)
			}
			if !tc.expectErr && !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("ParsePGTextArray(%v) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}
