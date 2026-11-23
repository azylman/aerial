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
		{name: "nil input", input: nil, wantSlice: []string{}},
		{name: "empty array string", input: "{}", wantSlice: []string{}},
		{name: "null string", input: "NULL", wantSlice: []string{}},
		{name: "bytes slice valid array", input: []byte(`{"foo","bar"}`), wantSlice: []string{"foo", "bar"}},
		{name: "native string slice pass through", input: []string{"alpha", "beta"}, wantSlice: []string{"alpha", "beta"}},
		{name: "dest nil scan success", destNil: true, input: `{"only","test"}`},
		{name: "unsupported integer type", input: 12345, wantErr: true},
		{name: "malformed missing brackets", input: "raw_string_without_braces", wantErr: true},
		{name: "unclosed quote error", input: `{"unclosed}`, wantErr: true},
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
		{"empty slice", []string{}, "{}"},
		{"nil slice", nil, "{}"},
		{"single element", []string{"single"}, `{"single"}`},
		{"multiple elements", []string{"apple", "banana"}, `{"apple","banana"}`},
		{"escaping quotes and backslashes", []string{`say "hello"`, `path\to\file`}, `{"say \"hello\"","path\\to\\file"}`},
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
		{"already string slice", []string{"first", "second"}, []string{"first", "second"}, false},
		{"empty brackets string", "{}", []string{}, false},
		{"whitespace only string", "   ", []string{}, false},
		{"empty brackets bytes", []byte("{}"), []string{}, false},
		{"escaped quotes and backslashes in content", `{"say \"hi\"","path\\file"}`, []string{`say "hi"`, `path\file`}, false},
		{"single item without quotes", "{hello}", []string{"hello"}, false},
		{"trailing backslash escape error", `{"escaped\`, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePGTextArray(tc.input)
			if (err != nil) != tc.expectErr {
				t.Fatalf("parsePGTextArray(%v) err = %v, expectErr = %v", tc.input, err, tc.expectErr)
			}
			if !tc.expectErr && !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("parsePGTextArray(%v) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}
