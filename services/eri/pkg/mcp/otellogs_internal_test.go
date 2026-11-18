package mcp

import (
	"encoding/json"
	"testing"
)

func TestOtlpAnyValue_AsString(t *testing.T) {
	t.Parallel()
	strVal := "hello"
	dblVal := 3.14
	boolVal := true

	tests := []struct {
		name string
		val  *otlpAnyValue
		want string
	}{
		{
			name: "nil returns empty",
			val:  nil,
			want: "",
		},
		{
			name: "stringValue returns string",
			val:  &otlpAnyValue{StringValue: &strVal},
			want: "hello",
		},
		{
			name: "intValue returns formatted int",
			val:  &otlpAnyValue{IntValue: 123},
			want: "123",
		},
		{
			name: "doubleValue returns formatted float",
			val:  &otlpAnyValue{DoubleValue: &dblVal},
			want: "3.14",
		},
		{
			name: "boolValue returns formatted bool",
			val:  &otlpAnyValue{BoolValue: &boolVal},
			want: "true",
		},
		{
			name: "empty struct returns empty",
			val:  &otlpAnyValue{},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.val.AsString()
			if got != tc.want {
				t.Errorf("AsString() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOtlpAnyValue_AsScalar(t *testing.T) {
	t.Parallel()
	strVal := "test"
	dblVal := 2.5
	boolVal := false

	tests := []struct {
		name     string
		val      *otlpAnyValue
		wantVal  any
		wantType string
	}{
		{
			name:     "nil returns nil",
			val:      nil,
			wantVal:  nil,
			wantType: "",
		},
		{
			name:     "stringValue",
			val:      &otlpAnyValue{StringValue: &strVal},
			wantVal:  "test",
			wantType: "string",
		},
		{
			name:     "intValue float64",
			val:      &otlpAnyValue{IntValue: float64(456)},
			wantVal:  int64(456),
			wantType: typeInt,
		},
		{
			name:     "intValue valid string",
			val:      &otlpAnyValue{IntValue: "789"},
			wantVal:  int64(789),
			wantType: typeInt,
		},
		{
			name:     "intValue invalid string",
			val:      &otlpAnyValue{IntValue: "not-int"},
			wantVal:  "not-int",
			wantType: typeInt,
		},
		{
			name:     "intValue json.Number",
			val:      &otlpAnyValue{IntValue: json.Number("101")},
			wantVal:  int64(101),
			wantType: typeInt,
		},
		{
			name:     "intValue other type",
			val:      &otlpAnyValue{IntValue: int64(42)},
			wantVal:  int64(42),
			wantType: typeInt,
		},
		{
			name:     "doubleValue",
			val:      &otlpAnyValue{DoubleValue: &dblVal},
			wantVal:  2.5,
			wantType: "double",
		},
		{
			name:     "boolValue",
			val:      &otlpAnyValue{BoolValue: &boolVal},
			wantVal:  false,
			wantType: "bool",
		},
		{
			name:     "empty struct",
			val:      &otlpAnyValue{},
			wantVal:  nil,
			wantType: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotVal, gotType := tc.val.AsScalar()
			if gotType != tc.wantType || gotVal != tc.wantVal {
				t.Errorf("AsScalar() = (%v, %q), want (%v, %q)", gotVal, gotType, tc.wantVal, tc.wantType)
			}
		})
	}
}

func TestOtlpAnyValue_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		data        string
		wantString  string
		wantHasInt  bool
		wantErrBool bool
	}{
		{
			name: "empty bytes returns nil",
			data: "",
		},
		{
			name:       "quoted string",
			data:       `"direct-string"`,
			wantString: "direct-string",
		},
		{
			name:       "object with intValue",
			data:       `{"intValue":42}`,
			wantHasInt: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var val otlpAnyValue
			err := val.UnmarshalJSON([]byte(tc.data))
			if (err != nil) != tc.wantErrBool {
				t.Fatalf("UnmarshalJSON error = %v, wantErr = %v", err, tc.wantErrBool)
			}
			if tc.wantString != "" && (val.StringValue == nil || *val.StringValue != tc.wantString) {
				t.Errorf("StringValue = %v, want %q", val.StringValue, tc.wantString)
			}
			if tc.wantHasInt && val.IntValue == nil {
				t.Errorf("expected IntValue to be non-nil")
			}
		})
	}
}

func TestInferEngine(t *testing.T) {
	t.Parallel()
	strClaude := "claude"
	strCodex := "codex"

	tests := []struct {
		name         string
		recordAttrs  []otlpKeyValue
		resAttrs     []otlpKeyValue
		scopeName    string
		rawEventName string
		want         string
	}{
		{
			name: "record attribute engine",
			recordAttrs: []otlpKeyValue{
				{Key: "engine", Value: &otlpAnyValue{StringValue: &strClaude}},
			},
			want: "claude",
		},
		{
			name: "record attribute agent.engine",
			recordAttrs: []otlpKeyValue{
				{Key: "agent.engine", Value: &otlpAnyValue{StringValue: &strCodex}},
			},
			want: "codex",
		},
		{
			name: "resource attribute engine",
			resAttrs: []otlpKeyValue{
				{Key: "engine", Value: &otlpAnyValue{StringValue: &strClaude}},
			},
			want: "claude",
		},
		{
			name: "resource service.name codex",
			resAttrs: []otlpKeyValue{
				{Key: "service.name", Value: &otlpAnyValue{StringValue: &strCodex}},
			},
			want: "codex",
		},
		{
			name:      "scope name claude",
			scopeName: "claude-code-scope",
			want:      "claude",
		},
		{
			name:      "scope name codex",
			scopeName: "codex-scope",
			want:      "codex",
		},
		{
			name:         "event name claude_code prefix",
			rawEventName: "claude_code.command",
			want:         "claude",
		},
		{
			name:         "event name codex prefix",
			rawEventName: "codex.action",
			want:         "codex",
		},
		{
			name: "no match returns empty",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &otlpLogRecord{Attributes: tc.recordAttrs}
			got := inferEngine(rec, tc.resAttrs, tc.scopeName, tc.rawEventName)
			if got != tc.want {
				t.Errorf("inferEngine() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractRecordMessage(t *testing.T) {
	t.Parallel()
	bodyStr := "custom body message"

	tests := []struct {
		name         string
		body         *otlpAnyValue
		rawEventName string
		want         string
	}{
		{
			name: "body string value takes precedence",
			body: &otlpAnyValue{StringValue: &bodyStr},
			want: "custom body message",
		},
		{
			name:         "nil body with rawEventName",
			body:         nil,
			rawEventName: "api_call",
			want:         "api_call",
		},
		{
			name:         "nil body with empty rawEventName",
			body:         nil,
			rawEventName: "",
			want:         "agent log",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &otlpLogRecord{Body: tc.body}
			got := extractRecordMessage(rec, tc.rawEventName)
			if got != tc.want {
				t.Errorf("extractRecordMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}
