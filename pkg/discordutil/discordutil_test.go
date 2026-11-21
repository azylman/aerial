package discordutil

import "testing"

func TestIsSnowflake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "empty", input: "", want: false},
		{name: "digits", input: "123456789012345678", want: true},
		{name: "single digit", input: "0", want: true},
		{name: "leading space not trimmed", input: " 123", want: false},
		{name: "trailing newline not trimmed", input: "123\n", want: false},
		{name: "letters", input: "12a3", want: false},
		{name: "negative sign", input: "-123", want: false},
		{name: "non ascii digit", input: "١٢٣", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsSnowflake(tc.input); got != tc.want {
				t.Errorf("IsSnowflake(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
