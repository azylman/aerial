package discord

import (
	"testing"

	"github.com/bwmarrin/discordgo"
)

type extractComponentsTestCase struct {
	name       string
	components []discordgo.MessageComponent
	want       string
}

func extractSimpleComponentsCases() []extractComponentsTestCase {
	return []extractComponentsTestCase{
		{
			name:       "NilSlice",
			components: nil,
			want:       "",
		},
		{
			name:       "EmptySlice",
			components: []discordgo.MessageComponent{},
			want:       "",
		},
		{
			name: "NilElementsAndTypedNils",
			components: []discordgo.MessageComponent{
				nil,
				(*discordgo.TextDisplay)(nil),
				(*discordgo.Section)(nil),
				(*discordgo.Container)(nil),
				(*discordgo.ActionsRow)(nil),
				(*discordgo.Button)(nil),
			},
			want: "",
		},
		{
			name: "PointerTextDisplay",
			components: []discordgo.MessageComponent{
				&discordgo.TextDisplay{Content: "Hello world"},
			},
			want: "Hello world",
		},
		{
			name: "ValueTextDisplay",
			components: []discordgo.MessageComponent{
				discordgo.TextDisplay{Content: "Value world"},
			},
			want: "Value world",
		},
		{
			name: "TextDisplay_WhitespaceTrimming",
			components: []discordgo.MessageComponent{
				&discordgo.TextDisplay{Content: "   trimmed content   \n"},
			},
			want: "trimmed content",
		},
		{
			name: "PointerButton_LabelAndURL",
			components: []discordgo.MessageComponent{
				&discordgo.Button{Label: "Click Here", URL: "https://example.com"},
			},
			want: "[Click Here](https://example.com)",
		},
		{
			name: "ValueButton_LabelAndURL",
			components: []discordgo.MessageComponent{
				discordgo.Button{Label: "Value Button", URL: "https://example.com/val"},
			},
			want: "[Value Button](https://example.com/val)",
		},
		{
			name: "Button_LabelOnly",
			components: []discordgo.MessageComponent{
				&discordgo.Button{Label: "Only Label"},
			},
			want: "[Only Label]",
		},
		{
			name: "Button_URLOnly",
			components: []discordgo.MessageComponent{
				&discordgo.Button{URL: "https://example.com/only"},
			},
			want: "https://example.com/only",
		},
		{
			name: "Button_EmptyBoth",
			components: []discordgo.MessageComponent{
				&discordgo.Button{Label: "   ", URL: ""},
			},
			want: "",
		},
	}
}

func extractComplexComponentsCases() []extractComponentsTestCase {
	return []extractComponentsTestCase{
		{
			name: "PointerSection_WithButtonLabelAndURL",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "Section text"},
					},
					Accessory: &discordgo.Button{Label: "PR #1", URL: "https://example.com/pr/1"},
				},
			},
			want: "Section text [PR #1](https://example.com/pr/1)",
		},
		{
			name: "ValueSection_WithValueButton",
			components: []discordgo.MessageComponent{
				discordgo.Section{
					Components: []discordgo.MessageComponent{
						discordgo.TextDisplay{Content: "Value section"},
					},
					Accessory: discordgo.Button{Label: "PR #2", URL: "https://example.com/pr/2"},
				},
			},
			want: "Value section [PR #2](https://example.com/pr/2)",
		},
		{
			name: "Section_WithButtonLabelOnly",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "Section text"},
					},
					Accessory: &discordgo.Button{Label: "PR #3"},
				},
			},
			want: "Section text [PR #3]",
		},
		{
			name: "Section_WithButtonURLOnly",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "Section text"},
					},
					Accessory: &discordgo.Button{URL: "https://example.com/pr/4"},
				},
			},
			want: "Section text https://example.com/pr/4",
		},
		{
			name: "Section_NoAccessory",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "Plain section"},
					},
				},
			},
			want: "Plain section",
		},
		{
			name: "Section_EmptyChildrenWithButton",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Accessory: &discordgo.Button{Label: "Button Alone", URL: "https://example.com"},
				},
			},
			want: "[Button Alone](https://example.com)",
		},
		{
			name: "Section_EmptyChildrenWithButtonURLOnly",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Accessory: &discordgo.Button{URL: "https://example.com/solo"},
				},
			},
			want: "https://example.com/solo",
		},
		{
			name: "Section_TypedNilAccessory",
			components: []discordgo.MessageComponent{
				&discordgo.Section{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "Typed nil accessory"},
					},
					Accessory: (*discordgo.Button)(nil),
				},
			},
			want: "Typed nil accessory",
		},
		{
			name: "PointerContainer_Nested",
			components: []discordgo.MessageComponent{
				&discordgo.Container{
					Components: []discordgo.MessageComponent{
						&discordgo.TextDisplay{Content: "Title"},
						&discordgo.Section{
							Components: []discordgo.MessageComponent{
								&discordgo.TextDisplay{Content: "Chunk row"},
							},
							Accessory: &discordgo.Button{Label: "View PR", URL: "https://github.com/pr"},
						},
						&discordgo.TextDisplay{Content: "Footer"},
					},
				},
			},
			want: "Title\nChunk row [View PR](https://github.com/pr)\nFooter",
		},
		{
			name: "ValueContainer",
			components: []discordgo.MessageComponent{
				discordgo.Container{
					Components: []discordgo.MessageComponent{
						discordgo.TextDisplay{Content: "Inside value container"},
					},
				},
			},
			want: "Inside value container",
		},
		{
			name: "PointerActionsRow",
			components: []discordgo.MessageComponent{
				&discordgo.ActionsRow{
					Components: []discordgo.MessageComponent{
						&discordgo.Button{Label: "Row Button", URL: "https://example.com/row"},
					},
				},
			},
			want: "[Row Button](https://example.com/row)",
		},
		{
			name: "ValueActionsRow",
			components: []discordgo.MessageComponent{
				discordgo.ActionsRow{
					Components: []discordgo.MessageComponent{
						discordgo.Button{Label: "Row Button Val"},
					},
				},
			},
			want: "[Row Button Val]",
		},
		{
			name: "MultipleTopLevelBlocks",
			components: []discordgo.MessageComponent{
				&discordgo.TextDisplay{Content: "Block 1"},
				&discordgo.TextDisplay{Content: "Block 2"},
			},
			want: "Block 1\nBlock 2",
		},
	}
}

func extractMessageComponentsCases() []extractComponentsTestCase {
	cases := extractSimpleComponentsCases()
	return append(cases, extractComplexComponentsCases()...)
}

func TestExtractMessageComponentsText(t *testing.T) {
	t.Parallel()
	tests := extractMessageComponentsCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ExtractMessageComponentsText(tc.components)
			if got != tc.want {
				t.Errorf("ExtractMessageComponentsText() = %q, want %q", got, tc.want)
			}
		})
	}
}
