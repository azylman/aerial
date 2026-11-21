package widget

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

// widgetTestNow is the fixed render time for BuildStatusWidgetComponents cases.
var widgetTestNow = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

// widgetTestStarted is the build start ten minutes before widgetTestNow.
var widgetTestStarted = widgetTestNow.Add(-10 * time.Minute)

var buildStatusWidgetCases = []struct {
	name          string
	req           ericlient.StatusWidgetRequest
	expectBanner  bool
	expectSummary string
	expectTextSub string
	// wantComponents and wantRunes pin exact totals when non-zero; otherwise
	// the card must only stay within Discord's 40-component and 4000-rune limits.
	wantComponents int
	wantRunes      int
	wantFolds      []string
}{
	{
		name: "single pr widget with banner",
		req: ericlient.StatusWidgetRequest{
			ID:        "pr-aerial-100",
			Title:     "Add widget",
			Banner:    "Important note",
			Status:    "running",
			StartedAt: widgetTestStarted,
			Chunks: []ericlient.StatusWidgetChunk{
				{ID: "c1", Title: "Step 1", Status: "running", PRNumber: 100, Repo: "aerial"},
			},
		},
		expectBanner:  true,
		expectTextSub: "Important note",
	},
	{
		name: "multi-chunk completed build without banner",
		req: ericlient.StatusWidgetRequest{
			ID:        "build-12345",
			Title:     "Multi-chunk build",
			Status:    "completed",
			StartedAt: widgetTestStarted,
			Chunks: []ericlient.StatusWidgetChunk{
				{ID: "c1", Title: "Chunk 1", Status: "completed"},
				{ID: "c2", Title: "Chunk 2", Status: "completed"},
			},
		},
		expectBanner:  false,
		expectTextSub: "Multi-chunk build",
	},
	{
		name: "build without title and earlier chunk start and action mode",
		req: ericlient.StatusWidgetRequest{
			ID:     "build-empty-title",
			Status: "completed",
			Chunks: []ericlient.StatusWidgetChunk{
				{
					ID:          "c1",
					Title:       "Action chunk",
					Status:      "completed",
					Mode:        "action",
					StartedAt:   widgetTestStarted.Add(-5 * time.Minute),
					CompletedAt: widgetTestStarted.Add(10 * time.Minute),
					ActiveStep:  "Step 1\n\n  \nStep 2",
				},
				{
					ID:          "c2",
					Title:       "Standard chunk",
					Status:      "deployed",
					Repo:        "azylman/mirrormere/sub",
					StartedAt:   widgetTestStarted,
					CompletedAt: widgetTestStarted.Add(20 * time.Minute),
				},
			},
		},
		expectBanner:  false,
		expectTextSub: "`action`",
	},
	{
		name: "collapsing build with more than ten chunks",
		req: ericlient.StatusWidgetRequest{
			ID:        "build-many-chunks",
			Title:     "Mega Build",
			Status:    "running",
			StartedAt: widgetTestStarted,
			Chunks:    collapseFixture(15, 9, "merged"),
		},
		expectBanner:  false,
		expectSummary: "✅ ***7/15** earlier chunks completed and merged*",
	},
	{
		name:           "today's 18 chunk card renders unfolded",
		req:            ericlient.StatusWidgetRequest{ID: "build-18", Title: "Eighteen", Status: "running", Chunks: collapseFixture(18, 2, "merged")},
		wantComponents: 21,
	},
	{
		name:           "today's 22 chunk card renders unfolded",
		req:            ericlient.StatusWidgetRequest{ID: "build-22", Title: "Twenty Two", Status: "running", Chunks: collapseFixture(22, 19, "completed")},
		expectSummary:  "✅ ***17/22** earlier chunks completed and merged*",
		wantComponents: 9,
	},
	{
		name: "44 chunk build folds pending rows to fit 40 components",
		req: ericlient.StatusWidgetRequest{ID: "build-44", Title: "Sweep", Status: "running", Chunks: slices.Concat(
			stageFixture("done", 6, "completed", 100, "Done"),
			stageFixture("open", 3, "pr_opened", 200, "Open"),
			stageFixture("run", 1, "running", 0, "Run"),
			stageFixture("pend", 34, "pending", 0, "Pending"),
		)},
		expectSummary:  "✅ ***4/44** earlier chunks completed and merged*",
		wantComponents: 40,
		wantFolds:      []string{"⏳ *+15 more pending chunks*"},
	},
	{
		name:           "60 pending chunks with none done",
		req:            ericlient.StatusWidgetRequest{ID: "build-60", Title: "Pending", Status: "running", Chunks: stageFixture("pend", 60, "pending", 0, "Pending")},
		wantComponents: 40,
		wantFolds:      []string{"⏳ *+24 more pending chunks*"},
	},
	{
		name:           "45 running chunks fold active overflow",
		req:            ericlient.StatusWidgetRequest{ID: "build-45", Title: "Running", Status: "running", Chunks: stageFixture("run", 45, "running", 0, "Run")},
		wantComponents: 40,
		wantFolds:      []string{"🔨 *+9 more active chunks*"},
	},
	{
		name: "active overflow folds done and pending stages too",
		req: ericlient.StatusWidgetRequest{ID: "build-53", Title: "Mixed", Status: "running", Chunks: slices.Concat(
			stageFixture("done", 3, "merged", 0, "Done"),
			stageFixture("run", 45, "running", 0, "Run"),
			stageFixture("pend", 5, "pending", 0, "Pending"),
		)},
		expectSummary:  "✅ ***1/53** earlier chunks completed and merged*",
		wantComponents: 40,
		wantFolds:      []string{"🔨 *+12 more active chunks*", "✅ *+2 more completed chunks*", "⏳ *+5 more pending chunks*"},
	},
	{
		name:           "long pending titles fold to fit 4000 runes",
		req:            ericlient.StatusWidgetRequest{ID: "build-long", Title: "Long Titles", Status: "running", Chunks: stageFixture("pend", 30, "pending", 0, strings.Repeat("x", 200))},
		wantComponents: 22,
		wantRunes:      3959,
		wantFolds:      []string{"⏳ *+12 more pending chunks*"},
	},
	{
		name: "long done title folds before active rows",
		req: ericlient.StatusWidgetRequest{ID: "build-long-done", Title: "Long", Status: "running", Chunks: slices.Concat(
			stageFixture("done", 1, "completed", 0, strings.Repeat("d", 3000)),
			stageFixture("run", 1, "running", 0, strings.Repeat("r", 3000)),
		)},
		wantComponents: 5,
		wantFolds:      []string{"✅ *+1 more completed chunks*"},
	},
	{
		name:           "long active titles fold the last active row",
		req:            ericlient.StatusWidgetRequest{ID: "build-long-run", Title: "Long", Status: "running", Chunks: stageFixture("run", 2, "running", 0, strings.Repeat("r", 3000))},
		wantComponents: 5,
		wantFolds:      []string{"🔨 *+1 more active chunks*"},
	},
	{
		name:           "oversized header folds every row and stops",
		req:            ericlient.StatusWidgetRequest{ID: "build-huge", Title: strings.Repeat("h", 5000), Status: "running", Chunks: stageFixture("pend", 1, "pending", 0, "Pending")},
		wantComponents: 4,
		wantRunes:      5059,
		wantFolds:      []string{"⏳ *+1 more pending chunks*"},
	},
}

func TestBuildStatusWidgetComponentsTable(t *testing.T) {
	t.Parallel()
	now := widgetTestNow

	for _, tc := range buildStatusWidgetCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			comps, err := BuildStatusWidgetComponents(tc.req, now)
			require.NoError(t, err)
			require.Len(t, comps, 1)

			container, ok := comps[0].(*discordgo.Container)
			require.True(t, ok)
			assert.NotNil(t, container.AccentColor)
			assert.NotEmpty(t, container.Components)

			fullText := extractTestComponentsText(comps)
			if tc.expectBanner {
				assert.Contains(t, fullText, tc.req.Banner)
			} else if tc.req.Banner != "" {
				assert.NotContains(t, fullText, tc.req.Banner)
			}
			if tc.expectSummary != "" {
				assert.Contains(t, fullText, tc.expectSummary)
			}
			if tc.expectTextSub != "" {
				assert.Contains(t, fullText, tc.expectTextSub)
			}

			gotComponents, gotRunes := countTestComponents(comps)
			if tc.wantComponents != 0 {
				assert.Equal(t, tc.wantComponents, gotComponents)
			}
			assert.LessOrEqual(t, gotComponents, 40)
			if tc.wantRunes != 0 {
				assert.Equal(t, tc.wantRunes, gotRunes)
			} else {
				assert.LessOrEqual(t, gotRunes, 4000)
			}
			for _, fold := range tc.wantFolds {
				assert.Contains(t, fullText, fold)
			}
			if len(tc.wantFolds) == 0 {
				assert.NotContains(t, fullText, "*+")
			}
		})
	}
}

// countTestComponents returns the Discord component total (nested children and
// Section accessories included) and the total TextDisplay runes.
func countTestComponents(comps []discordgo.MessageComponent) (int, int) {
	count, runes := 0, 0
	for _, comp := range comps {
		count++
		switch c := comp.(type) {
		case *discordgo.TextDisplay:
			runes += utf8.RuneCountInString(c.Content)
		case *discordgo.Section:
			n, r := countTestComponents(c.Components)
			count += n
			runes += r
			if c.Accessory != nil {
				count++
			}
		case *discordgo.Container:
			n, r := countTestComponents(c.Components)
			count += n
			runes += r
		}
	}
	return count, runes
}

// stageFixture builds n chunks sharing status and title; PR numbers start at firstPR when non-zero.
func stageFixture(prefix string, n int, status string, firstPR int, title string) []ericlient.StatusWidgetChunk {
	chunks := make([]ericlient.StatusWidgetChunk, 0, n)
	for i := range n {
		pr := 0
		if firstPR > 0 {
			pr = firstPR + i
		}
		chunks = append(chunks, ericlient.StatusWidgetChunk{ID: fmt.Sprintf("%s-%02d", prefix, i), Title: title, Status: status, Repo: "aerial", PRNumber: pr})
	}
	return chunks
}
