package memory

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	"github.com/azylman/aerial/brain/pkg/db"
)

var (
	reDiscordContent = regexp.MustCompile(`(?s)(?:^|\n)- content:\s*(.+?)(?:\n- timestamp:|\n- mentions:|\n- attachments:|\n</USER_REQUEST>|\z)`)
	reUserRequest    = regexp.MustCompile(`(?s)<USER_REQUEST>(.*?)</USER_REQUEST>`)
)

// ExtractQueryText extracts the core user utterance from structured prompt envelopes (e.g. Discord funnel or XML blocks).
func ExtractQueryText(content string) string {
	clean := strings.TrimSpace(content)
	if clean == "" {
		return ""
	}

	// 1. Check for Discord funnel format: "- content: <user utterance>"
	if m := reDiscordContent.FindStringSubmatch(clean); len(m) > 1 {
		userText := strings.TrimSpace(m[1])
		if userText != "" {
			if len(userText) > 1000 {
				userText = userText[:1000]
			}
			return userText
		}
	}

	// 2. Check for XML wrapper <USER_REQUEST>...</USER_REQUEST>
	if m := reUserRequest.FindStringSubmatch(clean); len(m) > 1 {
		inner := strings.TrimSpace(m[1])
		if inner != "" {
			if len(inner) > 1000 {
				inner = inner[:1000]
			}
			return inner
		}
	}

	// 3. Raw text fallback
	if len(clean) > 1000 {
		clean = clean[:1000]
	}
	return clean
}


const (
	DefaultMinScoreThreshold = 0.45
	DefaultMaxFacts          = 10
)

// DotProduct calculates the dot product of two float32 slices.
// For L2-normalized vectors (like BGE embeddings), DotProduct equals Cosine Similarity.
func DotProduct(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

type ScoredFact struct {
	Fact  db.Fact
	Score float64
}

// RankFacts computes similarity scores for facts against queryVector, sorts descending, and returns Top N matching minScore.
func RankFacts(queryVector []float32, facts []db.FactWithEmbedding, minScore float64, topN int) []db.Fact {
	if len(queryVector) == 0 || len(facts) == 0 {
		return nil
	}
	if topN <= 0 {
		topN = DefaultMaxFacts
	}
	if minScore <= 0 {
		minScore = DefaultMinScoreThreshold
	}

	var scored []ScoredFact
	for _, f := range facts {
		if len(f.Embedding) != len(queryVector) {
			continue
		}
		sim := DotProduct(queryVector, f.Embedding)
		finalScore := sim * f.Fact.Importance
		if finalScore >= minScore {
			scored = append(scored, ScoredFact{
				Fact:  f.Fact,
				Score: finalScore,
			})
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	if len(scored) > topN {
		scored = scored[:topN]
	}

	result := make([]db.Fact, len(scored))
	for i, s := range scored {
		result[i] = s.Fact
	}
	return result
}

// RetrieveRelevantFacts fetches relevant stored facts for a given query string.
// If vector embedding generation fails or times out (1s timeout + 1 retry), logs warning and returns empty slice gracefully.
func RetrieveRelevantFacts(ctx context.Context, database *sql.DB, client *Client, queryText string, maxFacts int) ([]db.Fact, error) {
	queryText = strings.TrimSpace(queryText)
	if database == nil || client == nil || queryText == "" {
		return nil, nil
	}
	if len(queryText) > 1000 {
		queryText = queryText[:1000]
	}

	// Generate query embedding with BGE query prefix and 1 retry
	queryVector, err := client.GenerateEmbedding(ctx, queryText, true, 1)
	if err != nil {
		log.Printf("[Memory] Warning: Vector search embedding failed/timed out: %v. Proceeding without RAG context.", err)
		return nil, nil
	}

	allFacts, err := db.GetAllFactsWithEmbeddings(database)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch facts from db: %w", err)
	}

	ranked := RankFacts(queryVector, allFacts, DefaultMinScoreThreshold, maxFacts)
	return ranked, nil
}

// FormatMemoryContext formats retrieved facts into a markdown prompt block.
func FormatMemoryContext(facts []db.Fact) string {
	if len(facts) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("<retrieved_memory>\n")
	for _, f := range facts {
		cat := f.Category
		if cat == "" {
			cat = "general"
		}
		sb.WriteString(fmt.Sprintf("- [%s] %s\n", cat, f.FactText))
	}
	sb.WriteString("</retrieved_memory>\n")
	return sb.String()
}
