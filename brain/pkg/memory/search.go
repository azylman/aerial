package memory

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
)

var (
	rePreviousSessionEnvelope = regexp.MustCompile(`(?s)<PREVIOUS_SESSION>.*?</PREVIOUS_SESSION>`)
)

func sanitizeQueryText(text string) string {
	cleaned := sanitizer.SanitizeMentions(text)
	cleaned = sanitizer.NormalizeWhitespace(cleaned)
	if len(cleaned) > 1000 {
		cleaned = cleaned[:1000]
	}
	return cleaned
}

// ExtractQueryText extracts the core user utterance from prompt envelopes or raw text for vector search.
func ExtractQueryText(content string) string {
	clean := strings.TrimSpace(content)
	if clean == "" {
		return ""
	}

	clean = rePreviousSessionEnvelope.ReplaceAllString(clean, "")
	body := db.ExtractMessageBody(clean)
	return sanitizeQueryText(body)
}

const (
	DefaultMinScoreThreshold = 0.20
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
// Accepts either a db.FactStore interface or a legacy *sql.DB pointer.
// If vector embedding generation fails or times out (1s timeout + 1 retry), logs warning and returns empty slice gracefully.
func RetrieveRelevantFacts(ctx context.Context, factStore db.FactStore, client *Client, queryText string, maxFacts int) ([]db.Fact, error) {
	queryText = strings.TrimSpace(queryText)
	if factStore == nil || client == nil || queryText == "" {
		return nil, nil
	}
	if len(queryText) > 1000 {
		queryText = queryText[:1000]
	}

	start := time.Now()
	defer func() {
		metrics.MemorySearchDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	// Generate query embedding with BGE query prefix and 1 retry
	queryVector, err := client.GenerateEmbedding(ctx, queryText, true, 1)
	if err != nil {
		metrics.MemoryOperationsTotal.WithLabelValues("search", "error").Inc()
		log.Printf("[Memory] Warning: Vector search embedding failed/timed out: %v. Proceeding without RAG context.", err)
		return nil, nil
	}

	ranked, err := factStore.SearchSimilarFacts(ctx, queryVector, maxFacts, DefaultMinScoreThreshold, "")
	if err != nil {
		metrics.MemoryOperationsTotal.WithLabelValues("search", "error").Inc()
		return nil, fmt.Errorf("failed to search similar facts: %w", err)
	}
	metrics.MemoryOperationsTotal.WithLabelValues("search", "success").Inc()
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
