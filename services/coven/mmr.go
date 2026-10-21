package main

import (
	"math"
)

// CosineSimilarity calculates the cosine similarity between two float vectors.
func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0.0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0.0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// MMRSelect performs Maximal Marginal Relevance diversity re-ranking on candidate facts.
// Iteratively picks candidate c maximizing:
//
//	lambda * normScore(c) - (1-lambda) * max_{s in S} cosineSim(c.Embedding, s.Embedding)
func MMRSelect(candidates []ScoredFactWithEmbedding, lambda float64, limit int) []ScoredFact {
	if len(candidates) == 0 || limit <= 0 {
		return nil
	}
	if limit > len(candidates) {
		limit = len(candidates)
	}
	if lambda <= 0.0 || lambda > 1.0 {
		lambda = 0.70
	}

	minScore := candidates[0].Score
	maxScore := candidates[0].Score
	for _, c := range candidates[1:] {
		if c.Score < minScore {
			minScore = c.Score
		}
		if c.Score > maxScore {
			maxScore = c.Score
		}
	}

	normScores := make([]float64, len(candidates))
	scoreRange := maxScore - minScore
	for i, c := range candidates {
		if scoreRange > 1e-9 {
			normScores[i] = (c.Score - minScore) / scoreRange
		} else {
			normScores[i] = 1.0
		}
	}

	selected := make([]ScoredFact, 0, limit)
	selectedEmbeddings := make([][]float32, 0, limit)
	selectedIndices := make(map[int]bool, limit)

	for len(selected) < limit {
		bestIdx := -1
		bestMMR := -math.MaxFloat64

		for i, cand := range candidates {
			if selectedIndices[i] {
				continue
			}

			maxSim := 0.0
			for _, sEmb := range selectedEmbeddings {
				if len(cand.Embedding) > 0 && len(sEmb) > 0 {
					sim := CosineSimilarity(cand.Embedding, sEmb)
					if sim > maxSim {
						maxSim = sim
					}
				}
			}

			mmrScore := lambda*normScores[i] - (1.0-lambda)*maxSim
			if mmrScore > bestMMR {
				bestMMR = mmrScore
				bestIdx = i
			}
		}

		if bestIdx == -1 {
			break
		}

		selectedIndices[bestIdx] = true
		selected = append(selected, ScoredFact{
			Fact:  candidates[bestIdx].Fact,
			Score: candidates[bestIdx].Score,
		})
		selectedEmbeddings = append(selectedEmbeddings, candidates[bestIdx].Embedding)
	}

	return selected
}
