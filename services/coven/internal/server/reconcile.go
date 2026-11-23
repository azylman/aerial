package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/coven/internal/store"
)

func applyReconcileDecision(ctx context.Context, s store.Store, item ExtractedFactItem, emb []float32, neighbors []store.ScoredFact, dec ExtractedFactItem, agent ...string) error {
	validIDs := make(map[int64]bool, len(neighbors))
	for _, n := range neighbors {
		validIDs[n.Fact.ID] = true
	}

	action := strings.ToUpper(strings.TrimSpace(dec.Action))
	switch action {
	case actionDelete:
		targetID := dec.DeleteID
		if targetID <= 0 {
			targetID = dec.SupersedesID
		}
		if validIDs[targetID] {
			err := s.DeleteFact(ctx, targetID)
			switch {
			case errors.Is(err, store.ErrFactNotFound):
				return nil // already deleted by a concurrent writer
			case err != nil:
				return fmt.Errorf("error deleting fact: %w", err)
			}
			RecordFactStored("deleted")
			return nil
		}
		slog.InfoContext(ctx, "[Memory] tier 3 delete rejected: ID not in whitelisted neighbors (NOOP)", "fact_id", targetID)
		return nil
	case actionUpdate:
		targetID := dec.SupersedesID
		if targetID <= 0 {
			targetID = dec.DeleteID
		}
		if validIDs[targetID] {
			factText := dec.FactText
			if strings.TrimSpace(factText) == "" {
				factText = item.FactText
			}
			if err := s.UpdateFact(ctx, targetID, factText, emb); err != nil {
				return fmt.Errorf("error updating fact: %w", err)
			}
			RecordFactStored("updated")
			return nil
		}
		slog.InfoContext(ctx, "[Memory] tier 3 update rejected: ID not in whitelisted neighbors; falling back to ADD", "fact_id", targetID)
		if _, err := s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting fallback fact: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	case actionNoop:
		targetID := dec.SupersedesID
		if !validIDs[targetID] {
			targetID = neighbors[0].Fact.ID
		}
		if err := s.ReinforceFact(ctx, targetID); err != nil {
			return fmt.Errorf("error reinforcing NOOP fact: %w", err)
		}
		RecordFactStored("reinforced")
		return nil
	default: // "ADD" or fallback
		if _, err := s.InsertFact(ctx, item.FactText, emb, agent...); err != nil {
			return fmt.Errorf("error inserting fact: %w", err)
		}
		RecordFactStored("inserted")
		return nil
	}
}

type factConnProvider interface {
	Conn(ctx context.Context) (*sql.Conn, error)
	DB() store.DBTX
	WithPinnedConn(conn *sql.Conn) store.Store
}

func rollbackConn(ctx context.Context, conn *sql.Conn) {
	if _, err := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); err != nil && !errors.Is(err, sql.ErrTxDone) {
		slog.DebugContext(ctx, "failed to rollback fact write transaction", "error", err)
	}
}

func runFactWriteTx(ctx context.Context, conn *sql.Conn, shouldClose bool, txStore store.Store, fn func(context.Context, store.Store) error) error {
	if shouldClose {
		defer httpkit.CloseWarn(conn, "fact write transaction connection")
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("coven tx: begin fact write: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackConn(ctx, conn)
		}
	}()
	if err := fn(ctx, txStore); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("coven tx: commit fact write: %w", err)
	}
	committed = true
	return nil
}

// withFactWriteTx runs fn in one short transaction on a pinned connection so a multi-row fact
// write (consolidation: insert merged facts, delete sources) commits or rolls back as a unit.
// It takes no global lock: per-thread extraction is serialized by the CovenThread Virtual Object,
// and cross-thread writes rely on Postgres row locks (UPDATE/DELETE lock only their rows).
// Stores without a pinnable connection run fn directly.
func withFactWriteTx(ctx context.Context, s store.Store, fn func(ctx context.Context, txStore store.Store) error) error {
	cp, ok := s.(factConnProvider)
	if !ok {
		return fn(ctx, s)
	}
	conn, err := cp.Conn(ctx)
	if errors.Is(err, store.ErrNoPinnedConn) {
		return fn(ctx, s)
	}
	if err != nil {
		return fmt.Errorf("coven tx: acquire fact write connection: %w", err)
	}
	if conn == nil {
		return fmt.Errorf("coven tx: acquire fact write connection: nil connection")
	}
	_, isPinnedConn := cp.DB().(*sql.Conn)
	return runFactWriteTx(ctx, conn, !isPinnedConn, cp.WithPinnedConn(conn), fn)
}

// reconcilePlan is the read-and-LLM half of candidate reconciliation: the candidate's
// embedding and, when the nearest neighbor sat in the tier-3 band, the LLM's decision.
type reconcilePlan struct {
	Admit     bool               `json:"admit,omitempty"`
	Embedding []float32          `json:"embedding,omitempty"`
	Decision  *ExtractedFactItem `json:"decision,omitempty"`
}

// reconcileCandidateFact plans and then writes one candidate fact.
func reconcileCandidateFact(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, item ExtractedFactItem, agent string) error {
	plan, err := planCandidateReconcile(ctx, s, client, llmFunc, item, agent)
	if err != nil {
		return err
	}
	return writeCandidateReconcile(ctx, s, item, plan, agent)
}

// planCandidateReconcile admits the candidate, embeds it and, for tier-3 neighbors, asks the
// LLM for a decision. It writes nothing, so the LLM call never holds a transaction or lock.
func planCandidateReconcile(ctx context.Context, s store.Store, client store.OllamaClient, llmFunc LLMClientFunc, item ExtractedFactItem, agent string) (reconcilePlan, error) {
	if s == nil || client == nil {
		return reconcilePlan{}, fmt.Errorf("nil s or client")
	}
	if strings.EqualFold(strings.TrimSpace(item.Action), actionDelete) {
		return reconcilePlan{}, nil
	}
	if admit, _ := EvaluateCandidateAdmission(item); !admit {
		return reconcilePlan{}, nil
	}

	emb, err := client.GenerateEmbedding(ctx, item.FactText, false, 1)
	if err != nil {
		return reconcilePlan{}, fmt.Errorf("failed to generate embedding for candidate fact: %w", err)
	}
	if len(emb) != store.ExpectedEmbeddingDim {
		return reconcilePlan{}, fmt.Errorf("candidate fact embedding dimension mismatch: got %d, want %d", len(emb), store.ExpectedEmbeddingDim)
	}

	neighbors, err := s.FindNearestScoredFacts(ctx, emb, 5, 0.60)
	if err != nil {
		return reconcilePlan{}, fmt.Errorf("FindNearestScoredFacts error: %w", err)
	}
	plan := reconcilePlan{Admit: true, Embedding: emb}
	if len(neighbors) > 0 && neighbors[0].Score <= 0.88 {
		dec := evaluateTier3CandidateDecision(ctx, llmFunc, item, neighbors, agent)
		plan.Decision = &dec
	}
	return plan, nil
}

// writeCandidateReconcile re-reads the neighbors and applies the plan with a single fact
// mutation. A neighbor that a concurrent writer deleted after planning is not an error:
// the candidate is inserted as a new fact instead (dropping it is worse than a near-duplicate,
// which dreaming consolidation merges later).
func writeCandidateReconcile(ctx context.Context, s store.Store, item ExtractedFactItem, plan reconcilePlan, agent string) error {
	if !plan.Admit {
		return nil
	}
	if s == nil {
		return fmt.Errorf("nil s")
	}
	emb := plan.Embedding
	neighbors, err := s.FindNearestScoredFacts(ctx, emb, 5, 0.60)
	if err != nil {
		return fmt.Errorf("FindNearestScoredFacts error: %w", err)
	}

	switch {
	case len(neighbors) == 0: // Tier 1: maxSim < 0.60
		err = insertCandidateFact(ctx, s, item.FactText, emb, agent)
	case neighbors[0].Score > 0.88: // Tier 2
		if err = s.ReinforceFact(ctx, neighbors[0].Fact.ID); err == nil {
			RecordFactStored("reinforced")
		}
	default: // Tier 3: 0.60 <= score <= 0.88
		decision := ExtractedFactItem{Action: actionAdd, FactText: item.FactText}
		if plan.Decision != nil {
			decision = *plan.Decision
		}
		err = applyReconcileDecision(ctx, s, item, emb, neighbors, decision, agent)
	}
	if errors.Is(err, store.ErrFactNotFound) {
		slog.InfoContext(ctx, "[Memory] reconcile target fact vanished concurrently; inserting candidate", "fact_text", item.FactText)
		return insertCandidateFact(ctx, s, item.FactText, emb, agent)
	}
	return err
}

func insertCandidateFact(ctx context.Context, s store.Store, factText string, emb []float32, agent string) error {
	if _, err := s.InsertFact(ctx, factText, emb, agent); err != nil {
		return fmt.Errorf("error inserting fact: %w", err)
	}
	RecordFactStored("inserted")
	return nil
}

func evaluateTier3CandidateDecision(ctx context.Context, llmFunc LLMClientFunc, item ExtractedFactItem, neighbors []store.ScoredFact, agent ...string) ExtractedFactItem {
	fallback := ExtractedFactItem{Action: actionAdd, FactText: item.FactText}
	if llmFunc == nil {
		return fallback
	}

	reconcilePrompt := buildCandidateReconcilePrompt(item, neighbors)
	reconcileAgent := ""
	if len(agent) > 0 {
		reconcileAgent = strings.TrimSpace(agent[0])
	}
	reconcileCtx := withLLMKey(ctx, "coven:reconcile:"+reconcileAgent+":"+hash16(item.FactText))
	resp, err := llmFunc(reconcileCtx, reconcilePrompt)
	if err != nil {
		slog.WarnContext(ctx, "[Memory] tier 3 reconciliation LLM error; falling back to ADD", "error", err)
		return fallback
	}

	decisionPayload, err := parseFactsJSON(resp)
	if err != nil || len(decisionPayload.Facts) == 0 {
		slog.WarnContext(ctx, "[Memory] tier 3 parse error; falling back to ADD", "error", err)
		return fallback
	}

	return decisionPayload.Facts[0]
}
