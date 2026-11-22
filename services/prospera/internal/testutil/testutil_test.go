package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/prospera/internal/model"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

func TestSetUpTestStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := SetUpTestStore(t)
	call := model.VoiceCall{CallID: "call-alpha", Direction: "outbound", State: "queued", FromNumber: "+15105550100", ToNumber: "+15105550101"}
	if err := st.CreateOutboundCall(ctx, call, 1); err != nil {
		t.Fatalf("CreateOutboundCall() error = %v", err)
	}
	got, err := st.GetCall(ctx, "call-alpha")
	if err != nil {
		t.Fatalf("GetCall() error = %v", err)
	}
	if got.ToNumber != "+15105550101" || got.State != "queued" {
		t.Errorf("GetCall() = {ToNumber: %q, State: %q}, want {+15105550101, queued}", got.ToNumber, got.State)
	}
}
