package fakes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

func TestMain(m *testing.M) {
	InstallTestTracing()
	os.Exit(m.Run())
}

func fakesErrText(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func fakesCallText(c *model.VoiceCall, err error) string {
	if c == nil {
		return "nil " + fakesErrText(err)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s turns=%d cursor=%d instr=%d vm=%s report=%s/%s %s", c.CallID, c.ProviderCallSID, c.State, c.EndReason,
		c.AnsweredBy, len(c.Transcript), c.LiveTranscriptCursor, len(c.OwnerInstructions), c.VoicemailRecordingURL, c.ReportStatus, c.ReportSummary, fakesErrText(err))
}

// fakesScript drives every fake method once per branch and logs each outcome.
func fakesScript(s *FakeStore, tw *FakeTwilio, failTo []string) []string {
	ctx := context.Background()
	SeedFailTransitions(s, failTo)
	var log []string
	add := func(format string, a ...any) { log = append(log, fmt.Sprintf(format, a...)) }

	add("create-out %s", fakesErrText(s.CreateOutboundCall(ctx, model.VoiceCall{CallID: "c1", Direction: "outbound", ToNumber: "+15105550123", ReportStatus: "pending"}, 1)))
	add("create-in %s", fakesErrText(s.CreateInboundCall(ctx, model.VoiceCall{CallID: "c2", Direction: "inbound"})))
	add("bind %s %s", fakesErrText(s.BindProviderCallSID(ctx, "c1", "CA1")), fakesErrText(s.BindProviderCallSID(ctx, "missing", "CA9")))
	add("get %s | %s", fakesCallText(s.GetCall(ctx, "c1")), fakesCallText(s.GetCall(ctx, "missing")))
	add("by-sid %s | %s", fakesCallText(s.GetCallByProviderSID(ctx, "CA1")), fakesCallText(s.GetCallByProviderSID(ctx, "CA9")))
	calls, err := s.ListCalls(ctx, 10)
	add("list-calls %d %s", len(calls), fakesErrText(err))
	add("recent %s | %s", fakesCallText(s.FindRecentOutboundCall(ctx, "+15105550123", time.Hour)), fakesCallText(s.FindRecentOutboundCall(ctx, "+15105550199", time.Hour)))
	add("state %s %s %s", fakesErrText(s.UpdateCallState(ctx, "c1", "ended", "hangup", "human")),
		fakesErrText(s.UpdateCallState(ctx, "c1", "done", "", "")), fakesErrText(s.UpdateCallState(ctx, "missing", "done", "", "")))
	add("turn %s %s", fakesErrText(s.AppendTranscriptTurn(ctx, "c1", model.TranscriptTurn{})), fakesErrText(s.AppendTranscriptTurn(ctx, "missing", model.TranscriptTurn{})))
	adv1, err1 := s.AdvanceLiveTranscriptCursor(ctx, "c1", 0, 2)
	adv2, err2 := s.AdvanceLiveTranscriptCursor(ctx, "c1", 0, 3)
	adv3, err3 := s.AdvanceLiveTranscriptCursor(ctx, "missing", 0, 1)
	add("advance %t %s %t %s %t %s", adv1, fakesErrText(err1), adv2, fakesErrText(err2), adv3, fakesErrText(err3))
	add("instr %s %s", fakesErrText(s.AppendOwnerInstruction(ctx, "c1", model.OwnerInstruction{})),
		fakesErrText(s.AppendOwnerInstruction(ctx, "missing", model.OwnerInstruction{})))
	add("vm %s %s", fakesErrText(s.SetVoicemailRecording(ctx, "c1", "https://rec.example.com/1")),
		fakesErrText(s.SetVoicemailRecording(ctx, "missing", "https://rec.example.com/2")))
	t1, terr1 := s.TransitionReportStatus(ctx, "c1", []string{"pending"}, "reported", "sum")
	t2, terr2 := s.TransitionReportStatus(ctx, "c1", []string{"pending"}, "retry", "")
	t3, terr3 := s.TransitionReportStatus(ctx, "c1", nil, "final", "")
	t4, terr4 := s.TransitionReportStatus(ctx, "missing", nil, "final", "")
	add("trans %t %s %t %s %t %s %t %s", t1, fakesErrText(terr1), t2, fakesErrText(terr2), t3, fakesErrText(terr3), t4, fakesErrText(terr4))
	n, err := s.ReconcileOrphanedCalls(ctx, nil)
	add("reconcile %d %s", n, fakesErrText(err))
	saved, err := s.SaveVoiceMessage(ctx, model.VoiceMessage{MessageSID: "m1"})
	add("save %t %s", saved, fakesErrText(err))
	add("mark %s %s", fakesErrText(s.MarkVoiceMessageDelivered(ctx, "m1")), fakesErrText(s.MarkVoiceMessageDelivered(ctx, "missing")))
	msgs, err := s.ListVoiceMessages(ctx, 10)
	delivered := slices.ContainsFunc(msgs, func(m model.VoiceMessage) bool { return m.DiscordDelivered })
	add("list-msgs %d %t %s", len(msgs), delivered, fakesErrText(err))
	add("final %s", fakesCallText(s.GetCall(ctx, "c1")))
	add("store advances=%d trans=%v vm=%q close=%s", s.advances, s.statusTrans, s.vmRecordings["c1"], fakesErrText(s.Close()))

	sid, err := tw.CreateCall(ctx, twilio.CreateCallRequest{To: "+15105550123"})
	add("tw-call %s %s", sid, fakesErrText(err))
	add("tw-hangup %s", fakesErrText(tw.HangupCall(ctx, "CA1")))
	active, err := tw.IsCallActive(ctx, "CA1")
	add("tw-active %t %s", active, fakesErrText(err))
	sid, err = tw.SendSMS(ctx, "+15105550123", "+15105550100", "hi")
	add("tw-sms %s %s", sid, fakesErrText(err))
	num, err := tw.EnsurePhoneNumber(ctx, "510", "https://hooks.example.com")
	add("tw-phone %s %s", num, fakesErrText(err))
	data, ctype, err := tw.DownloadRecording(ctx, "https://rec.example.com/1")
	add("tw-download %q %q %s", data, ctype, fakesErrText(err))
	add("tw-recorded to=%s last=%s/%s/%s calls=%d hungup=%v sms=%v", tw.LastCreateCallReq.To, tw.LastSendSMSTo, tw.LastSendSMSFrom,
		tw.LastWebhookBaseURL, len(tw.CreatedCalls), tw.HungUpCallSIDs, tw.SMSList)

	traced, traceID := TraceCaller(true)
	header := http.Header{}
	propagation.TraceContext{}.Inject(traced, propagation.HeaderCarrier(header))
	bare, bareID := TraceCaller(false)
	add("trace %t %t %q %q %t", traceID != "" && HeaderTraceID(header) == traceID, bare != nil, bareID, HeaderTraceID(http.Header{}),
		StaticConfig{Cfg: &config.Config{BrainURL: "http://brain"}}.Current().BrainURL == "http://brain")
	return log
}

func TestFakes(t *testing.T) {
	t.Parallel()
	errOp, errBind, errState, errTrans := errors.New("op"), errors.New("bind"), errors.New("state"), errors.New("trans")
	errAdv, errVM, errMark, errSave := errors.New("advance"), errors.New("vmrec"), errors.New("mark"), errors.New("save")
	errDL, errCall, errSMS, errPhone, errHang := errors.New("download"), errors.New("call"), errors.New("sms"), errors.New("phone"), errors.New("hangup")

	tests := []struct {
		name   string
		store  *FakeStore
		twilio *FakeTwilio
		failTo []string
		want   []string
	}{
		{
			name:   "happy path",
			store:  NewFakeStore(),
			twilio: &FakeTwilio{AudioData: []byte("wav"), ContentType: "audio/wav"},
			want: []string{
				"create-out <nil>",
				"create-in <nil>",
				"bind <nil> <nil>",
				"get c1/CA1/// turns=0 cursor=0 instr=0 vm= report=pending/ <nil> | nil <nil>",
				"by-sid c1/CA1/// turns=0 cursor=0 instr=0 vm= report=pending/ <nil> | nil <nil>",
				"list-calls 2 <nil>",
				"recent c1/CA1/// turns=0 cursor=0 instr=0 vm= report=pending/ <nil> | nil <nil>",
				"state <nil> <nil> <nil>",
				"turn <nil> <nil>",
				"advance true <nil> false <nil> false <nil>",
				"instr <nil> <nil>",
				"vm <nil> <nil>",
				"trans true <nil> false <nil> true <nil> false <nil>",
				"reconcile 0 <nil>",
				"save true <nil>",
				"mark <nil> <nil>",
				"list-msgs 1 true <nil>",
				"final c1/CA1/done/hangup/human turns=1 cursor=2 instr=1 vm=https://rec.example.com/1 report=final/sum <nil>",
				`store advances=1 trans=[reported final] vm="https://rec.example.com/1" close=<nil>`,
				"tw-call CA12345 <nil>",
				"tw-hangup <nil>",
				"tw-active true <nil>",
				"tw-sms SM12345 <nil>",
				"tw-phone +15105550100 <nil>",
				`tw-download "wav" "audio/wav" <nil>`,
				"tw-recorded to=+15105550123 last=+15105550123/+15105550100/https://hooks.example.com calls=1 hungup=[CA1] sms=[+15105550123:hi]",
				`trace true true "" "" true`,
			},
		},
		{
			name: "injected errors",
			store: &FakeStore{
				calls: map[string]*model.VoiceCall{}, messages: map[string]*model.VoiceMessage{}, vmRecordings: map[string]string{},
				OpErr: errOp, BindErr: errBind, UpdateStateErr: errState, TransErr: errTrans, AdvanceErr: errAdv, VMRecErr: errVM, MarkErr: errMark, SaveErr: errSave,
			},
			twilio: &FakeTwilio{DownloadErr: errDL, CreateCallErr: errCall, SendSMSErr: errSMS, EnsurePhoneErr: errPhone, HangupErr: errHang},
			want: []string{
				"create-out op",
				"create-in <nil>",
				"bind bind bind",
				"get nil op | nil op",
				"by-sid nil op | nil op",
				"list-calls 0 op",
				"recent nil <nil> | nil <nil>",
				"state state state state",
				"turn <nil> <nil>",
				"advance false advance false advance false advance",
				"instr <nil> <nil>",
				"vm vmrec vmrec",
				"trans false trans false trans false trans false trans",
				"reconcile 0 <nil>",
				"save false save",
				"mark mark mark",
				"list-msgs 0 false op",
				"final nil op",
				`store advances=0 trans=[] vm="" close=<nil>`,
				"tw-call  call",
				"tw-hangup hangup",
				"tw-active true <nil>",
				"tw-sms  sms",
				"tw-phone  phone",
				`tw-download "" "" download`,
				"tw-recorded to=+15105550123 last=+15105550123/+15105550100/https://hooks.example.com calls=1 hungup=[CA1] sms=[+15105550123:hi]",
				`trace true true "" "" true`,
			},
		},
		{
			name:   "seeded failed transition",
			store:  NewFakeStore(),
			twilio: &FakeTwilio{},
			failTo: []string{"reported"},
			want: []string{
				"create-out <nil>",
				"create-in <nil>",
				"bind <nil> <nil>",
				"get c1/CA1/// turns=0 cursor=0 instr=0 vm= report=pending/ <nil> | nil <nil>",
				"by-sid c1/CA1/// turns=0 cursor=0 instr=0 vm= report=pending/ <nil> | nil <nil>",
				"list-calls 2 <nil>",
				"recent c1/CA1/// turns=0 cursor=0 instr=0 vm= report=pending/ <nil> | nil <nil>",
				"state <nil> <nil> <nil>",
				"turn <nil> <nil>",
				"advance true <nil> false <nil> false <nil>",
				"instr <nil> <nil>",
				"vm <nil> <nil>",
				"trans false transition to reported failed true <nil> true <nil> false <nil>",
				"reconcile 0 <nil>",
				"save true <nil>",
				"mark <nil> <nil>",
				"list-msgs 1 true <nil>",
				"final c1/CA1/done/hangup/human turns=1 cursor=2 instr=1 vm=https://rec.example.com/1 report=final/ <nil>",
				`store advances=1 trans=[retry final] vm="https://rec.example.com/1" close=<nil>`,
				"tw-call CA12345 <nil>",
				"tw-hangup <nil>",
				"tw-active true <nil>",
				"tw-sms SM12345 <nil>",
				"tw-phone +15105550100 <nil>",
				`tw-download "" "" <nil>`,
				"tw-recorded to=+15105550123 last=+15105550123/+15105550100/https://hooks.example.com calls=1 hungup=[CA1] sms=[+15105550123:hi]",
				`trace true true "" "" true`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fakesScript(tc.store, tc.twilio, tc.failTo)
			if !slices.Equal(got, tc.want) {
				t.Errorf("script log mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

func TestTwilioSignature(t *testing.T) {
	t.Parallel()
	const hook = "https://example.com/twilio/voice"
	tests := []struct {
		name string
		form url.Values
		want string
	}{
		{"empty form signs url only", nil, "SFP0X3QJ6D9JMwhi4ysYEBK+lYc="},
		{"keys sorted", url.Values{"From": {"+15105550100"}, "CallSid": {"CA1"}}, "uw0rWi6HmuFpXh6Trr4McXJi6UM="},
		{"every value of a key in order", url.Values{"Media": {"b", "a"}}, "xfOJPvR1N8WDxfW/Oi8bbN/X9bc="},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TwilioSignature("token-a", hook, tc.form); got != tc.want {
				t.Errorf("TwilioSignature() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFakeStore_ListUnfinishedReportCalls(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		calls     []model.VoiceCall
		storeErr  error
		limit     int
		wantCount int
		wantErr   bool
	}{
		{
			name:      "empty store returns empty slice",
			limit:     10,
			wantCount: 0,
		},
		{
			name:     "store error returns error",
			storeErr: errors.New("query error"),
			limit:    10,
			wantErr:  true,
		},
		{
			name: "filters pending and processing calls up to limit",
			calls: []model.VoiceCall{
				{CallID: "c1", ReportStatus: "pending", StartedAt: time.Now().Add(-2 * time.Minute)},
				{CallID: "c2", ReportStatus: "delivered", StartedAt: time.Now().Add(-1 * time.Minute)},
				{CallID: "c3", ReportStatus: "processing", StartedAt: time.Now()},
			},
			limit:     1,
			wantCount: 1,
		},
		{
			name: "returns all matching when limit exceeds count",
			calls: []model.VoiceCall{
				{CallID: "c1", ReportStatus: "pending", StartedAt: time.Now().Add(-2 * time.Minute)},
				{CallID: "c2", ReportStatus: "processing", StartedAt: time.Now()},
			},
			limit:     10,
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := NewFakeStore()
			st.OpErr = tc.storeErr
			for _, c := range tc.calls {
				_ = st.CreateOutboundCall(context.Background(), c, 5)
			}
			got, err := st.ListUnfinishedReportCalls(context.Background(), tc.limit)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ListUnfinishedReportCalls() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("len(got) = %d, want %d", len(got), tc.wantCount)
			}
		})
	}
}
