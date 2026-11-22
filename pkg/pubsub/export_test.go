package pubsub

import (
	"time"

	"github.com/nats-io/nats.go"
)

func newJetStreamBusWithContext(nc *nats.Conn, js nats.JetStreamContext) *JetStreamBus {
	return &JetStreamBus{nc: nc, js: js, heartbeatInterval: 1 * time.Millisecond}
}
