package pubsub

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// newJetStreamBusWithContext builds a bus around js with a 1ms heartbeat. A
// non-nil nc also gets a jetstream handle so durable consumers work.
func newJetStreamBusWithContext(nc *nats.Conn, js nats.JetStreamContext) *JetStreamBus {
	bus := &JetStreamBus{nc: nc, js: js, heartbeatInterval: 1 * time.Millisecond, pullHeartbeat: defaultPullHeartbeat}
	if nc != nil {
		jet, err := jetstream.New(nc)
		if err != nil {
			panic(err)
		}
		bus.jet = jet
	}
	return bus
}
