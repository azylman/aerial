package pubsub

import "time"

// NakBackoff returns the redelivery delay for a message that failed on its
// numDelivered-th delivery: 1s for the first delivery, then doubling
// (1s<<(numDelivered-1)), clamped to maxDelay.
func NakBackoff(numDelivered uint64, maxDelay time.Duration) time.Duration {
	delay := time.Second
	if numDelivered > 1 {
		delay = time.Second << min(numDelivered-1, 30)
	}
	return min(delay, maxDelay)
}
