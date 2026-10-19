package mysensors

import (
	"testing"
)

// TestForceOtaCompletion_ClearsBlockNumber requires a bus mock for the
// persistence verification inside forceOtaCompletion (p.getNode).
// Enable once a mockBus is available.
func TestForceOtaCompletion_ClearsBlockNumber(t *testing.T) {
	t.Skip("requires a bus mock for getNode persistence verification")
}