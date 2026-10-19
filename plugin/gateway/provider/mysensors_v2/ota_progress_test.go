package mysensors

import (
	"testing"
	"time"

	"github.com/mycontroller-org/server/v2/pkg/types"
	"github.com/mycontroller-org/server/v2/pkg/types/cmap"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	"github.com/mycontroller-org/server/v2/pkg/utils/convertor"
)

// newOtaTestNode builds a minimal node for OTA state tests.
func newOtaTestNode(version string) *nodeTY.Node {
	return &nodeTY.Node{
		ID:        "gw_40",
		GatewayID: "gw",
		NodeID:    "40",
		Labels:    cmap.CustomStringMap{"version": version},
		Others:    cmap.CustomMap{},
	}
}

func TestComputeOtaProgressState_ArmsOnStartBlock(t *testing.T) {
	cases := []struct {
		name       string
		order      string
		startBlock int
	}{
		{"reverse: lastBlock is start", OTABlockOrderReverse, 943},
		{"forward: block 0 is start", OTABlockOrderForward, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newOtaTestNode("32")
			n.Labels.Set(types.LabelNodeOTABlockOrder, tc.order)
			// Simulate a cleanly closed previous OTA.
			n.Others.Set(types.FieldOTARunning, false, nil)
			n.Others.Set(types.FieldOTAProgress, 100, nil)
			n.Others.Set(types.FieldOTAEndTime, time.Now().Add(-time.Hour), nil)

			state, arm, ok := computeOtaProgressState(n, tc.startBlock, 944*16, 16)
			if !ok || !arm {
				t.Fatalf("start block must arm: ok=%v arm=%v", ok, arm)
			}
			if running, _ := state[types.FieldOTARunning].(bool); !running {
				t.Fatalf("state must carry running=true, got %v", state[types.FieldOTARunning])
			}
			if got := int(convertor.ToInteger(state[types.FieldOTAProgress])); got != 0 {
				t.Fatalf("progress must be 0, got %d", got)
			}
		})
	}
}

func TestComputeOtaProgressState_IgnoresLateBlockAfterClose(t *testing.T) {
	n := newOtaTestNode("33")
	n.Others.Set(types.FieldOTARunning, false, nil)
	n.Others.Set(types.FieldOTAProgress, 100, nil)
	n.Others.Set(types.FieldOTAEndTime, time.Now().Add(-time.Minute), nil)

	// Block 500 in reverse order is not a start block and not an end block.
	_, arm, ok := computeOtaProgressState(n, 500, 944*16, 16)
	if ok || arm {
		t.Fatalf("late block must be ignored: ok=%v arm=%v", ok, arm)
	}
}

func TestComputeOtaProgressState_EndBlock(t *testing.T) {
	n := newOtaTestNode("33")
	n.Others.Set(types.FieldOTARunning, true, nil)
	n.Others.Set(types.FieldOTAStartTime, time.Now().Add(-time.Minute), nil)

	// Reverse order: block 0 is the end.
	state, arm, ok := computeOtaProgressState(n, 0, 944*16, 16)
	if !ok {
		t.Fatal("end block must produce state")
	}
	if arm {
		t.Fatal("end block must not arm")
	}
	if running, _ := state[types.FieldOTARunning].(bool); running {
		t.Fatal("end block must set running=false")
	}
	if got := int(convertor.ToInteger(state[types.FieldOTAProgress])); got != 100 {
		t.Fatalf("progress must be 100, got %d", got)
	}
	if state[types.FieldOTABlockNumber] != nil {
		t.Fatalf("block_number must be nil on end, got %v", state[types.FieldOTABlockNumber])
	}
	if state[types.FieldOTAEndTime] == nil {
		t.Fatal("end_time must be set")
	}
}

func TestComputeOtaProgressState_NonMilestoneIgnored(t *testing.T) {
	n := newOtaTestNode("32")
	n.Others.Set(types.FieldOTARunning, true, nil)

	// Block 7 is not a milestone, not start, not end.
	_, _, ok := computeOtaProgressState(n, 7, 944*16, 16)
	if ok {
		t.Fatal("non-milestone block must be ignored")
	}
}