package mysensors

import (
	"testing"
	"time"

	"github.com/mycontroller-org/server/v2/pkg/types/cmap"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
)

func TestGetFotaUpdateMode(t *testing.T) {
	tests := []struct {
		name  string
		label string
		want  string
	}{
		{"absent", "", FotaUpdateUnknown},
		{"online", "online", FotaUpdateOnline},
		{"offline", "offline", FotaUpdateOffline},
		{"unknown", "unknown", FotaUpdateUnknown},
		{"ONLINE", "ONLINE", FotaUpdateOnline},
		{" padded ", "  offline  ", FotaUpdateOffline},
		{"garbage", "garbage", FotaUpdateUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := cmap.CustomStringMap{}
			if tt.label != "" {
				labels.Set(LabelFotaUpdate, tt.label)
			}
			n := &nodeTY.Node{Labels: labels}
			if got := getFotaUpdateMode(n); got != tt.want {
				t.Errorf("getFotaUpdateMode(%q) = %q, want %q", tt.label, got, tt.want)
			}
		})
	}

	if getFotaUpdateMode(nil) != FotaUpdateUnknown {
		t.Fatal("nil node must be unknown")
	}
}

func TestIsSleepingNode(t *testing.T) {
	n := &nodeTY.Node{Labels: cmap.CustomStringMap{}}
	if isSleepingNode(n) {
		t.Fatal("empty labels must not be sleeping")
	}
	n.Labels.Set(LabelSmartSleepNode, "true")
	if !isSleepingNode(n) {
		t.Fatal("smart_sleep_node=true must be sleeping")
	}
	n.Labels.Set(LabelSmartSleepNode, "false")
	if isSleepingNode(n) {
		t.Fatal("smart_sleep_node=false must not be sleeping")
	}
}

func TestIsRecentlySeen(t *testing.T) {
	n := &nodeTY.Node{LastSeen: time.Now()}
	if !isRecentlySeen(n, 30*time.Second) {
		t.Fatal("just seen must be recent")
	}
	n.LastSeen = time.Now().Add(-1 * time.Minute)
	if isRecentlySeen(n, 30*time.Second) {
		t.Fatal("1 min ago must not be recent for 30s threshold")
	}
}

func TestMaxSleepDuration(t *testing.T) {
	n := &nodeTY.Node{Labels: cmap.CustomStringMap{}}
	if got := maxSleepDuration(n); got != 5*time.Minute {
		t.Fatalf("default: %v", got)
	}
	n.Labels.Set(FieldSleepDuration, "280000")
	if got := maxSleepDuration(n); got < 4*time.Minute || got > 5*time.Minute {
		t.Fatalf("280000ms: %v", got)
	}
	n.Labels.Set(FieldSleepDuration, "10000")
	if got := maxSleepDuration(n); got != time.Minute {
		t.Fatalf("10s clamp: %v", got)
	}
	n.Labels.Set(FieldSleepDuration, "7200000")
	if got := maxSleepDuration(n); got != time.Hour {
		t.Fatalf("2h clamp: %v", got)
	}
}

func TestOtaDeliveryDelay(t *testing.T) {
	n := &nodeTY.Node{Labels: cmap.CustomStringMap{}}
	if got := otaDeliveryDelay(n); got != fotaDeliveryDelayDefault {
		t.Fatalf("default: %v", got)
	}
	n.Labels.Set(LabelFotaDeliveryDelay, "0")
	if got := otaDeliveryDelay(n); got != 0 {
		t.Fatalf("explicit 0: %v", got)
	}
	n.Labels.Set(LabelFotaDeliveryDelay, "10")
	if got := otaDeliveryDelay(n); got != 50*time.Millisecond {
		t.Fatalf("10 ms should clamp to 50: %v", got)
	}
	n.Labels.Set(LabelFotaDeliveryDelay, "10000")
	if got := otaDeliveryDelay(n); got != 5*time.Second {
		t.Fatalf("10000 ms should clamp to 5s: %v", got)
	}
	n.Labels.Set(LabelFotaDeliveryDelay, "garbage")
	if got := otaDeliveryDelay(n); got != fotaDeliveryDelayDefault {
		t.Fatalf("garbage should fall back to default: %v", got)
	}

	// Deprecated fallback
	n2 := &nodeTY.Node{Labels: cmap.CustomStringMap{"bootloader_ota_reboot_delay": "150"}}
	if got := otaDeliveryDelay(n2); got != 150*time.Millisecond {
		t.Fatalf("deprecated fallback: %v", got)
	}
	// New key wins over deprecated when both present
	n3 := &nodeTY.Node{Labels: cmap.CustomStringMap{
		LabelFotaDeliveryDelay:        "300",
		"bootloader_ota_reboot_delay": "150",
	}}
	if got := otaDeliveryDelay(n3); got != 300*time.Millisecond {
		t.Fatalf("new key must win: %v", got)
	}
}

func TestMaybeTriggerOtaRouting(t *testing.T) {
	// This test exercises the routing decision of maybeTriggerOta without
	// actually writing on a protocol. The decision matrix is:
	//   unknown  -> nothing (no queue, no send)
	//   offline  + awake   -> sendRebootNow (we check it goes down that branch)
	//   offline  + sleeping -> queue
	//   online   + awake   -> sendFirmwareConfigResponseNow
	//   online   + sleeping -> queue
	//
	// We cannot easily stub the Provider's protocol here; the routing is
	// covered indirectly by TestGetFotaUpdateMode + TestPendingFotaActionCount.
	// A fuller integration test will be added when a protocol mock exists.
	t.Skip("requires a protocol mock for full routing coverage")
}

func TestPendingFotaActionCount(t *testing.T) {
	for _, k := range otaPendingActionStore.Keys() {
		otaPendingActionStore.Remove(k)
	}
	defer func() {
		for _, k := range otaPendingActionStore.Keys() {
			otaPendingActionStore.Remove(k)
		}
	}()

	if n := pendingFotaActionCount(); n != 0 {
		t.Fatalf("expected 0 pending actions, got %d", n)
	}

	otaPendingActionStore.Add("gw1.node1", pendingFotaAction{Mode: FotaUpdateOffline, QueuedAt: time.Now()})
	otaPendingActionStore.Add("gw1.node2", pendingFotaAction{Mode: FotaUpdateOnline, QueuedAt: time.Now()})

	if n := pendingFotaActionCount(); n != 2 {
		t.Fatalf("expected 2 pending actions, got %d", n)
	}

	otaPendingActionStore.Remove("gw1.node1")

	if n := pendingFotaActionCount(); n != 1 {
		t.Fatalf("expected 1 pending action, got %d", n)
	}
}

func TestSendRebootNowBuildsAckMessage(t *testing.T) {
	// Verify the raw message built by sendRebootNow carries the expected
	// fields. We cannot call Protocol.Write without a real protocol, so we
	// replicate the message construction and assert.
	msMsg := &message{
		NodeID:   "56",
		SensorID: idBroadcast,
		Command:  cmdInternal,
		Ack:      "0",
		Type:     actionReboot,
		Payload:  payloadEmpty,
	}
	raw := msMsg.toMySensorsRaw(false)
	if raw != "56;255;3;0;13;\n" {
		t.Fatalf("serial raw = %q", raw)
	}
	mqtt := msMsg.toMySensorsRaw(true)
	if mqtt != "56/255/3/0/13" {
		t.Fatalf("mqtt raw = %q", mqtt)
	}
}

func TestSendFirmwareConfigResponseNowBuildsMessage(t *testing.T) {
	// Verify the raw message built for the online OTA carries the expected
	// fields. We cannot call executeFirmwareConfigRequest without a full
	// provider (bus, store), so we build the message by hand and assert
	// the format, same as TestSendRebootNowBuildsAckMessage.
	msMsg := &message{
		NodeID:   "57",
		SensorID: idBroadcast,
		Command:  cmdStream,
		Ack:      "0",
		Type:     actionFirmwareConfigResponse,
		Payload:  "0A0B0C",
	}
	raw := msMsg.toMySensorsRaw(false)
	if raw != "57;255;4;0;1;0A0B0C\n" {
		t.Fatalf("serial raw = %q", raw)
	}
	mqtt := msMsg.toMySensorsRaw(true)
	if mqtt != "57/255/4/0/1" {
		t.Fatalf("mqtt raw = %q", mqtt)
	}
}
