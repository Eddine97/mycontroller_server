package mysensors

import (
	"fmt"
	"time"

	msgTY "github.com/mycontroller-org/server/v2/pkg/types/message"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	gwPtl "github.com/mycontroller-org/server/v2/plugin/gateway/protocol"
	"go.uber.org/zap"
)

// smartSleepRecentlySeen is the window during which a smart-sleep node is
// considered "awake". Below this threshold, we assume it can receive messages.
const smartSleepRecentlySeen = 30 * time.Second

// otaRebootDeliveryDelayDefault is the delay between a wake signal and the
// I_REBOOT. Must stay strictly below the node's awake_duration.
// Default: 200 ms. Overridable per node with bootloader_ota_reboot_delay.
const otaRebootDeliveryDelayDefault = 200 * time.Millisecond

// shouldRebootForOta reports whether the node opts into automatic reboot for OTA.
func shouldRebootForOta(node *nodeTY.Node) bool {
	if node == nil || node.Labels == nil {
		return false
	}
	return node.Labels.GetBool(LabelBootloaderOtaReboot)
}

// isSleepingNode reports whether the node uses smart sleep.
func isSleepingNode(node *nodeTY.Node) bool {
	if node == nil || node.Labels == nil {
		return false
	}
	return node.Labels.GetBool(LabelSmartSleepNode)
}

// isRecentlySeen reports whether the node was seen within the given window.
func isRecentlySeen(node *nodeTY.Node, threshold time.Duration) bool {
	if node == nil {
		return false
	}
	return time.Since(node.LastSeen) < threshold
}

// maxSleepDuration returns an upper bound on the node's sleep cycle.
func maxSleepDuration(node *nodeTY.Node) time.Duration {
	if node == nil || node.Labels == nil {
		return 5 * time.Minute
	}
	ms := int64(node.Labels.GetInt(FieldSleepDuration))
	if ms <= 0 {
		return 5 * time.Minute
	}
	d := time.Duration(ms) * time.Millisecond
	if d < time.Minute {
		d = time.Minute
	}
	if d > time.Hour {
		d = time.Hour
	}
	return d
}

// otaRebootDeliveryDelay returns the delay for a given node.
func otaRebootDeliveryDelay(node *nodeTY.Node) time.Duration {
	if node == nil || node.Labels == nil {
		return otaRebootDeliveryDelayDefault
	}
	v := node.Labels.GetInt(LabelBootloaderOtaRebootDelay)
	if v < 0 {
		return otaRebootDeliveryDelayDefault
	}
	if v == 0 {
		// v == 0 explicitly means no delay
		return 0
	}
	d := time.Duration(v) * time.Millisecond
	if d < 50*time.Millisecond {
		d = 50 * time.Millisecond
	}
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// maybeSendRebootForOta is called before serving a firmware-update action.
// It sends I_REBOOT to awake nodes, or queues it for sleeping ones.
func (p *Provider) maybeSendRebootForOta(msg *msgTY.Message) error {
	node, err := p.getNode(msg.GatewayID, msg.NodeID)
	if err != nil {
		return fmt.Errorf("load node for OTA reboot check: %w", err)
	}

	explicitOptIn := shouldRebootForOta(node)
	detectedMYSBootloader := isMYSBootloaderKind(bootloaderKindForNode(node))

	if !explicitOptIn && !detectedMYSBootloader {
		p.logger.Debug("no pre-OTA reboot needed",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.String("bootloaderKind", bootloaderKindForNode(node)),
			zap.Bool("explicitOptIn", explicitOptIn),
		)
		return nil
	}

	// [FIX n°2] For smart-sleep nodes, always queue the reboot, even if the
	// node was seen recently. With awake_duration of only a few hundred ms,
	// an immediate I_REBOOT almost always fails with "no acknowledgement
	// received" because the node is already back asleep before the ACK
	// round-trip completes.
	if isSleepingNode(node) {
		p.queueRebootForNode(node)
		p.logger.Info("smart-sleep node, I_REBOOT queued until next wake",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.String("bootloaderKind", bootloaderKindForNode(node)),
			zap.Bool("explicitOptIn", explicitOptIn),
			zap.Bool("recentlySeen", isRecentlySeen(node, smartSleepRecentlySeen)),
			zap.String("awakeDuration", node.Others.GetString(FieldAwakeDuration)),
			zap.String("sleepDuration", node.Labels.Get(FieldSleepDuration)),
			zap.Duration("maxWait", maxSleepDuration(node)),
		)
		return nil
	}

	return p.sendRebootNow(node)
}

// sendRebootNow sends an I_REBOOT to the node immediately.
//
// [FIX n°3] Build the raw MySensors message directly and call Protocol.Write,
// bypassing p.Post() -> toRawMessage() -> handleActions() -> ActionReboot
// -> queueRebootIfSleeping(). That path re-queues the reboot instead of
// sending it, producing an infinite loop and "command type should not be
// empty" error.
func (p *Provider) sendRebootNow(node *nodeTY.Node) error {
	// MySensors raw format: node-id;child-sensor-id;command;ack;type;payload
	// I_REBOOT is: c=3 (C_INTERNAL), t=13 (I_REBOOT), no payload.
	// Child-sensor-id 255 = broadcast at sensor level.
	raw := fmt.Sprintf("%s;255;3;0;13;\n", node.NodeID)

	rawMsg := &msgTY.RawMessage{
		Timestamp: time.Now(),
		Data:      []byte(raw),
		ID:        fmt.Sprintf("%s-%s-reboot", node.GatewayID, node.NodeID),
	}
	rawMsg.Others = rawMsg.Others.Init()

	// MQTT protocol uses a topic instead of a raw payload.
	if p.ProtocolType == gwPtl.TypeMQTT {
		rawMsg.Data = []byte("255/3/0/13")
		rawMsg.Others.Set(gwPtl.KeyMqttTopic, []string{
			fmt.Sprintf("out_%s/%s/255/3/0/13", p.GatewayConfig.ID, node.NodeID),
		}, nil)
	}

	if err := p.Protocol.Write(rawMsg); err != nil {
		return fmt.Errorf("write reboot message: %w", err)
	}

	p.logger.Info("sent I_REBOOT before OTA",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("bootloaderKind", node.Labels.Get(LabelBootloaderKind)),
		zap.String("bootloaderVersion", node.Labels.Get(LabelBootloaderVersion)),
	)
	return nil
}

// queueRebootForNode stores a pending I_REBOOT for delivery when the node wakes.
func (p *Provider) queueRebootForNode(node *nodeTY.Node) {
	if node == nil {
		return
	}
	key := p.getNodeStoreID(node.GatewayID, node.NodeID)
	otaPendingRebootStore.Add(key, time.Now())
	p.logger.Info("reboot queued",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("key", key),
	)
}

// deliverQueuedRebootIfAny sends a pending I_REBOOT when the node is known to be awake.
// Safe to call multiple times; the entry is removed on first successful delivery.
//
// [FIX n°4] Use p.GatewayConfig.ID consistently. Earlier versions used
// msg.GatewayID which was often empty, producing mismatched store keys.
func (p *Provider) deliverQueuedRebootIfAny(gatewayID, nodeID string) {
	key := p.getNodeStoreID(gatewayID, nodeID)
	v := otaPendingRebootStore.Get(key)
	if v == nil {
		return
	}
	otaPendingRebootStore.Remove(key)

	node, err := p.getNode(gatewayID, nodeID)
	if err != nil {
		p.logger.Error("failed to load node for queued reboot delivery",
			zap.String("gatewayId", gatewayID),
			zap.String("nodeId", nodeID),
			zap.Error(err),
		)
		return
	}

	delay := otaRebootDeliveryDelay(node)

	p.logger.Info("delivering queued I_REBOOT to awake node",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.Duration("delay", delay),
		zap.String("awakeDuration", node.Others.GetString(FieldAwakeDuration)),
	)

	go func() {
		time.Sleep(delay)
		if err := p.sendRebootNow(node); err != nil {
			p.logger.Error("failed to deliver queued reboot",
				zap.String("nodeId", node.NodeID),
				zap.Error(err),
			)
		}
	}()
}

// pendingRebootCount returns the number of queued reboots (used by tests).
func pendingRebootCount() int {
	return len(otaPendingRebootStore.Keys())
}