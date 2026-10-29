// OTA strategy for the mysensors_v2 provider.
//
// The fota_update node label drives the OTA strategy:
//
//	offline  The bootloader handles OTA. The controller sends I_REBOOT
//	         (immediately for an awake node, queued for a smart-sleep
//	         node). The bootloader restarts, emits
//	         ST_FIRMWARE_CONFIG_REQUEST, and the controller serves the
//	         blocks.
//
//	online   The sketch handles OTA (MY_OTA_FIRMWARE_FEATURE). The
//	         controller sends ST_FIRMWARE_CONFIG_RESPONSE directly to the
//	         sketch, which compares the version and starts its own OTA
//	         when it differs. No reboot is involved.
//
//	unknown  Passive. The controller only answers ST_FIRMWARE_* requests
//	         that arrive spontaneously.
//
// Smart-sleep nodes always queue the action and deliver it on the next
// wake (I_PRE_SLEEP_NOTIFICATION), with a short delay controlled by
// fota_delivery_delay.
package mysensors

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	msgTY "github.com/mycontroller-org/server/v2/pkg/types/message"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	gwPtl "github.com/mycontroller-org/server/v2/plugin/gateway/protocol"
	"go.uber.org/zap"
)

// smartSleepRecentlySeen is the window during which a smart-sleep node is
// considered "awake". Below this threshold, we assume it can receive messages.
const smartSleepRecentlySeen = 30 * time.Second

// fotaDeliveryDelayDefault is the delay between a wake signal and the
// delivery of the queued OTA action. Must stay strictly below the node's
// awake_duration. Default: 200 ms. Overridable per node with fota_delivery_delay.
const fotaDeliveryDelayDefault = 200 * time.Millisecond

// getFotaUpdateMode returns the normalised fota_update value for a node.
// Absent, empty or unrecognised values fall back to FotaUpdateUnknown
// (fail-safe: the controller does nothing proactive).
func getFotaUpdateMode(node *nodeTY.Node) string {
	if node == nil || node.Labels == nil {
		return FotaUpdateUnknown
	}
	switch strings.ToLower(strings.TrimSpace(node.Labels.Get(LabelFotaUpdate))) {
	case FotaUpdateOnline:
		return FotaUpdateOnline
	case FotaUpdateOffline:
		return FotaUpdateOffline
	default:
		return FotaUpdateUnknown
	}
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

// otaDeliveryDelay returns the delay for a given node.
// Reads fota_delivery_delay; falls back to the deprecated
// bootloader_ota_reboot_delay for migration.
//
// Absent label  -> fotaDeliveryDelayDefault (200 ms)
// Present "0"   -> 0 (no delay, caller's explicit choice)
// Present < 50  -> clamped to 50 ms
// Present > 5000 -> clamped to 5 s
func otaDeliveryDelay(node *nodeTY.Node) time.Duration {
	if node == nil || node.Labels == nil {
		return fotaDeliveryDelayDefault
	}

	raw, present := readDelayLabel(node, LabelFotaDeliveryDelay)
	if !present {
		raw, present = readDelayLabel(node, "bootloader_ota_reboot_delay")
	}
	if !present {
		return fotaDeliveryDelayDefault
	}

	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v < 0 {
		return fotaDeliveryDelayDefault
	}
	if v == 0 {
		// explicit "0" means no delay
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

// readDelayLabel returns the raw string value and whether the key is
// present in the labels map.
func readDelayLabel(node *nodeTY.Node, key string) (string, bool) {
	if node == nil || node.Labels == nil {
		return "", false
	}
	v := node.Labels.Get(key)
	if v == "" {
		return "", false
	}
	return v, true
}

// maybeTriggerOta is called before serving a firmware-update action.
// It routes the OTA action according to fota_update:
//
//   - offline: send I_REBOOT now (awake) or queue it (sleeping)
//   - online:  send ST_FIRMWARE_CONFIG_RESPONSE now (awake) or queue it
//     (sleeping). The sketch (with MY_OTA_FIRMWARE_FEATURE) listens
//     for this message and starts its OTA when the version differs.
//   - unknown: do nothing (passive; only answer ST_FIRMWARE_* requests)
//
// The node's fota_update influences the routing. smart_sleep_node influences
// only the delivery path (immediate vs queued).
func (p *Provider) maybeTriggerOta(msg *msgTY.Message) error {
	node, err := p.getNode(msg.GatewayID, msg.NodeID)
	if err != nil {
		return fmt.Errorf("load node for OTA trigger: %w", err)
	}

	if isFotaDisabled(node) {
		p.logger.Debug("no OTA trigger: fota_disabled",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
		)
		return nil
	}

	mode := getFotaUpdateMode(node)
	if mode == FotaUpdateUnknown {
		p.logger.Info("no OTA trigger: fota_update=unknown (passive)",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
		)
		return nil
	}

	// Smart-sleep nodes are always queued, whatever the mode. With awake_duration
	// of only a few hundred ms, an immediate write almost always fails with
	// "no acknowledgement received" because the node is already back asleep
	// before the ACK round-trip completes.
	if isSleepingNode(node) {
		p.queueFotaActionForNode(node, mode)
		p.logger.Info("fota action queued until next wake",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.String("fotaUpdate", mode),
			zap.Bool("recentlySeen", isRecentlySeen(node, smartSleepRecentlySeen)),
			zap.String("awakeDuration", node.Others.GetString(FieldAwakeDuration)),
			zap.String("sleepDuration", node.Labels.Get(FieldSleepDuration)),
			zap.Duration("maxWait", maxSleepDuration(node)),
		)
		return nil
	}

	switch mode {
	case FotaUpdateOffline:
		return p.sendRebootNow(node)
	case FotaUpdateOnline:
		return p.sendFirmwareConfigResponseNow(node)
	}
	return nil
}

// sendRebootNow sends an I_REBOOT to the node immediately (offline mode).
//
// Build the raw MySensors message directly, bypassing p.Post() ->
// toRawMessage() -> handleActions() -> ActionReboot -> queueRebootIfSleeping()
// (that path re-queues the reboot instead of sending it, producing an
// infinite loop and "command type should not be empty" error).
//
// However, we DO want the same ACK + retry behaviour as p.Post() for
// reliable delivery. Build the raw message here and call writeWithAck().
func (p *Provider) sendRebootNow(node *nodeTY.Node) error {
	msMsg := &message{
		NodeID:   node.NodeID,
		SensorID: idBroadcast,
		Command:  cmdInternal,
		Ack:      "0",
		Type:     actionReboot,
		Payload:  payloadEmpty,
	}

	rawMsg := &msgTY.RawMessage{
		Timestamp: time.Now(),
		ID:        fmt.Sprintf("%s-%s-reboot", node.GatewayID, node.NodeID),
	}
	rawMsg.Others = rawMsg.Others.Init()

	switch p.ProtocolType {
	case gwPtl.TypeSerial, gwPtl.TypeEthernet:
		rawMsg.Data = []byte(msMsg.toMySensorsRaw(false))
	case gwPtl.TypeMQTT:
		rawMsg.Data = []byte(msMsg.Payload)
		rawMsg.Others.Set(gwPtl.KeyMqttTopic, []string{msMsg.toMySensorsRaw(true)}, nil)
	default:
		return fmt.Errorf("protocol type not implemented: %s", p.ProtocolType)
	}

	if err := p.writeWithAck(rawMsg); err != nil {
		return fmt.Errorf("write reboot message: %w", err)
	}

	p.logger.Info("sent I_REBOOT before OTA (fota_update=offline)",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("bootloaderKind", node.Labels.Get(LabelBootloaderKind)),
		zap.String("bootloaderVersion", node.Labels.Get(LabelBootloaderVersion)),
	)
	return nil
}

// sendFirmwareConfigResponseNow sends the ST_FIRMWARE_CONFIG_RESPONSE
// directly to the node (online mode). The sketch (with
// MY_OTA_FIRMWARE_FEATURE) listens for this message, compares the version
// and starts its own OTA when it differs.
//
// This restores the base-development behaviour: the controller does not
// wait for the sketch to emit ST_FIRMWARE_CONFIG_REQUEST, it pushes the
// response directly. No reboot is involved.
func (p *Provider) sendFirmwareConfigResponseNow(node *nodeTY.Node) error {
	// Compute the response payload exactly like the ST_FIRMWARE_CONFIG_REQUEST
	// handler does. We call executeFirmwareConfigRequest with an empty payload
	// (equivalent to what the UI sends today) and use the returned hex.
	//
	// The function signature expects a *msgTY.Message with at least one
	// payload. We build a synthetic one so we don't have to duplicate the
	// logic.
	synthetic := msgTY.NewMessage(false)
	synthetic.GatewayID = node.GatewayID
	synthetic.NodeID = node.NodeID
	pl := msgTY.NewPayload()
	pl.SetValue("")
	synthetic.Payloads = []msgTY.Payload{pl}

	hexPayload, err := p.executeFirmwareConfigRequest(&synthetic)
	if err != nil {
		return fmt.Errorf("compute firmware config response: %w", err)
	}
	if hexPayload == "" {
		p.logger.Info("fota_update=online: no firmware to advertise, nothing sent",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
		)
		return nil
	}

	msMsg := &message{
		NodeID:   node.NodeID,
		SensorID: idBroadcast,
		Command:  cmdStream,
		Ack:      "0",
		Type:     actionFirmwareConfigResponse,
		Payload:  strings.ToUpper(hexPayload),
	}

	rawMsg := &msgTY.RawMessage{
		Timestamp: time.Now(),
		ID:        fmt.Sprintf("%s-%s-config-response", node.GatewayID, node.NodeID),
	}
	rawMsg.Others = rawMsg.Others.Init()

	switch p.ProtocolType {
	case gwPtl.TypeSerial, gwPtl.TypeEthernet:
		rawMsg.Data = []byte(msMsg.toMySensorsRaw(false))
	case gwPtl.TypeMQTT:
		rawMsg.Data = []byte(msMsg.Payload)
		rawMsg.Others.Set(gwPtl.KeyMqttTopic, []string{msMsg.toMySensorsRaw(true)}, nil)
	default:
		return fmt.Errorf("protocol type not implemented: %s", p.ProtocolType)
	}

	if err := p.writeWithAck(rawMsg); err != nil {
		return fmt.Errorf("write firmware config response: %w", err)
	}

	p.logger.Info("sent ST_FIRMWARE_CONFIG_RESPONSE for online OTA",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("bootloaderKind", node.Labels.Get(LabelBootloaderKind)),
	)
	return nil
}

// queueFotaActionForNode stores a pending OTA action for delivery when the
// node wakes up.
func (p *Provider) queueFotaActionForNode(node *nodeTY.Node, mode string) {
	if node == nil {
		return
	}
	key := p.getNodeStoreID(node.GatewayID, node.NodeID)
	otaPendingActionStore.Add(key, pendingFotaAction{
		Mode:     mode,
		QueuedAt: time.Now(),
	})
	p.logger.Info("fota action queued",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("fotaUpdate", mode),
		zap.String("key", key),
	)
}

// deliverQueuedFotaActionIfAny sends the pending OTA action when the node is
// known to be awake. Safe to call multiple times; the entry is removed ONLY
// on successful delivery.
//
// The pending entry is kept on failure so the next inbound message retries
// delivery. Expired entries (> fotaCommandMaxAge) are dropped.
func (p *Provider) deliverQueuedFotaActionIfAny(gatewayID, nodeID string) {
	key := p.getNodeStoreID(gatewayID, nodeID)
	v := otaPendingActionStore.Get(key)
	if v == nil {
		return
	}
	action, ok := v.(pendingFotaAction)
	if !ok {
		otaPendingActionStore.Remove(key)
		return
	}
	if time.Since(action.QueuedAt) > fotaCommandMaxAge {
		otaPendingActionStore.Remove(key)
		p.logger.Warn("pending fota action expired",
			zap.String("gatewayId", gatewayID),
			zap.String("nodeId", nodeID),
			zap.String("fotaUpdate", action.Mode),
		)
		return
	}

	node, err := p.getNode(gatewayID, nodeID)
	if err != nil {
		p.logger.Error("failed to load node for queued fota action delivery",
			zap.String("gatewayId", gatewayID),
			zap.String("nodeId", nodeID),
			zap.Error(err),
		)
		return // keep queued, retry on next wake
	}

	delay := otaDeliveryDelay(node)

	p.logger.Info("delivering queued fota action to awake node",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("fotaUpdate", action.Mode),
		zap.Duration("delay", delay),
		zap.String("awakeDuration", node.Others.GetString(FieldAwakeDuration)),
	)

	go func() {
		time.Sleep(delay)
		var sendErr error
		switch action.Mode {
		case FotaUpdateOffline:
			sendErr = p.sendRebootNow(node)
		case FotaUpdateOnline:
			sendErr = p.sendFirmwareConfigResponseNow(node)
		default:
			sendErr = nil
		}
		if sendErr != nil {
			p.logger.Error("failed to deliver queued fota action, keeping queued for retry",
				zap.String("nodeId", node.NodeID),
				zap.String("gatewayId", node.GatewayID),
				zap.String("fotaUpdate", action.Mode),
				zap.Error(sendErr),
			)
			return // keep queued
		}
		otaPendingActionStore.Remove(key)
	}()
}

// purgeExpiredFotaActions drops expired queued OTA actions. Called by the
// scheduler registered in Provider.Start.
func (p *Provider) purgeExpiredFotaActions() {
	now := time.Now()
	for _, key := range otaPendingActionStore.Keys() {
		v := otaPendingActionStore.Get(key)
		action, ok := v.(pendingFotaAction)
		if !ok || now.Sub(action.QueuedAt) > fotaCommandMaxAge {
			otaPendingActionStore.Remove(key)
		}
	}
}

// pendingFotaActionCount returns the number of queued OTA actions (used by tests).
func pendingFotaActionCount() int {
	return len(otaPendingActionStore.Keys())
}
