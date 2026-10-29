package mysensors

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	msgTY "github.com/mycontroller-org/server/v2/pkg/types/message"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	"go.uber.org/zap"
)

const (
	sleepRecentlySeen       = 30 * time.Second
	sleepCommandVPercentage = "3"
	sleepCommandMaxAge      = 2 * time.Hour
)

// parseSleepDurationPayload extracts the optional sensor_id and the percentage.
func parseSleepDurationPayload(raw string) (sensorID string, percent uint8, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, fmt.Errorf("empty payload")
	}
	valueStr := raw
	if idx := strings.Index(raw, ":"); idx >= 0 {
		sensorID = strings.TrimSpace(raw[:idx])
		valueStr = strings.TrimSpace(raw[idx+1:])
	}
	pct, convErr := strconv.Atoi(valueStr)
	if convErr != nil || pct < 0 || pct > 100 {
		return "", 0, fmt.Errorf("invalid sleep percentage: %q (expected integer 0-100)", valueStr)
	}
	return sensorID, uint8(pct), nil
}

func (p *Provider) sendSleepCommand(node *nodeTY.Node, sensorID string, percent uint8, requestID string) error {
	if node == nil {
		return fmt.Errorf("nil node")
	}
	if sensorID == "" {
		return fmt.Errorf("sleep command sensor id is empty")
	}
	if percent > 100 {
		return fmt.Errorf("invalid sleep percentage: %d", percent)
	}
	if !isSleepingNode(node) {
		return fmt.Errorf("node %s is not a smart_sleep node", node.ID)
	}

	if isRecentlySeen(node, sleepRecentlySeen) {
		return p.sendSleepCommandNow(node, sensorID, percent)
	}

	p.queueSleepCommand(node, sensorID, percent, requestID)
	p.logger.Info("sleep command queued until next wake",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("sensorId", sensorID),
		zap.Uint8("percent", percent),
		zap.String("requestId", requestID),
		zap.String("sleepDuration", node.Labels.Get(FieldSleepDuration)),
		zap.Duration("maxWait", maxSleepDuration(node)),
	)
	return nil
}

func (p *Provider) sendSleepCommandNow(node *nodeTY.Node, sensorID string, percent uint8) error {
	sleepMsg := msgTY.NewMessage(false)
	sleepMsg.GatewayID = node.GatewayID
	sleepMsg.NodeID = node.NodeID
	sleepMsg.SourceID = sensorID
	sleepMsg.Type = msgTY.TypeSet

	pl := msgTY.NewPayload()
	pl.Key = "V_PERCENTAGE"
	pl.SetValue(fmt.Sprintf("%d", percent))
	pl.Labels.Set(LabelType, sleepCommandVPercentage)

	sleepMsg.Payloads = []msgTY.Payload{pl}

	if err := p.Post(&sleepMsg); err != nil {
		return fmt.Errorf("post sleep command: %w", err)
	}

	p.logger.Info("sent sleep command",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("sensorId", sensorID),
		zap.Uint8("percent", percent),
	)
	return nil
}

func (p *Provider) queueSleepCommand(node *nodeTY.Node, sensorID string, percent uint8, requestID string) {
	if node == nil {
		return
	}
	key := p.getNodeStoreID(node.GatewayID, node.NodeID)
	pendingSleepStore.Add(key, pendingSleepCommand{
		SensorID:  sensorID,
		Percent:   percent,
		QueuedAt:  time.Now(),
		RequestID: requestID,
	})
}

func (p *Provider) deliverQueuedSleepCommandIfAny(gatewayID, nodeID string) {
	key := p.getNodeStoreID(gatewayID, nodeID)
	v := pendingSleepStore.Get(key)
	if v == nil {
		return
	}
	cmd, ok := v.(pendingSleepCommand)
	if !ok {
		pendingSleepStore.Remove(key)
		return
	}
	if time.Since(cmd.QueuedAt) > sleepCommandMaxAge {
		pendingSleepStore.Remove(key)
		p.logger.Warn("pending sleep command expired",
			zap.String("gatewayId", gatewayID),
			zap.String("nodeId", nodeID),
		)
		return
	}
	pendingSleepStore.Remove(key)

	node, err := p.getNode(gatewayID, nodeID)
	if err != nil {
		p.logger.Error("failed to load node for queued sleep command",
			zap.String("gatewayId", gatewayID),
			zap.String("nodeId", nodeID),
			zap.Error(err),
		)
		return
	}

	p.logger.Info("delivering queued sleep command to awake node",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("sensorId", cmd.SensorID),
		zap.Uint8("percent", cmd.Percent),
	)

	go func() {
		time.Sleep(500 * time.Millisecond)
		if err := p.sendSleepCommandNow(node, cmd.SensorID, cmd.Percent); err != nil {
			p.logger.Error("failed to deliver queued sleep command",
				zap.String("nodeId", node.NodeID),
				zap.Error(err),
			)
		}
	}()
}

func pendingSleepCount() int {
	return len(pendingSleepStore.Keys())
}

func purgeExpiredSleepCommands() {
	now := time.Now()
	for _, key := range pendingSleepStore.Keys() {
		v := pendingSleepStore.Get(key)
		cmd, ok := v.(pendingSleepCommand)
		if !ok || now.Sub(cmd.QueuedAt) > sleepCommandMaxAge {
			pendingSleepStore.Remove(key)
		}
	}
}
