package mysensors

import (
    "errors"
    "fmt"
    "strconv"
    "strings"
    "time"

    "github.com/mycontroller-org/server/v2/pkg/types"
    "github.com/mycontroller-org/server/v2/pkg/types/cmap"
    msgTY "github.com/mycontroller-org/server/v2/pkg/types/message"
    nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
    rsTY "github.com/mycontroller-org/server/v2/pkg/types/resource_service"
    busUtils "github.com/mycontroller-org/server/v2/pkg/utils/bus_utils"
    "github.com/mycontroller-org/server/v2/pkg/utils/bus_utils/query"
    "github.com/mycontroller-org/server/v2/pkg/utils/convertor"
    gwTY "github.com/mycontroller-org/server/v2/plugin/gateway/types"
    "go.uber.org/zap"
)

// handleActions routes globally-defined features (reboot, discover, etc.)
func (p *Provider) handleActions(gwCfg *gwTY.Config, fn string, msg *msgTY.Message, msMsg *message) error {
	switch fn {

	case gwTY.ActionDiscoverNodes:
		msMsg.Type = actionDiscoverRequest
		msMsg.Payload = payloadEmpty
		msMsg.NodeID = idBroadcast

	case nodeTY.ActionHeartbeatRequest:
		msMsg.Type = actionHeartBeatRequest
		msMsg.Payload = payloadEmpty

	case nodeTY.ActionReboot:
		// [FIX] For smart-sleep nodes, queue the reboot until the next wake.
		if p.queueRebootIfSleeping(msg) {
			return nil
		}
		msMsg.Type = actionReboot
		msMsg.Payload = payloadEmpty

	case nodeTY.ActionRefreshNodeInfo:
		msMsg.Type = actionRequestPresentation
		msMsg.Payload = payloadEmpty

	case nodeTY.ActionReset:
		err := p.updateResetFlag(msg)
		if err != nil {
			return err
		}
		if p.queueRebootIfSleeping(msg) {
			return nil
		}
		msMsg.Type = actionReboot
		msMsg.Payload = payloadEmpty

	case "I_CONFIG":
		msMsg.Type = actionConfig
		isImperial := gwCfg.Labels.GetBool(LabelImperialSystem)
		if isImperial {
			msMsg.Payload = "I"
		} else {
			msMsg.Payload = "M"
		}

	case "I_ID_REQUEST":
		msMsg.Type = actionIDResponse
		msMsg.Payload = p.getNodeID(gwCfg.ID)
		if msMsg.Payload == "" {
			return errors.New("error on getting node ID")
		}

	case "I_TIME":
		msMsg.Payload = p.getTimestamp(gwCfg)
		msMsg.Type = actionTime

	case nodeTY.ActionFirmwareUpdate:
		// Check if the node is sleeping BEFORE preparing the config.
		if node, err := p.getNode(msg.GatewayID, msg.NodeID); err == nil && isSleepingNode(node) {
			if err := p.maybeSendRebootForOta(msg); err != nil {
				p.logger.Error("firmware-update: pre-OTA reboot failed",
					zap.String("nodeId", msg.NodeID),
					zap.String("gatewayId", msg.GatewayID),
					zap.Error(err),
				)
			}
			p.logger.Info("firmware-update: config deferred until node wakes and reboots",
				zap.String("nodeId", node.NodeID),
				zap.String("gatewayId", node.GatewayID),
			)
			return nil
		}

		if err := p.maybeSendRebootForOta(msg); err != nil {
			p.logger.Error("firmware-update: pre-OTA reboot failed",
				zap.String("nodeId", msg.NodeID),
				zap.String("gatewayId", msg.GatewayID),
				zap.Error(err),
			)
		}
		pl, err := p.executeFirmwareConfigRequest(msg)
		if err != nil {
			return err
		}
		if pl == "" {
			return nil
		}
		msMsg.Command = cmdStream
		msMsg.Type = actionFirmwareConfigResponse
		msMsg.Payload = strings.ToUpper(pl)

	case "ST_FIRMWARE_CONFIG_REQUEST":
		pl, err := p.executeFirmwareConfigRequest(msg)
		if err != nil {
			return err
		}
		if pl == "" {
			return nil
		}
		msMsg.Command = cmdStream
		msMsg.Type = actionFirmwareConfigResponse
		msMsg.Payload = strings.ToUpper(pl)

	case "ST_FIRMWARE_REQUEST":
		pl, err := p.executeFirmwareRequest(msg)
		if err != nil {
			return err
		}
		if pl == "" {
			return nil
		}
		msMsg.Command = cmdStream
		msMsg.Type = actionFirmwareResponse
		msMsg.Payload = strings.ToUpper(pl)

	case nodeTY.ActionSetSleepDuration:
		raw := strings.TrimSpace(msg.Payloads[0].Value.String())
		sensorID, pct, err := parseSleepDurationPayload(raw)
		if err != nil {
			return err
		}
		node, err := p.getNode(msg.GatewayID, msg.NodeID)
		if err != nil {
			return err
		}
		if sensorID == "" {
			sensorID = node.Labels.Get(LabelSleepCommandSensorID)
		}
		if sensorID == "" {
			return fmt.Errorf("no sleep command sensor configured for node %s", node.NodeID)
		}
		if sensorID != node.Labels.Get(LabelSleepCommandSensorID) {
			node.Labels.Set(LabelSleepCommandSensorID, sensorID)
			p.setNodeLabels(node)
		}
		return p.sendSleepCommand(node, sensorID, pct, "")

	default:
		return fmt.Errorf("this function is not implemented: %s", fn)
	}
	return nil
}

// queueRebootIfSleeping checks whether the target node is a smart-sleep node.
// If so, it queues an I_REBOOT for delivery on the next wake and returns true.
func (p *Provider) queueRebootIfSleeping(msg *msgTY.Message) bool {
	if msg == nil {
		return false
	}
	node, err := p.getNode(msg.GatewayID, msg.NodeID)
	if err != nil {
		p.logger.Warn("queueRebootIfSleeping: cannot load node, fall back to immediate send",
			zap.String("gatewayId", msg.GatewayID),
			zap.String("nodeId", msg.NodeID),
			zap.Error(err),
		)
		return false
	}
	if !isSleepingNode(node) {
		return false
	}
	p.queueRebootForNode(node)
	p.logger.Info("smart-sleep node, manual I_REBOOT queued until next wake",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("awakeDuration", node.Others.GetString(FieldAwakeDuration)),
		zap.String("sleepDuration", node.Labels.Get(FieldSleepDuration)),
		zap.Duration("maxWait", maxSleepDuration(node)),
	)
	return true
}

// getTimestamp returns timestamp in seconds from 1970 with zone offset.
func (p *Provider) getTimestamp(gwCfg *gwTY.Config) string {
	var loc *time.Location
	tz := gwCfg.Labels.Get(types.LabelTimezone)
	if tz != "" {
		_loc, err := time.LoadLocation(tz)
		if err != nil {
			p.logger.Error("Failed to parse used defined timezone, fallback to system time zone", zap.String("userDefinedTimezone", tz))
			_loc = time.Now().Location()
		}
		loc = _loc
	}
	if loc == nil {
		loc = time.Now().Location()
	}
	_, offset := time.Now().In(loc).Zone()
	timestamp := time.Now().Unix() + int64(offset)
	return strconv.FormatInt(timestamp, 10)
}

// getNodeID returns the first available node id.
func (p *Provider) getNodeID(gatewayID string) string {
	var reservedIDsString []string
	updateNodeIDs := func(item interface{}) bool {
		ids, ok := item.(*[]string)
		if !ok {
			p.logger.Error("error on data conversion", zap.String("receivedType", fmt.Sprintf("%T", item)))
			return false
		}
		reservedIDsString = *ids
		return false
	}
	filter := map[string]interface{}{types.KeyGatewayID: gatewayID}
	out := make([]string, 0)
	err := query.QueryResource(p.logger, p.bus, "", rsTY.TypeNode, rsTY.CommandGetIds, filter, updateNodeIDs, &out, queryTimeout)
	if err != nil {
		p.logger.Error("error on finding list of nodes", zap.String("gatewayId", gatewayID), zap.Error(err))
		return ""
	}

	if reservedIDsString == nil {
		p.logger.Warn("there is no reserved ids found")
		return ""
	}

	reservedIDs := make([]int, len(reservedIDsString))
	for index, value := range reservedIDsString {
		reservedIDs[index] = int(convertor.ToInteger(value))
	}

	electedID := 1
	for id := 1; id <= 255; id++ {
		electedID = id
		found := false
		for _, rid := range reservedIDs {
			if rid == id {
				found = true
				break
			}
		}
		if !found {
			break
		}
	}

	if electedID == 255 {
		p.logger.Error("No space left on this network. Reached maximum node counts.", zap.String("gatewayId", gatewayID))
		return ""
	}
	return strconv.Itoa(electedID)
}

func (p *Provider) updateResetFlag(msg *msgTY.Message) error {
	node, err := p.getNode(msg.GatewayID, msg.NodeID)
	if err != nil {
		return err
	}
	var labels cmap.CustomStringMap
	labels = labels.Init()
	labels.Set(LabelEraseEEPROM, "true")
	busUtils.PostToResourceService(p.logger, p.bus, node.ID, labels, rsTY.TypeNode, rsTY.CommandSetLabel, "")
	return nil
}