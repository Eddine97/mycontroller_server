package mysensors

import (
	"bytes"
	"encoding/binary"
	hexENC "encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mycontroller-org/server/v2/pkg/types"
	msgTY "github.com/mycontroller-org/server/v2/pkg/types/message"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	rsTY "github.com/mycontroller-org/server/v2/pkg/types/resource_service"
	busUtils "github.com/mycontroller-org/server/v2/pkg/utils/bus_utils"
	converterUtils "github.com/mycontroller-org/server/v2/pkg/utils/convertor"
	"go.uber.org/zap"
)

// executeFirmwareConfigRequest executes firmware config request and response with hex payload
func (p *Provider) executeFirmwareConfigRequest(msg *msgTY.Message) (string, error) {
	startTime := time.Now()
	rxPL := sanitizeOtaHex(msg.Payloads[0].Value.String())

	// convert the received hex to matching struct format (first 10 bytes when present)
	fwCfgReq := &firmwareConfigRequest{}
	if rxPL != "" {
		err := toStruct(rxPL, fwCfgReq)
		if err != nil {
			p.logger.Error("error on converting firmwareConfigRequest", zap.String("payload", rxPL), zap.Error(err))
			return "", err
		}
	}

	node, err := p.getNode(msg.GatewayID, msg.NodeID)
	if err != nil {
		p.logger.Error("error to get node details", zap.Any("msg", msg), zap.Error(err))
		return "", err
	}

	// Detect and persist bootloader kind
	if rxPL != "" {
		p.rememberBootloaderKind(node, rxPL)
	}

	fromReq := otaBlockSizeFromRequest(rxPL)
	if fromReq > 0 {
		p.rememberOtaBlockSize(node, fromReq)
	}

	if isFotaDisabled(node) {
		return p.respondFotaDisabled(node, rxPL, fwCfgReq, startTime)
	}

	if hasFotaScript(node) {
		scriptDisabled, err := p.isFotaScriptDisabled(node)
		if err != nil {
			p.logger.Error("fota script lookup failed", zap.String("nodeId", node.ID), zap.Error(err))
			return "", err
		}
		if scriptDisabled {
			return p.respondFotaScriptInactive(node, rxPL, fwCfgReq, "fota script disabled in data repository")
		}
		scriptRes, err := p.runFotaOnConfig(node, rxPL)
		if err != nil {
			p.logger.Error("fota onConfig failed", zap.String("nodeId", node.ID), zap.Error(err))
			return "", err
		}
		if node.Labels.GetBool(LabelEraseEEPROM) {
			return p.buildFirmwareConfigResponse(node, &firmwareRaw{}, startTime, "")
		}
		if scriptRes.NoUpdate {
			p.logFotaNoUpdate(node, rxPL, fwCfgReq, scriptRes, startTime)
			// UI firmware_update has no CONFIG_REQUEST bytes. Do not invent a
			// type/version/CRC; a mismatch would start an unwanted OTA.
			if rxPL == "" {
				return "", nil
			}
			return p.toHex(&firmwareConfigResponse{
				Type:    fwCfgReq.Type,
				Version: fwCfgReq.Version,
				Blocks:  fwCfgReq.Blocks,
				CRC:     fwCfgReq.CRC,
			})
		}
		if scriptRes.ResponseHex != "" {
			p.logger.Debug("sending firmware config response from fota script",
				zap.String("nodeId", node.ID),
				zap.String("fotaScript", getFotaScriptID(node)),
				zap.String("timeTaken", time.Since(startTime).String()),
			)
			return scriptRes.ResponseHex, nil
		}
		blockSize, err := p.resolveOtaBlockSize(node, rxPL, fromReq, scriptRes.BlockSize)
		if err != nil {
			p.logger.Error("cannot advertise firmware: unknown OTA block size",
				zap.String("nodeId", node.ID),
				zap.String("payload", rxPL),
				zap.Int("parsedBlockSize", fromReq),
				zap.Int("scriptBlockSize", scriptRes.BlockSize),
				zap.Int("labelBlockSize", p.otaBlockSizeFromLabel(node)),
				zap.Error(err),
			)
			return "", err
		}
		fwRaw, err := p.fetchFirmwareByID(scriptRes.FirmwareID, fwCfgReq.Type, fwCfgReq.Version, false, blockSize)
		if err != nil {
			p.logger.Error("error to get firmware from fota onConfig", zap.String("firmwareId", scriptRes.FirmwareID), zap.Error(err))
			return "", err
		}
		fwRaw.LastAccess = time.Now()
		p.rememberFotaSession(node, scriptRes.FirmwareID, fwRaw.BlockSize)
		return p.buildFirmwareConfigResponse(node, fwRaw, startTime, scriptRes.FirmwareID)
	}

	// Stock path: assigned_firmware label (only when fota_script is not set)
	blockSize, err := p.resolveOtaBlockSize(node, rxPL, fromReq, 0)
	if err != nil {
		p.logger.Error("cannot advertise firmware: unknown OTA block size",
			zap.String("nodeId", node.ID),
			zap.String("payload", rxPL),
			zap.Int("parsedBlockSize", fromReq),
			zap.Error(err),
		)
		return "", err
	}
	fwRaw, err := p.fetchFirmware(node, fwCfgReq.Type, fwCfgReq.Version, false, blockSize)
	if err != nil {
		p.logger.Error("error to get firmware", zap.Any("fwCfgReq", fwCfgReq), zap.Error(err))
		return "", err
	}
	fwRaw.LastAccess = time.Now()
	return p.buildFirmwareConfigResponse(node, fwRaw, startTime, node.Labels.Get(types.LabelNodeAssignedFirmware))
}

func (p *Provider) logFotaNoUpdate(node *nodeTY.Node, rxPL string, req *firmwareConfigRequest, scriptRes *fotaScriptResult, startTime time.Time) {
	if p == nil || p.logger == nil {
		return
	}
	fields := []zap.Field{
		zap.String("nodeId", node.ID),
		zap.String("gatewayId", node.GatewayID),
		zap.String("msNodeId", node.NodeID),
		zap.String("fotaScript", getFotaScriptID(node)),
		zap.String("timeTaken", time.Since(startTime).String()),
		zap.String("payload", rxPL),
		zap.Any("request", fotaRequestForLog(rxPL)),
	}
	if req != nil {
		fields = append(fields,
			zap.Uint16("type", req.Type),
			zap.Uint16("version", req.Version),
			zap.Uint16("blocks", req.Blocks),
			zap.Uint16("crc", req.CRC),
			zap.String("crcHex", fmt.Sprintf("%04X", req.CRC)),
		)
	}
	if scriptRes != nil {
		if scriptRes.FirmwareID != "" {
			fields = append(fields, zap.String("firmwareId", scriptRes.FirmwareID))
		}
		if scriptRes.BlockSize > 0 {
			fields = append(fields, zap.Int("blockSize", scriptRes.BlockSize))
		}
		if len(scriptRes.Labels) > 0 {
			fields = append(fields, zap.Any("labels", scriptRes.Labels))
		}
	}
	p.logger.Debug("fota onConfig: no update", fields...)
}

func (p *Provider) respondFotaDisabled(node *nodeTY.Node, rxPL string, req *firmwareConfigRequest, startTime time.Time) (string, error) {
	p.logger.Debug("FOTA disabled for node",
		zap.String("nodeId", node.ID),
		zap.String("msNodeId", node.NodeID),
		zap.String("timeTaken", time.Since(startTime).String()),
	)
	return p.echoFirmwareConfigOrSkip(rxPL, req)
}

func (p *Provider) respondFotaScriptInactive(node *nodeTY.Node, rxPL string, req *firmwareConfigRequest, reason string) (string, error) {
	p.logger.Debug(reason,
		zap.String("nodeId", node.ID),
		zap.String("fotaScript", getFotaScriptID(node)),
	)
	return p.echoFirmwareConfigOrSkip(rxPL, req)
}

func (p *Provider) echoFirmwareConfigOrSkip(rxPL string, req *firmwareConfigRequest) (string, error) {
	if rxPL == "" || req == nil {
		return "", nil
	}
	return p.toHex(&firmwareConfigResponse{
		Type:    req.Type,
		Version: req.Version,
		Blocks:  req.Blocks,
		CRC:     req.CRC,
	})
}

// fotaRequestForLog is parseFotaRequest without raw []byte (JSON would be base64).
func fotaRequestForLog(rxPL string) map[string]interface{} {
	parsed := parseFotaRequest(rxPL, true)
	out := make(map[string]interface{}, len(parsed))
	for k, v := range parsed {
		if _, isBytes := v.([]byte); isBytes {
			continue
		}
		out[k] = v
	}
	return out
}

func (p *Provider) buildFirmwareConfigResponse(node *nodeTY.Node, fwRaw *firmwareRaw, startTime time.Time, fwID string) (string, error) {
	fwCfgRes := &firmwareConfigResponse{}

	// if erase eeprom set for this node, update erase eeprom command and clear the label on the node detail
	if node.Labels.GetBool(LabelEraseEEPROM) {
		p.logger.Debug("erase EEPROM enabled, sending erase EEPROM command to the node", zap.String("nodeId", node.ID))
		fwCfgRes.SetEraseEEPROM()
		node.Labels.Set(LabelEraseEEPROM, "false")
		p.setNodeLabels(node)
	} else {
		fwCfgRes.Type = fwRaw.Type
		fwCfgRes.Version = fwRaw.Version
		fwCfgRes.Blocks = fwRaw.Blocks
		fwCfgRes.CRC = fwRaw.CRC

		// OTA-COMPLETION: normalize versions before comparing.
		// The node label stores "1.31"; the firmware stores 31 (0x001F).
		// Compare the numeric part only, otherwise every CONFIG_REQUEST
		// is treated as a new update and the OTA state is re-armed forever.
		currentVersionNum := 0
		if v := node.Labels.Get("version"); v != "" {
			if idx := strings.LastIndex(v, "."); idx >= 0 {
				currentVersionNum, _ = strconv.Atoi(v[idx+1:])
			} else {
				currentVersionNum, _ = strconv.Atoi(v)
			}
		}
		targetVersionNum := int(fwRaw.Version)

		// OTA-COMPLETION: this function NO LONGER arms ota_running.
		//
		// Rationale:
		//   - A CONFIG_REQUEST is sent by the node both when it enters
		//     bootloader mode for a real update AND when it reboots after a
		//     successful OTA to confirm the running version.
		//   - Arming ota_running here caused a phantom OTA to re-arm itself
		//     on every post-OTA reboot (version already equal, no block ever
		//     served, yet progress reset to 0 and end_time cleared).
		//   - The OTA state must be armed when the node ACTUALLY starts
		//     requesting data blocks, i.e. on the first ST_FIRMWARE_REQUEST
		//     served by computeOtaProgressState()/updateFirmwareProgressStatus().
		//
		// We still memorize the target so forceOtaCompletion() can decide
		// success later (on I_SKETCH_VERSION, reboot, sleep, or timeout),
		// and we close a phantom OTA if it is somehow still running.
		if currentVersionNum != targetVersionNum {
			// Memorize the target version/CRC/type for OTA success detection.
			// Do NOT touch FieldOTARunning or FieldOTABlockTotal here.
			node.Others.Set(FieldOTATargetVersion, fmt.Sprintf("%d", fwRaw.Version), nil)
			node.Others.Set(FieldOTATargetCRC, fmt.Sprintf("%04X", fwRaw.CRC), nil)
			node.Others.Set(FieldOTATargetType, fmt.Sprintf("%d", fwRaw.Type), nil)
		} else if node.Others.GetBool(types.FieldOTARunning) {
			// OTA-COMPLETION: versions are equal but ota_running is still
			// true. This means a previous CONFIG_REQUEST armed the OTA (or a
			// forceOtaCompletion event was lost), and the node is already on
			// the target version. Close it as successful.
			//
			// With the new design (no arming in this function), this branch
			// should only trigger in pathological cases:
			//   - a real OTA is in progress and the node coincidentally
			//     reported its new version before the last block status;
			//   - or the OTA was armed by a first served block, then the node
			//     rebooted and re-sent CONFIG_REQUEST with the new version.
			// In both cases, closing as success is the right call.
			now := time.Now()
			timeTaken := int(converterUtils.ToInteger(node.Others[types.FieldOTATimeTaken]))
			timeTakenStr := converterUtils.ToString(node.Others[types.FieldOTATimeTakenStr])

			var startTimeVal interface{} = now
			if timeTaken > 0 {
				if v := node.Others[types.FieldOTAStartTime]; v != nil {
					startTimeVal = v
				}
			}

			state := map[string]interface{}{
				types.FieldOTARunning:      false,
				types.FieldOTAProgress:     100,
				types.FieldOTAStartTime:    startTimeVal,
				types.FieldOTAEndTime:      now,
				types.FieldOTATimeTaken:    timeTaken,
				types.FieldOTATimeTakenStr: timeTakenStr,
				types.FieldOTABlockNumber:  nil, // clear the current block on close
			}
			busUtils.PostToResourceService(p.logger, p.bus, node.ID, state, rsTY.TypeNode, rsTY.CommandFirmwareState, "")

			node.Others.Set(types.FieldOTARunning, false, nil)
			node.Others.Set(types.FieldOTAProgress, 100, nil)
			node.Others.Set(types.FieldOTAStartTime, startTimeVal, nil)
			node.Others.Set(types.FieldOTAEndTime, now, nil)
			node.Others.Set(types.FieldOTATimeTaken, timeTaken, nil)
			node.Others.Set(types.FieldOTATimeTakenStr, timeTakenStr, nil)
			node.Others.Set(types.FieldOTABlockNumber, nil, nil) // clear the current block on close

			p.logger.Info("OTA state cleared (node already on target version)",
				zap.String("nodeId", node.NodeID),
				zap.String("gatewayId", node.GatewayID),
				zap.Int("currentVersion", currentVersionNum),
				zap.Int("targetVersion", targetVersionNum),
				zap.Int("timeTaken", timeTaken),
			)
		}
	}

	p.logger.Debug("sending a firmware config response",
		zap.Any("response", fwCfgRes),
		zap.Int("blockSize", fwRaw.BlockSize),
		zap.String("firmwareId", fwID),
		zap.String("fotaScript", getFotaScriptID(node)),
		zap.String("timeTaken", time.Since(startTime).String()),
	)

	return p.toHex(fwCfgRes)
}

// formatDuration converts a duration in seconds to a compact string,
// e.g. 45 -> "45s", 103 -> "1m43s", 3725 -> "1h2m5s".
// Used by forceOtaCompletion to compute the real duration when the node
// rebooted before reporting the final status.
func formatDuration(seconds int) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%dm%ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh%dm%ds", seconds/3600, (seconds%3600)/60, seconds%60)
}

// OTA-COMPLETION: forceOtaCompletion publishes a final OTA state.
// Called when a reboot signal, version change, sleep notification or timeout
// indicates that the OTA is over. Success is decided by comparing the
// current node version with the target memorized at setup, unless the caller
// already knows the outcome.
func (p *Provider) forceOtaCompletion(node *nodeTY.Node, success bool, reason string) {
	if node == nil || node.Others == nil {
		return
	}
	if !node.Others.GetBool(types.FieldOTARunning) {
		return
	}

	// Cross-check: if the node already reports the target version, it's a success.
	if targetVersion := node.Others.GetString(FieldOTATargetVersion); targetVersion != "" {
		if node.Labels.Get("version") == targetVersion {
			success = true
		}
	}

	progress := 0
	if success {
		progress = 100
	}

	now := time.Now()

	// OTA-COMPLETION: recover the real duration.
	//
	// Case 1: the node reported a final status before rebooting.
	//         ota_time_taken > 0 and ota_time_taken_str is set.
	//         Keep them as-is.
	//
	// Case 2: the node rebooted or fell asleep before the last status was
	//         published. ota_time_taken is 0, but ota_start_time was set
	//         when the first block was received (and preserved across sleep
	//         cycles). Compute the duration from ota_start_time to now.
	//
	// Case 3: phantom OTA (no block ever served).
	//         ota_start_time is nil, ota_time_taken is 0.
	//         Set start = end = now so the UI renders "0s".
	timeTaken := int(converterUtils.ToInteger(node.Others[types.FieldOTATimeTaken]))
	timeTakenStr := converterUtils.ToString(node.Others[types.FieldOTATimeTakenStr])

	startVal := node.Others[types.FieldOTAStartTime]
	var startTime interface{} = now
	startTimeParsed := time.Time{}

	switch v := startVal.(type) {
	case time.Time:
		if !v.IsZero() {
			startTimeParsed = v
			startTime = v
		}
	case string:
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil && !t.IsZero() {
			startTimeParsed = t
			startTime = t
		}
	case float64:
		if v > 0 {
			t := time.Unix(int64(v), 0)
			startTimeParsed = t
			startTime = t
		}
	}

	// If the node didn't report a final duration, compute it from start_time.
	// This handles smart-sleep nodes that fall asleep before the last status
	// and reboot nodes that report the version before the final block status.
	if timeTaken <= 0 && !startTimeParsed.IsZero() {
		elapsed := int(now.Sub(startTimeParsed).Seconds())
		if elapsed > 0 {
			timeTaken = elapsed
			timeTakenStr = formatDuration(elapsed)
		}
	}

	// OTA-COMPLETION: preserve the start time if it was already recorded.
	// forceOtaCompletion may be called several times (reboot, sleep, version
	// change). The first call may happen before the node reports the final
	// status, and we must not overwrite the real start time with "now" in
	// subsequent calls, otherwise the duration computed above collapses to 0.
	// If startTimeParsed is zero (phantom OTA), set start = end = now.
	finalStartTime := startTime
	if startTimeParsed.IsZero() {
		finalStartTime = now
	}

	// Update the local cache immediately so that a subsequent CONFIG_REQUEST
	// in the same loop sees the cleared state, even if the bus event is
	// delayed or lost.
	node.Others.Set(types.FieldOTARunning, false, nil)
	node.Others.Set(types.FieldOTAProgress, progress, nil)
	node.Others.Set(types.FieldOTAStartTime, finalStartTime, nil)
	node.Others.Set(types.FieldOTAEndTime, now, nil)
	node.Others.Set(types.FieldOTATimeTaken, timeTaken, nil)
	node.Others.Set(types.FieldOTATimeTakenStr, timeTakenStr, nil)
	node.Others.Set(types.FieldOTABlockNumber, nil, nil) // clear the current block on close

	state := map[string]interface{}{
		types.FieldOTARunning:      false,
		types.FieldOTAProgress:     progress,
		types.FieldOTAStartTime:    finalStartTime,
		types.FieldOTAEndTime:      now,
		types.FieldOTATimeTaken:    timeTaken,
		types.FieldOTATimeTakenStr: timeTakenStr,
		types.FieldOTABlockNumber:  nil, // clear the current block on close
	}
	busUtils.PostToResourceService(p.logger, p.bus, node.ID, state, rsTY.TypeNode, rsTY.CommandFirmwareState, "")

	// OTA-COMPLETION: verify persistence. If the bus event did not reach
	// the storage service, ota_running will still be true on the next read.
	// Logging it here makes the failure visible instead of silent.
	if n, err := p.getNode(node.GatewayID, node.NodeID); err == nil {
		persisted := !n.Others.GetBool(types.FieldOTARunning)
		if !persisted {
			p.logger.Warn("OTA state not persisted after post",
				zap.String("nodeId", node.NodeID),
				zap.String("gatewayId", node.GatewayID),
				zap.Bool("expectedRunning", false),
				zap.Any("others", n.Others),
			)
		}
	} else {
		p.logger.Warn("OTA state verification failed",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.Error(err),
		)
	}

	p.logger.Info("OTA completion forced",
		zap.String("nodeId", node.NodeID),
		zap.String("gatewayId", node.GatewayID),
		zap.Bool("success", success),
		zap.String("reason", reason),
		zap.String("targetVersion", node.Others.GetString(FieldOTATargetVersion)),
		zap.String("currentVersion", node.Labels.Get("version")),
		zap.Int("timeTaken", timeTaken),
		zap.String("timeTakenStr", timeTakenStr),
	)
}

// executeFirmwareRequest executes firmware request and response with hex payload
func (p *Provider) executeFirmwareRequest(msg *msgTY.Message) (string, error) {
	rxPL := msg.Payloads[0].Value.String()
	startTime := time.Now()

	// convert the received hex to matching struct format
	fwReq := &firmwareRequest{}
	err := toStruct(rxPL, fwReq)
	if err != nil {
		p.logger.Error("error on converting firmwareRequest", zap.String("payload", rxPL), zap.Error(err))
		return "", err
	}

	node, err := p.getNode(msg.GatewayID, msg.NodeID)
	if err != nil {
		p.logger.Error("error to get node details", zap.Any("msg", msg), zap.Error(err))
		return "", err
	}

	// Use the slice size advertised in CONFIG_RESPONSE. Do not re-guess per block.
	blockSize := p.getFotaSessionBlockSize(node)
	if blockSize <= 0 {
		blockSize = p.otaBlockSizeForNode(node)
	}
	if blockSize <= 0 {
		blockSize = defaultFirmwareBlockSize
	}
	if isFotaDisabled(node) {
		p.logger.Debug("FOTA disabled for node",
			zap.String("nodeId", node.ID),
			zap.String("msNodeId", node.NodeID),
		)
		return "", nil
	}

	var fwRaw *firmwareRaw
	if hasFotaScript(node) {
		scriptDisabled, err := p.isFotaScriptDisabled(node)
		if err != nil {
			return "", err
		}
		if scriptDisabled {
			p.logger.Debug("fota script disabled in data repository",
				zap.String("nodeId", node.ID),
				zap.String("fotaScript", getFotaScriptID(node)),
			)
			return "", nil
		}
		scriptRes, err := p.runFotaOnBlock(node, rxPL, fwReq.Type, fwReq.Version, fwReq.Block)
		if err != nil {
			return "", fmt.Errorf("fota onBlock failed: %w", err)
		}
		if scriptRes.ResponseHex != "" {
			p.logger.Debug("sending firmware block response from fota script",
				zap.String("nodeId", node.ID),
				zap.Uint16("block", fwReq.Block),
				zap.String("timeTaken", time.Since(startTime).String()),
			)
			return scriptRes.ResponseHex, nil
		}
		fwRaw, err = p.fetchFirmwareByID(scriptRes.FirmwareID, fwReq.Type, fwReq.Version, false, blockSize)
		if err != nil {
			return "", fmt.Errorf("error on getting firmware from fota onBlock: %w", err)
		}
	} else {
		fwRaw, err = p.fetchFirmware(node, fwReq.Type, fwReq.Version, true, blockSize)
		if err != nil {
			return "", fmt.Errorf("error on getting firmware. %s", err.Error())
		}
	}
	fwRaw.LastAccess = time.Now()

	bs := fwRaw.BlockSize
	if bs <= 0 {
		bs = blockSize
	}
	if bs <= 0 {
		bs = defaultFirmwareBlockSize
	}
	startAddr := int(fwReq.Block) * bs
	endAddr := startAddr + bs
	if endAddr > len(fwRaw.Data) {
		p.logger.Error("requested block is not available", zap.Int("startAddr", startAddr), zap.Int("endAddr", endAddr), zap.Int("maxAvailableAddr", len(fwRaw.Data)))
		return "", fmt.Errorf("requested block is not available: %v", endAddr)
	}
	chunk := fwRaw.Data[startAddr:endAddr]
	if len(chunk) != bs {
		return "", fmt.Errorf("firmware block %d slice length %d != blockSize %d", fwReq.Block, len(chunk), bs)
	}
	hexPL, err := p.encodeFirmwareBlockResponse(fwReq.Type, fwReq.Version, fwReq.Block, chunk)
	if err != nil {
		return "", err
	}
	if len(hexPL) != 2*(6+bs) {
		return "", fmt.Errorf("firmware block hex length %d, want %d (blockSize=%d)", len(hexPL), 2*(6+bs), bs)
	}
	if fwReq.Block == 0 || fwReq.Block%50 == 0 {
		p.logger.Debug("sending a firmware response",
			zap.Any("request", fwReq),
			zap.Int("blockSize", bs),
			zap.Int("responseBytes", 6+bs),
			zap.Int("responseHexLen", len(hexPL)),
			zap.String("timeTaken", time.Since(startTime).String()),
		)
	} else {
		p.logger.Debug("sending a firmware response", zap.Any("request", fwReq), zap.Int("blockSize", bs), zap.String("timeTaken", time.Since(startTime).String()))
	}

	p.updateFirmwareProgressStatus(node, int(fwReq.Block), len(fwRaw.Data), bs)

	return hexPL, nil
}

func packFirmwareBlockResponse(typeID, versionID, block uint16, data []byte) (string, error) {
	buf := make([]byte, 6+len(data))
	binary.LittleEndian.PutUint16(buf[0:2], typeID)
	binary.LittleEndian.PutUint16(buf[2:4], versionID)
	binary.LittleEndian.PutUint16(buf[4:6], block)
	copy(buf[6:], data)
	return hexENC.EncodeToString(buf), nil
}

// encodeFirmwareBlockResponse uses the same 22-byte toHex layout as the
// working 328 path when blockSize is 16.
func (p *Provider) encodeFirmwareBlockResponse(typeID, versionID, block uint16, data []byte) (string, error) {
	if len(data) == defaultFirmwareBlockSize {
		fwRes := firmwareResponse{Type: typeID, Version: versionID, Block: block}
		copy(fwRes.Data[:], data)
		return p.toHex(&fwRes)
	}
	return packFirmwareBlockResponse(typeID, versionID, block, data)
}

func sanitizeOtaHex(requestHex string) string {
	requestHex = strings.TrimSpace(requestHex)
	requestHex = strings.TrimPrefix(requestHex, "0x")
	requestHex = strings.TrimPrefix(requestHex, "0X")
	requestHex = strings.ReplaceAll(requestHex, " ", "")
	requestHex = strings.ReplaceAll(requestHex, "\r", "")
	requestHex = strings.ReplaceAll(requestHex, "\n", "")
	if len(requestHex)%2 == 1 {
		requestHex = requestHex[:len(requestHex)-1]
	}
	return requestHex
}

func validOtaBlockSize(n int) int {
	if n < 8 || n > maxFirmwareBlockSize || n%8 != 0 {
		return 0
	}
	return n
}

func otaBlockSizeFromRequest(requestHex string) int {
	return validOtaBlockSize(advertisedOtaBlockSize(requestHex))
}

// advertisedOtaBlockSize returns the protocol 3.1 size the node reported.
func advertisedOtaBlockSize(requestHex string) int {
	requestHex = sanitizeOtaHex(requestHex)
	if requestHex == "" {
		return 0
	}
	req := parseFotaRequest(requestHex, true)
	if raw, ok := req["blockSize"]; ok {
		n := int(converterUtils.ToInteger(raw))
		if n >= 8 && n%8 == 0 {
			return n
		}
	}
	b, err := hexENC.DecodeString(requestHex)
	if err != nil {
		return 0
	}
	if len(b) >= 11 && binary.LittleEndian.Uint16(b[8:10]) == 0x0103 {
		n := int(b[10])
		if n >= 8 && n%8 == 0 {
			return n
		}
	}
	for i := 0; i+2 < len(b); i++ {
		if b[i] == 0x03 && b[i+1] == 0x01 {
			n := int(b[i+2])
			if n >= 8 && n%8 == 0 {
				return n
			}
		}
	}
	return 0
}

func (p *Provider) rememberOtaBlockSize(node *nodeTY.Node, blockSize int) {
	if node == nil || node.Labels == nil {
		return
	}
	blockSize = validOtaBlockSize(blockSize)
	if blockSize <= 0 {
		return
	}
	want := fmt.Sprintf("%d", blockSize)
	if node.Labels.Get(LabelOtaBlockSize) == want {
		return
	}
	node.Labels.Set(LabelOtaBlockSize, want)
	p.setNodeLabels(node)
}

func (p *Provider) otaBlockSizeFromLabel(node *nodeTY.Node) int {
	if node == nil || node.Labels == nil {
		return 0
	}
	return validOtaBlockSize(int(converterUtils.ToInteger(node.Labels[LabelOtaBlockSize])))
}

func (p *Provider) otaBlockSizeForNode(node *nodeTY.Node) int {
	if n := p.getFotaSessionBlockSize(node); n > 0 {
		return n
	}
	n := p.otaBlockSizeFromLabel(node)
	if n > 0 {
		return n
	}
	if hasFotaScript(node) {
		return 0
	}
	return defaultFirmwareBlockSize
}

// resolveOtaBlockSize picks the node's OTA slice size.
// Priority: live CONFIG_REQUEST, script return, trusted label.
// Stock DualOptiboot falls back to 16. Custom FOTA must not advertise 16
// unless the node actually reported 16.
func (p *Provider) resolveOtaBlockSize(node *nodeTY.Node, requestHex string, fromReq, fromScript int) (int, error) {
	if fromReq <= 0 {
		fromReq = otaBlockSizeFromRequest(requestHex)
	}
	fromScript = validOtaBlockSize(fromScript)
	fromLabel := p.otaBlockSizeForNode(node)

	blockSize := fromReq
	if blockSize == 0 {
		blockSize = fromScript
	}
	if blockSize == 0 {
		blockSize = fromLabel
	}
	if blockSize == 0 && !hasFotaScript(node) {
		blockSize = defaultFirmwareBlockSize
	}

	if p.logger != nil {
		p.logger.Debug("resolved OTA block size",
			zap.String("payload", requestHex),
			zap.Int("payloadBytes", len(requestHex)/2),
			zap.Int("fromRequest", fromReq),
			zap.Int("fromScript", fromScript),
			zap.Int("fromLabel", p.otaBlockSizeFromLabel(node)),
			zap.Int("blockSize", blockSize),
		)
	}

	if blockSize == 0 {
		if advertised := advertisedOtaBlockSize(requestHex); advertised > maxFirmwareBlockSize {
			return 0, fmt.Errorf("node advertised OTA blockSize=%d; controller max is %d (raise maxFirmwareBlockSize if radio/MQTT allow it)", advertised, maxFirmwareBlockSize)
		}
		return 0, fmt.Errorf("unknown OTA block size: node did not advertise protocol 3.1 blockSize yet (do not default to 16)")
	}
	if fromReq == 0 && blockSize != defaultFirmwareBlockSize {
		p.rememberOtaBlockSize(node, blockSize)
	}
	return blockSize, nil
}

func firmwareRawCacheKey(fwID string, blockSize int) string {
	return fmt.Sprintf("%s#%d", fwID, blockSize)
}

// fetchFirmware looks requested firmware on memory store (stock path: assigned_firmware label),
// if not available, loads it from disk
func (p *Provider) fetchFirmware(node *nodeTY.Node, typeID, versionID uint16, verifyID bool, blockSize int) (*firmwareRaw, error) {
	fwID := node.Labels.Get(types.LabelNodeAssignedFirmware)
	if fwID == "" {
		return nil, fmt.Errorf("firmware not assigned for this node. gatewayId:%s, nodeId:%s, typeId:%d, versionId:%d",
			node.GatewayID, node.NodeID, typeID, versionID)
	}
	return p.fetchFirmwareByID(fwID, typeID, versionID, verifyID, blockSize)
}

// fetchFirmwareByID loads/caches firmware raw by entity id.
func (p *Provider) fetchFirmwareByID(fwID string, typeID, versionID uint16, verifyID bool, blockSize int) (*firmwareRaw, error) {
	if fwID == "" {
		return nil, fmt.Errorf("firmware id is empty")
	}
	if blockSize <= 0 {
		blockSize = defaultFirmwareBlockSize
	}
	cacheKey := firmwareRawCacheKey(fwID, blockSize)

	// lambda function to load firmware
	loadFirmwareRawFn := func() (*firmwareRaw, error) {
		fw, err := p.getFirmware(fwID)
		if err != nil {
			p.logger.Error("error to get firmware raw", zap.Any("fwID", fwID), zap.Error(err))
			return nil, err
		}

		// get mysensor specific ids
		if fw.Labels.Get(LabelFirmwareTypeID) == "" || fw.Labels.Get(LabelFirmwareVersionID) == "" {
			return nil, fmt.Errorf("firmware '%s' or '%s' labels are not set", LabelFirmwareTypeID, LabelFirmwareVersionID)
		}
		fwTypeID := uint16(fw.Labels.GetInt(LabelFirmwareTypeID))
		fwVersionID := uint16(fw.Labels.GetInt(LabelFirmwareVersionID))

		fwRaw, err := p.getFirmwareRaw(fw.ID, fwTypeID, fwVersionID, blockSize)
		if err != nil {
			p.logger.Error("error on getting firmware data", zap.String("firmwareId", fw.ID), zap.Error(err))
			return nil, err
		}

		// keep it on memory store
		fwRawStore.Add(cacheKey, fwRaw)
		return fwRaw, nil
	}

	// check firmware on memory store
	// if not found, load it from disk
	var fwRaw *firmwareRaw
	fwRawInf := fwRawStore.Get(cacheKey)
	if fwRawInf == nil {
		_fwRaw, err := loadFirmwareRawFn()
		if err != nil {
			return nil, err
		}
		fwRaw = _fwRaw
	} else {
		_fwRaw, ok := fwRawInf.(*firmwareRaw)
		if !ok {
			return nil, fmt.Errorf("error on converting target type. firmwareID: %s", fwID)
		}
		fwRaw = _fwRaw
	}

	if verifyID { // verify firmware ids
		if fwRaw.Type != typeID || fwRaw.Version != versionID {
			return nil, fmt.Errorf("requested firmware type id or version id not matching[Req, Avl], TypeId:[%v, %v], VersionId:[%v, %v]",
				typeID, fwRaw.Type, versionID, fwRaw.Version)
		}
	}

	return fwRaw, nil
}

// isIntelHexFile reports whether file bytes look like Intel HEX (leading ':').
func isIntelHexFile(data []byte) bool {
	for _, b := range data {
		if b == ' ' || b == '\t' || b == '\r' || b == '\n' {
			continue
		}
		return b == ':'
	}
	return false
}

// Source: https://en.wikipedia.org/wiki/Intel_HEX
// https://github.com/mycontroller-org/mycontroller-v1-legacy/blob/1.5.0.Final/modules/core/src/main/java/org/mycontroller/standalone/firmware/FirmwareUtils.java#L118
// https://github.com/mysensors/MySensorsSampleController/blob/9dbae76081a9c080d5fdd68fba9870626025343f/NodeJsController.js#L172
// I8HEX files use only record types 00 and 01 (16-bit addresses)
// 00 - data, 01 - End
//
// Also accepts raw binary (e.g. STM32 signed.bin) when content does not start with ':'.
func (p *Provider) hexByteToLocalFormat(typeID, versionID uint16, hexByte []byte, blockSize int) (*firmwareRaw, error) {
	if len(hexByte) == 0 {
		return nil, errors.New("no data available")
	}
	if blockSize <= 0 {
		return nil, fmt.Errorf("invalid blockSize: %d", blockSize)
	}

	// Raw binary path, not Intel HEX
	if !isIntelHexFile(hexByte) {
		return p.bytesToFirmwareRaw(typeID, versionID, hexByte, blockSize, false)
	}

	hexString := string(hexByte)
	hexString = strings.ReplaceAll(hexString, "\r", "") // remove all "\r" char
	hexLines := strings.Split(hexString, "\n")          // split as separate lines

	actualData := make([]byte, 0)
	for index, line := range hexLines {
		line = strings.TrimSpace(line) // remove spaces if any
		if len(line) == 0 {
			continue
		}
		// first char of the line should be ':'
		if line[0] != ':' {
			return nil, fmt.Errorf("hex line not started with the char ':', line number:%d, data:%s", index+1, line)
		}

		// we are not going to use byte count, address and checksum
		// ignore those fields
		// byte count => line[1:3]
		// address => line[3:7]

		recordType, err := strconv.ParseInt(line[7:9], 16, 64)
		if err != nil {
			return nil, err
		}

		if recordType != 0 {
			continue
		}

		// get only data bytes and convert to bytes from string bytes
		data := line[9 : len(line)-2]
		dataBytes, err := hexENC.DecodeString(data)
		if err != nil {
			p.logger.Error("failed", zap.Any("data", data), zap.Error(err))
			return nil, err
		}
		// include it to our main slice
		actualData = append(actualData, dataBytes...)
	}
	// check the processed bytes length
	if len(actualData) == 0 {
		return nil, errors.New("no data available")
	}

	// DualOptiboot / AVR: pad to 128-byte pages
	return p.bytesToFirmwareRaw(typeID, versionID, actualData, blockSize, true)
}

// bytesToFirmwareRaw builds firmwareRaw from a contiguous image.
// padToAVRPage: when true, pad to 128-byte pages (classic DualOptiboot); when false
// (raw binary), pad only to OTA blockSize with 0xFF.
func (p *Provider) bytesToFirmwareRaw(typeID, versionID uint16, data []byte, blockSize int, padToAVRPage bool) (*firmwareRaw, error) {
	actualData := make([]byte, len(data))
	copy(actualData, data)

	if padToAVRPage {
		// ATMega328 has 64 words per page / 128 bytes per page
		paddingCount := 128 - (len(actualData) % 128)
		for paddingCount > 0 {
			actualData = append(actualData, 255) // 255 => 0xFF
			paddingCount--
		}
	} else if rem := len(actualData) % blockSize; rem != 0 {
		for i := 0; i < blockSize-rem; i++ {
			actualData = append(actualData, 0xFF)
		}
	}

	nBlocks := len(actualData) / blockSize
	if nBlocks > 0xFFFF {
		return nil, fmt.Errorf("image too large for OTA block count: %d bytes (%d blocks)", len(actualData), nBlocks)
	}
	numberOfBlocks := uint16(nBlocks)

	// calculate crc
	// Source: https://github.com/mysensors/MySensorsBootloaderRF24/blob/37dcc50bf2825a2639fe904be8f3309df7b5859e/HW.h#L235
	crc := uint16(0xFFFF)
	for _, b := range actualData {
		crc ^= uint16(b)
		for bit := 0; bit < 8; bit++ {
			crc = (crc >> 1) ^ (-(crc & 1) & 0xA001)
		}
	}

	return &firmwareRaw{
		Type:       typeID,
		Version:    versionID,
		Data:       actualData,
		Blocks:     numberOfBlocks,
		CRC:        crc,
		BlockSize:  blockSize,
		LastAccess: time.Now(),
	}, nil
}

func (p *Provider) setNodeLabels(node *nodeTY.Node) {
	if p == nil || p.bus == nil || node == nil {
		return
	}
	// CommandSetLabel expects label map payload
	busUtils.PostToResourceService(p.logger, p.bus, node.ID, node.Labels, rsTY.TypeNode, rsTY.CommandSetLabel, "")
}

// toHex returns hex string
func (p *Provider) toHex(in interface{}) (string, error) {
	var bBuf bytes.Buffer
	err := binary.Write(&bBuf, binary.LittleEndian, in)
	if err != nil {
		return "", err
	}
	return hexENC.EncodeToString(bBuf.Bytes()), nil
}

// toStruct updates struct from hex string
func toStruct(hex string, out interface{}) error {
	hb, err := hexENC.DecodeString(hex)
	if err != nil {
		return err
	}
	r := bytes.NewReader(hb)
	return binary.Read(r, binary.LittleEndian, out)
}

// computeOtaProgressState returns the state to publish for a given served block.
// Pure function: no bus, no logger, no store access. Testable directly.
//
// The returned `arm` flag is true when the block is the start of a new OTA
// (block 0 in forward order, lastBlock in reverse order). The caller is
// responsible for arming FieldOTARunning in the node cache (if it wants to)
// and for publishing `state` on the bus.
//
// `ok == false` means the block must be ignored (late block after a clean
// close, or non-milestone block).
func computeOtaProgressState(node *nodeTY.Node, currentBlock, totalBytes, blockSize int) (state map[string]interface{}, arm bool, ok bool) {
	otaBlockOrder := node.Labels.Get(types.LabelNodeOTABlockOrder)
	if otaBlockOrder == "" {
		otaBlockOrder = OTABlockOrderReverse
	}
	if blockSize <= 0 {
		blockSize = defaultFirmwareBlockSize
	}

	totalBlocks := totalBytes / blockSize
	if totalBytes%blockSize != 0 {
		totalBlocks++
	}
	lastBlock := totalBlocks - 1

	var isStartCandidate bool
	if otaBlockOrder == OTABlockOrderReverse {
		isStartCandidate = (currentBlock == lastBlock)
	} else {
		isStartCandidate = (currentBlock == 0)
	}

	// Late block after a clean close: ignore, unless it is the start block
	// of a new OTA (which is allowed to re-arm).
	if !isStartCandidate {
		if !node.Others.GetBool(types.FieldOTARunning) &&
			int(converterUtils.ToInteger(node.Others[types.FieldOTAProgress])) >= 100 {
			return nil, false, false
		}
		if v := node.Others[types.FieldOTAEndTime]; v != nil && v != "" {
			return nil, false, false
		}
	}

	// Publish only on milestone blocks (every 10), on the first block, and
	// on the last block. Intermediate blocks would flood the bus.
	isMilestone := currentBlock == 0 ||
		currentBlock%10 == 0 ||
		currentBlock == lastBlock
	if !isMilestone {
		return nil, false, false
	}

	// Determine whether this block marks the end of the OTA.
	// The end is:
	//   - block 0 in reverse order (blocks are requested from lastBlock to 0)
	//   - block lastBlock in forward order (blocks are requested from 0 to lastBlock)
	// The start is the other end.
	//
	// Symmetric, order-agnostic progress calculation. The previous
	// implementation used 1 - currentBlock/lastBlock for reverse order,
	// which yielded 0% on the first block (currentBlock == lastBlock) and
	// 100% on the last block (currentBlock == 0). The bug appeared when the
	// node sent lastBlock as the LAST block (out-of-order reception),
	// leaving the state at 0% / running=true forever.
	var received int
	var isEnd bool
	var isStart bool

	switch otaBlockOrder {
	case OTABlockOrderReverse:
		received = lastBlock - currentBlock + 1
		isEnd = (currentBlock == 0)
		isStart = (currentBlock == lastBlock)
	default:
		received = currentBlock + 1
		isEnd = (currentBlock == lastBlock)
		isStart = (currentBlock == 0)
	}

	if received < 0 {
		received = 0
	}
	if received > totalBlocks {
		received = totalBlocks
	}

	percentage := 0
	if totalBlocks > 0 {
		percentage = (received * 100) / totalBlocks
	}
	if percentage > 100 {
		percentage = 100
	}

	now := time.Now()

	// The current block number only makes sense during the transfer.
	// On the end block, clear it so the UI does not show a stale "0".
	state = map[string]interface{}{
		types.FieldOTARunning:    !isEnd,
		types.FieldOTAProgress:   percentage,
		types.FieldOTAStatusOn:   now,
		types.FieldOTABlockTotal: totalBlocks,
	}
	if isEnd {
		state[types.FieldOTABlockNumber] = nil
	} else {
		state[types.FieldOTABlockNumber] = currentBlock
	}

	// On the start block, arm the OTA and stamp the start time. The caller
	// is responsible for actually setting FieldOTARunning in the node cache
	// (see updateFirmwareProgressStatus).
	if isStart {
		state[types.FieldOTAStartTime] = now
		arm = true
	}

	// On the end block, stamp the end time and compute the duration.
	if isEnd {
		state[types.FieldOTAEndTime] = now

		// If we just reached the end, also compute the duration so the UI
		// shows it immediately (rather than waiting for forceOtaCompletion).
		if startVal := node.Others[types.FieldOTAStartTime]; startVal != nil {
			var startParsed time.Time
			switch v := startVal.(type) {
			case time.Time:
				startParsed = v
			case string:
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					startParsed = t
				}
			case float64:
				if v > 0 {
					startParsed = time.Unix(int64(v), 0)
				}
			}
			if !startParsed.IsZero() {
				elapsed := int(now.Sub(startParsed).Seconds())
				if elapsed > 0 {
					state[types.FieldOTATimeTaken] = elapsed
					state[types.FieldOTATimeTakenStr] = formatDuration(elapsed)
				}
			}
		}
	}

	return state, arm, true
}

// updateFirmwareProgressStatus is a thin wrapper around computeOtaProgressState:
// it computes the state, arms the OTA in the cache if this is the start block,
// and publishes the state on the bus.
func (p *Provider) updateFirmwareProgressStatus(node *nodeTY.Node, currentBlock, totalBytes, blockSize int) {
	state, arm, ok := computeOtaProgressState(node, currentBlock, totalBytes, blockSize)
	if !ok {
		return
	}
	if arm {
		node.Others.Set(types.FieldOTARunning, true, nil)
	}
	busUtils.PostToResourceService(p.logger, p.bus, node.ID, state, rsTY.TypeNode, rsTY.CommandFirmwareState, "")
}
