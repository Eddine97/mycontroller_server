package mysensors

import (
	"encoding/binary"
	hexENC "encoding/hex"
	"fmt"
	"strings"

	"github.com/mycontroller-org/server/v2/pkg/types"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	"go.uber.org/zap"
)

// Labels set on nodes to remember the detected bootloader.
const (
	LabelBootloaderKind    = "bootloader_kind"    // e.g. "mysbootloader-1.4", "protocol-3.1"
	LabelBootloaderVersion = "bootloader_version" // e.g. "1.4" (MYSBootloader only)
)

// Bootloader kinds recognised by the controller.
const (
	BootloaderKindMYSBootloader10 = "mysbootloader-1.0"
	BootloaderKindMYSBootloader13 = "mysbootloader-1.3"
	BootloaderKindMYSBootloader14 = "mysbootloader-1.4"
	BootloaderKindMYSBootloader15 = "mysbootloader-1.5" // RF24 on STM32
	BootloaderKindProtocol31      = "protocol-3.1"
	BootloaderKindUnknown         = "unknown"

	// BootloaderKindMYSBootloaderCompatible is the fallback for a plausible
	// MYSBootloader version that has no named constant (minor > 9, or an
	// unexpected major).
	BootloaderKindMYSBootloaderCompatible = "mysbootloader-compatible"
)

// isMYSBootloaderKind reports whether a kind string designates any MYSBootloader.
func isMYSBootloaderKind(kind string) bool {
	return strings.HasPrefix(kind, "mysbootloader")
}

// mysBootloaderKindFor returns the canonical kind string for a MYSBootloader
// version. Known versions use named constants; unknown-but-plausible versions
// fall back to a formatted string so future bootloaders still classify.
//
// Version encoding: BLVersion is a little-endian uint16, packed as
// (minor << 8) | major. That is why the caller passes (major, minor) after
// having split the bytes.
func mysBootloaderKindFor(major, minor int) string {
	switch {
	case major == 1 && minor == 0:
		return BootloaderKindMYSBootloader10
	case major == 1 && minor == 3:
		return BootloaderKindMYSBootloader13
	case major == 1 && minor == 4:
		return BootloaderKindMYSBootloader14
	case major == 1 && minor == 5:
		return BootloaderKindMYSBootloader15
	case minor >= 0 && minor <= 9:
		return fmt.Sprintf("mysbootloader-%d.%d", major, minor)
	}
	return BootloaderKindMYSBootloaderCompatible
}

// detectBootloaderKind classifies the bootloader from a ST_FIRMWARE_CONFIG_REQUEST
// payload. Uses payload length and (for the 10-byte format) the MYSBootloader
// BLVersion field.
//
// Signatures:
//
//	6  bytes : MYSBootloader < 1.3 (type, version, blocks)
//	10 bytes : MYSBootloader >= 1.3 (type, version, blocks, crc, BLVersion)
//	11 bytes : protocol 3.1 (10 bytes + blockSize)
//	12+ bytes: protocol 3.1 extended (custom)
//
// Robustness: MYSBootloader 1.5 (RF24 STM32) is structurally identical to
// 1.3/1.4 but the surrounding gateway may pad the payload. BLVersion is
// authoritative when it looks like a MYSBootloader version (major 1..9,
// minor 0..9). Protocol 3.1 is only claimed when the payload has an official
// 3.1 length (11, 12 or 18) OR BLVersion is not a plausible MYSBootloader.
func detectBootloaderKind(requestHex string) string {
	b, err := hexENC.DecodeString(sanitizeOtaHex(requestHex))
	if err != nil || len(b) == 0 {
		return ""
	}
	if len(b) == 6 {
		return BootloaderKindMYSBootloader10
	}
	if len(b) < 10 {
		return BootloaderKindUnknown
	}

	blv := binary.LittleEndian.Uint16(b[8:10])
	major := int(blv & 0xFF)
	minor := int((blv >> 8) & 0xFF)
	mysPlausible := major >= 1 && major <= 9 && minor <= 9

	p31Plausible := len(b) > 10 && b[10] >= 8 && b[10]%8 == 0
	officialP31Len := len(b) == 11 || len(b) == 12 || len(b) == 18

	if p31Plausible && (officialP31Len || !mysPlausible) {
		return BootloaderKindProtocol31
	}
	if mysPlausible {
		return mysBootloaderKindFor(major, minor)
	}
	return BootloaderKindUnknown
}

// extractBootloaderVersion returns a human string like "1.4" for MYSBootloader,
// or "" if the version cannot be determined.
func extractBootloaderVersion(requestHex string) string {
	b, err := hexENC.DecodeString(sanitizeOtaHex(requestHex))
	if err != nil || len(b) < 10 {
		return ""
	}
	blv := binary.LittleEndian.Uint16(b[8:10])
	major := int(blv & 0xFF)
	minor := int((blv >> 8) & 0xFF)
	if major < 1 || major > 9 || minor > 9 {
		return ""
	}
	return fmt.Sprintf("%d.%d", major, minor)
}

// rememberBootloaderKind persists the detected bootloader in the node labels.
// Does nothing if nothing changed, to avoid unnecessary writes.
//
// Since the fota_update rework, this function NO LONGER forces
// bootloader_ota_reboot=true. The intent is carried by the fota_update flag
// instead, and the auto-initialisation lives in initFotaUpdateIfNeeded. When
// the bootloader kind is refined and the fota_update value was set by the
// controller (fota_update_auto=true), the flag is recomputed so the node
// switches to offline once a MYSBootloader is detected.
func (p *Provider) rememberBootloaderKind(node *nodeTY.Node, requestHex string) {
	if node == nil || node.Labels == nil || requestHex == "" {
		return
	}
	kind := detectBootloaderKind(requestHex)
	if kind == "" {
		return
	}
	version := extractBootloaderVersion(requestHex)

	changed := false
	if node.Labels.Get(LabelBootloaderKind) != kind {
		node.Labels.Set(LabelBootloaderKind, kind)
		changed = true
	}
	if version != "" && node.Labels.Get(LabelBootloaderVersion) != version {
		node.Labels.Set(LabelBootloaderVersion, version)
		changed = true
	}

	if changed {
		p.setNodeLabels(node)
		if p.logger != nil {
			p.logger.Info("detected bootloader",
				zap.String("nodeId", node.NodeID),
				zap.String("gatewayId", node.GatewayID),
				zap.String("kind", kind),
				zap.String("version", version),
				zap.Int("payloadBytes", len(requestHex)/2),
			)
		}
	}

	// If fota_update was auto-initialised (and not overridden by the user),
	// recompute it now that we know the kind. This handles the case where the
	// firmware was assigned before the first ST_FIRMWARE_CONFIG_REQUEST.
	if node.Labels.GetBool(LabelFotaUpdateAuto) {
		p.recomputeFotaUpdateFromKind(node)
	}
}

// recomputeFotaUpdateFromKind updates fota_update from bootloader_kind when
// the value was auto-initialised. Manual user choices are preserved (the
// fota_update_auto marker is cleared on manual change).
func (p *Provider) recomputeFotaUpdateFromKind(node *nodeTY.Node) {
	if node == nil || node.Labels == nil {
		return
	}
	mode := FotaUpdateOnline
	if isMYSBootloaderKind(bootloaderKindForNode(node)) {
		mode = FotaUpdateOffline
	}
	if node.Labels.Get(LabelFotaUpdate) == mode {
		return
	}
	node.Labels.Set(LabelFotaUpdate, mode)
	p.setNodeLabels(node)
	if p.logger != nil {
		p.logger.Info("recomputed fota_update from bootloader_kind",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.String("bootloaderKind", bootloaderKindForNode(node)),
			zap.String("fotaUpdate", mode),
		)
	}
}

// bootloaderKindForNode returns the persisted bootloader kind for a node, or "".
func bootloaderKindForNode(node *nodeTY.Node) string {
	if node == nil || node.Labels == nil {
		return ""
	}
	return node.Labels.Get(LabelBootloaderKind)
}

// IsMYSBootloader reports whether the node's persisted bootloader kind
// designates any MYSBootloader variant.
func (p *Provider) IsMYSBootloader(node *nodeTY.Node) bool {
	return isMYSBootloaderKind(bootloaderKindForNode(node))
}

// BootloaderKind returns the persisted bootloader kind for a node, or "".
func (p *Provider) BootloaderKind(node *nodeTY.Node) string {
	return bootloaderKindForNode(node)
}

// initFotaUpdateIfNeeded initialises fota_update the first time a firmware is
// assigned to a node, based on bootloader_kind:
//   - MYSBootloader -> offline
//   - any other kind (including unknown / protocol-3.1) -> online
//
// It does nothing if fota_update is already set (even to "unknown" explicitly:
// the user has made a choice). The value is tagged with fota_update_auto so it
// can be recomputed when the bootloader kind is refined later.
func (p *Provider) initFotaUpdateIfNeeded(node *nodeTY.Node) {
	if node == nil || node.Labels == nil {
		return
	}
	if strings.TrimSpace(node.Labels.Get(LabelFotaUpdate)) != "" {
		return // already set, respect user's choice (including explicit "unknown")
	}
	if strings.TrimSpace(node.Labels.Get(types.LabelNodeAssignedFirmware)) == "" {
		return // no firmware assigned
	}
	mode := FotaUpdateOnline
	if isMYSBootloaderKind(bootloaderKindForNode(node)) {
		mode = FotaUpdateOffline
	}
	node.Labels.Set(LabelFotaUpdate, mode)
	node.Labels.Set(LabelFotaUpdateAuto, "true")
	p.setNodeLabels(node)
	if p.logger != nil {
		p.logger.Info("fota_update auto-initialised",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.String("bootloaderKind", bootloaderKindForNode(node)),
			zap.String("fotaUpdate", mode),
		)
	}
}
