package mysensors

import (
	"fmt"
	"strings"

	"github.com/mycontroller-org/server/v2/pkg/types"
	repositoryTY "github.com/mycontroller-org/server/v2/pkg/types/data_repository"
	eventTY "github.com/mycontroller-org/server/v2/pkg/types/event"
	firmwareTY "github.com/mycontroller-org/server/v2/pkg/types/firmware"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	topicTY "github.com/mycontroller-org/server/v2/pkg/types/topic"
	"github.com/mycontroller-org/server/v2/pkg/utils/concurrency"
	queueUtils "github.com/mycontroller-org/server/v2/pkg/utils/queue"
	busTY "github.com/mycontroller-org/server/v2/plugin/bus/types"
	"go.uber.org/zap"
)

const (
	eventQueueLimit       = 50
	defaultEventQueueName = "event_listener_gw"
)

var (
	eventsQueue                  *queueUtils.Queue
	firmwareSubscriptionID       = int64(0)
	nodeSubscriptionID           = int64(0)
	dataRepositorySubscriptionID = int64(0)

	// migratedNodesHash retains the last seen legacy value per node so we can
	// break the re-entry loop caused by async label persistence: an inbound
	// node event may still carry the old bootloader_ota_reboot value even
	// after setNodeLabels() has been called, because the store update has not
	// landed yet.
	migratedNodesHash = concurrency.NewStore() // nodeStoreID -> string
)

// initEventListener service
func (p *Provider) initEventListener(gatewayID string) error {
	firmwareEventQueueName := fmt.Sprintf("%s_%s", defaultEventQueueName, gatewayID)
	eventsQueue = queueUtils.New(p.logger, firmwareEventQueueName, eventQueueLimit, p.processServiceEvent, 1)
	// on message receive add it in to our local queue
	sID, err := p.bus.Subscribe(topicTY.TopicEventFirmware, p.onEvent)
	if err != nil {
		return err
	}
	firmwareSubscriptionID = sID
	sID, err = p.bus.Subscribe(topicTY.TopicEventNode, p.onEvent)
	if err != nil {
		return err
	}
	nodeSubscriptionID = sID
	sID, err = p.bus.Subscribe(topicTY.TopicEventDataRepository, p.onEvent)
	if err != nil {
		return err
	}
	dataRepositorySubscriptionID = sID
	return nil
}

// closeEventListener service
func (p *Provider) closeEventListener() {
	if firmwareSubscriptionID != 0 {
		topic := topicTY.TopicEventFirmware
		err := p.bus.Unsubscribe(topic, firmwareSubscriptionID)
		if err != nil {
			p.logger.Error("error on unsubscribe", zap.Error(err), zap.String("topic", topic))
		}
	}
	if nodeSubscriptionID != 0 {
		topic := topicTY.TopicEventNode
		err := p.bus.Unsubscribe(topic, nodeSubscriptionID)
		if err != nil {
			p.logger.Error("error on unsubscribe", zap.Error(err), zap.String("topic", topic))
		}
	}
	if dataRepositorySubscriptionID != 0 {
		topic := topicTY.TopicEventDataRepository
		err := p.bus.Unsubscribe(topic, dataRepositorySubscriptionID)
		if err != nil {
			p.logger.Error("error on unsubscribe", zap.Error(err), zap.String("topic", topic))
		}
	}
	eventsQueue.Close()
}

func (p *Provider) onEvent(data *busTY.BusData) {
	event := &eventTY.Event{}
	err := data.LoadData(event)
	if err != nil {
		p.logger.Warn("Failed to convert to target type", zap.Error(err))
		return
	}
	p.logger.Debug("Received an event", zap.Any("event", event))

	if (event.EntityType != types.EntityNode &&
		event.EntityType != types.EntityFirmware &&
		event.EntityType != types.EntityDataRepository) ||
		event.Entity == nil {
		return
	}

	p.logger.Debug("Event added into processing queue", zap.Any("event", event))
	status := eventsQueue.Produce(event)
	if !status {
		p.logger.Warn("Failed to store the event into queue", zap.Any("event", event))
	}
}

// processServiceEvent from the queue
func (p *Provider) processServiceEvent(item interface{}) error {
	event := item.(*eventTY.Event)
	p.logger.Debug("Processing a request", zap.Any("event", event))

	// process events
	switch event.EntityType {
	case types.EntityFirmware:
		firmware := firmwareTY.Firmware{}
		err := event.LoadEntity(&firmware)
		if err != nil {
			p.logger.Error("error on loading firmware entity", zap.String("eventQuickId", event.EntityQuickID), zap.Error(err))
			return nil // Don't requeue invalid events
		}
		prefix := firmware.ID + "#"
		for _, key := range fwRawStore.Keys() {
			if key == firmware.ID || strings.HasPrefix(key, prefix) {
				fwRawStore.Remove(key)
			}
		}
		fwStore.Remove(firmware.ID)

	case types.EntityNode:
		node := nodeTY.Node{}
		err := event.LoadEntity(&node)
		if err != nil {
			p.logger.Error("error on loading node entity", zap.String("eventQuickId", event.EntityQuickID), zap.Error(err))
			return nil // Don't requeue invalid events
		}
		// Initialise fota_update when a firmware is assigned, if not set yet.
		p.initFotaUpdateIfNeeded(&node)
		// Migrate the deprecated bootloader_ota_reboot label if present.
		p.migrateDeprecatedOtaRebootLabel(&node)

		localID := p.getNodeStoreID(node.GatewayID, node.NodeID)
		if nodeStore.IsAvailable(localID) {
			nodeStore.Add(localID, &node)
		}

	case types.EntityDataRepository:
		repo := repositoryTY.Config{}
		err := event.LoadEntity(&repo)
		if err != nil {
			p.logger.Error("error on loading data repository entity", zap.String("eventQuickId", event.EntityQuickID), zap.Error(err))
			return nil
		}
		// invalidate cached FOTA scripts
		if repo.ID != "" {
			fotaScriptStore.Remove(repo.ID)
		}

	default:
		p.logger.Info("received unsupported event", zap.Any("event", event))
	}
	return nil
}

// migrateDeprecatedOtaRebootLabel converts a legacy bootloader_ota_reboot
// label into the new fota_update flag, and removes it. Also renames
// bootloader_ota_reboot_delay to fota_delivery_delay when present.
//
// Rules:
//   - bootloader_ota_reboot=true  + MYSBootloader kind -> fota_update=offline
//   - bootloader_ota_reboot=true  + other kind         -> fota_update=online
//   - bootloader_ota_reboot=false                      -> fota_update=unknown
//
// fota_update is only set if not already present (user may have migrated
// manually).
//
// Loop safety: this function is called from processServiceEvent, which is
// itself triggered by node events. Since the migration writes to the node
// labels (via setNodeLabels), it re-emits a node event and re-enters this
// function. Because label persistence is asynchronous, the store may still
// carry the old bootloader_ota_reboot value on re-entry, so a pure "changed"
// check is not enough. We dedup with a per-node hash of the legacy values:
// if we see the same legacy state twice in a row, we assume persistence has
// not landed yet and skip. When the legacy labels are finally gone from the
// inbound event, we clear the dedup entry so a future re-add can migrate.
func (p *Provider) migrateDeprecatedOtaRebootLabel(node *nodeTY.Node) {
	if node == nil || node.Labels == nil {
		return
	}

	key := p.getNodeStoreID(node.GatewayID, node.NodeID)
	legacy := node.Labels.Get("bootloader_ota_reboot")
	legacyDelay := node.Labels.Get("bootloader_ota_reboot_delay")

	// Persistence has caught up: no more legacy labels. Clear the dedup
	// entry so a future re-add (manual or restore) can migrate again.
	if legacy == "" && legacyDelay == "" {
		migratedNodesHash.Remove(key)
		return
	}

	// Dedup: if we already processed this node with the same legacy state
	// and persistence has not landed yet, skip.
	hash := legacy + "|" + legacyDelay
	if prev := migratedNodesHash.Get(key); prev != nil {
		if prevStr, ok := prev.(string); ok && prevStr == hash {
			return
		}
	}
	migratedNodesHash.Add(key, hash)

	changed := false

	// Rename bootloader_ota_reboot_delay -> fota_delivery_delay
	if legacyDelay != "" && node.Labels.Get(LabelFotaDeliveryDelay) == "" {
		node.Labels.Set(LabelFotaDeliveryDelay, legacyDelay)
		node.Labels.Remove("bootloader_ota_reboot_delay")
		changed = true
	}

	// Migrate bootloader_ota_reboot -> fota_update
	if legacy != "" {
		if node.Labels.Get(LabelFotaUpdate) == "" {
			mode := FotaUpdateUnknown
			if strings.EqualFold(legacy, "true") || legacy == "1" {
				mode = FotaUpdateOnline
				if isMYSBootloaderKind(bootloaderKindForNode(node)) {
					mode = FotaUpdateOffline
				}
			}
			node.Labels.Set(LabelFotaUpdate, mode)
			// Explicit migration counts as a user choice: do not auto-recompute.
			node.Labels.Remove(LabelFotaUpdateAuto)
		}
		node.Labels.Remove("bootloader_ota_reboot")
		changed = true
	}

	if !changed {
		return
	}

	p.setNodeLabels(node)
	if p.logger != nil {
		p.logger.Info("migrated bootloader_ota_reboot to fota_update",
			zap.String("nodeId", node.NodeID),
			zap.String("gatewayId", node.GatewayID),
			zap.String("legacy", legacy),
			zap.String("fotaUpdate", node.Labels.Get(LabelFotaUpdate)),
		)
	}
}
