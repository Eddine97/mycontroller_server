package mysensors

import (
	"testing"

	"github.com/mycontroller-org/server/v2/pkg/types"
	"github.com/mycontroller-org/server/v2/pkg/types/cmap"
	nodeTY "github.com/mycontroller-org/server/v2/pkg/types/node"
	"go.uber.org/zap"
)

func TestDetectBootloaderKind(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want string
	}{
		{"MYSBootloader 1.0 (6 bytes)", "E70019004802", BootloaderKindMYSBootloader10},
		{"MYSBootloader 1.3", "E700190048025B0E0103", BootloaderKindMYSBootloader13},
		{"MYSBootloader 1.4", "E700190048025B0E0104", BootloaderKindMYSBootloader14},
		{"MYSBootloader 1.5", "E700190048025B0E0105", BootloaderKindMYSBootloader15},
		// [FIX n°1] Padding tolerance: gateway may extend the payload.
		{"MYSBootloader 1.5 padded to 11 bytes", "E700190048025B0E0105FF", BootloaderKindMYSBootloader15},
		{"MYSBootloader 1.5 padded to 13 bytes", "E700190048025B0E0105FFFFFF", BootloaderKindMYSBootloader15},
		{"Protocol 3.1 (11 bytes)", "E700190048025B0E010310", BootloaderKindProtocol31},
		// 18-byte official 3.1 length: BLVersion 1.3 at offset 8-9 is
		// coincidental; the blockSize at offset 10 (16 or 32) plus the
		// official length must win over MYSBootloader classification.
		{"Protocol 3.1 extended 18 bytes blockSize 16", "E700190048025B0E010310A10000000000AB", BootloaderKindProtocol31},
		{"Protocol 3.1 extended 18 bytes blockSize 32", "E700190048025B0E010320A10000000000AB", BootloaderKindProtocol31},
		{"unknown 10-byte", "E700190048025B0E00FF", BootloaderKindUnknown},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectBootloaderKind(tt.hex)
			if got != tt.want {
				t.Errorf("detectBootloaderKind(%q) = %q, want %q", tt.hex, got, tt.want)
			}
		})
	}
}

func TestMYSBootloaderKindFor(t *testing.T) {
	tests := []struct {
		major, minor int
		want         string
	}{
		{1, 0, BootloaderKindMYSBootloader10},
		{1, 3, BootloaderKindMYSBootloader13},
		{1, 4, BootloaderKindMYSBootloader14},
		{1, 5, BootloaderKindMYSBootloader15},
		{1, 6, "mysbootloader-1.6"},
		{2, 0, "mysbootloader-2.0"},
		{1, 42, BootloaderKindMYSBootloaderCompatible},
	}
	for _, tt := range tests {
		if got := mysBootloaderKindFor(tt.major, tt.minor); got != tt.want {
			t.Errorf("mysBootloaderKindFor(%d,%d) = %q, want %q",
				tt.major, tt.minor, got, tt.want)
		}
	}
}

func TestExtractBootloaderVersion(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want string
	}{
		{"1.3", "E700190048025B0E0103", "1.3"},
		{"1.4", "E700190048025B0E0104", "1.4"},
		{"1.5", "E700190048025B0E0105", "1.5"},
		{"1.5 padded", "E700190048025B0E0105FF", "1.5"},
		{"6 bytes no version", "E70019004802", ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBootloaderVersion(tt.hex)
			if got != tt.want {
				t.Errorf("extractBootloaderVersion(%q) = %q, want %q", tt.hex, got, tt.want)
			}
		})
	}
}

func TestIsMYSBootloaderKind(t *testing.T) {
	if !isMYSBootloaderKind("mysbootloader-1.4") {
		t.Fatal("1.4 must be MYSBootloader")
	}
	if !isMYSBootloaderKind("mysbootloader-1.5") {
		t.Fatal("1.5 must be MYSBootloader")
	}
	if isMYSBootloaderKind("protocol-3.1") {
		t.Fatal("protocol-3.1 must not be MYSBootloader")
	}
	if isMYSBootloaderKind("unknown") {
		t.Fatal("unknown must not be MYSBootloader")
	}
}

// TestRememberBootloaderKindRecomputesFotaUpdate verifies that detecting a
// MYSBootloader recomputes fota_update when it was auto-initialised (the
// node was assigned a firmware before the first CONFIG_REQUEST).
func TestRememberBootloaderKindRecomputesFotaUpdate(t *testing.T) {
	n := &nodeTY.Node{
		NodeID: "56", GatewayID: "gw",
		Labels: cmap.CustomStringMap{
			LabelFotaUpdate:     FotaUpdateOnline, // auto-initialised earlier
			LabelFotaUpdateAuto: "true",
		},
	}
	p := &Provider{logger: zap.NewNop()}
	p.rememberBootloaderKind(n, "E700190048025B0E0105")
	if n.Labels.Get(LabelBootloaderKind) != BootloaderKindMYSBootloader15 {
		t.Fatalf("kind not set: %q", n.Labels.Get(LabelBootloaderKind))
	}
	if n.Labels.Get(LabelBootloaderVersion) != "1.5" {
		t.Fatalf("version not set: %q", n.Labels.Get(LabelBootloaderVersion))
	}
	if n.Labels.Get(LabelFotaUpdate) != FotaUpdateOffline {
		t.Fatalf("fota_update must be recomputed to offline, got %q", n.Labels.Get(LabelFotaUpdate))
	}
}

// TestRememberBootloaderKindPreservesManualFotaUpdate verifies that a
// user-chosen fota_update is NOT overwritten by bootloader detection.
func TestRememberBootloaderKindPreservesManualFotaUpdate(t *testing.T) {
	n := &nodeTY.Node{
		NodeID: "56", GatewayID: "gw",
		Labels: cmap.CustomStringMap{
			LabelFotaUpdate: FotaUpdateOnline, // user-chosen (no auto marker)
		},
	}
	p := &Provider{logger: zap.NewNop()}
	p.rememberBootloaderKind(n, "E700190048025B0E0104")
	if n.Labels.Get(LabelFotaUpdate) != FotaUpdateOnline {
		t.Fatalf("manual fota_update must be preserved, got %q", n.Labels.Get(LabelFotaUpdate))
	}
}

// TestInitFotaUpdateIfNeeded exercises the auto-initialisation rule.
func TestInitFotaUpdateIfNeeded(t *testing.T) {
	tests := []struct {
		name     string
		labels   cmap.CustomStringMap
		wantMode string
		wantSet  bool
	}{
		{
			name:     "no firmware assigned -> no init",
			labels:   cmap.CustomStringMap{},
			wantMode: "",
			wantSet:  false,
		},
		{
			name: "firmware assigned + MYSBootloader -> offline",
			labels: cmap.CustomStringMap{
				types.LabelNodeAssignedFirmware: "fw1",
				LabelBootloaderKind:             BootloaderKindMYSBootloader14,
			},
			wantMode: FotaUpdateOffline,
			wantSet:  true,
		},
		{
			name: "firmware assigned + protocol-3.1 -> online",
			labels: cmap.CustomStringMap{
				types.LabelNodeAssignedFirmware: "fw1",
				LabelBootloaderKind:             BootloaderKindProtocol31,
			},
			wantMode: FotaUpdateOnline,
			wantSet:  true,
		},
		{
			name: "firmware assigned + unknown kind -> online",
			labels: cmap.CustomStringMap{
				types.LabelNodeAssignedFirmware: "fw1",
			},
			wantMode: FotaUpdateOnline,
			wantSet:  true,
		},
		{
			name: "fota_update already set (explicit unknown) -> no init",
			labels: cmap.CustomStringMap{
				types.LabelNodeAssignedFirmware: "fw1",
				LabelFotaUpdate:                 FotaUpdateUnknown,
				LabelBootloaderKind:             BootloaderKindMYSBootloader14,
			},
			wantMode: FotaUpdateUnknown,
			wantSet:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nodeTY.Node{NodeID: "56", GatewayID: "gw", Labels: tt.labels}
			p := &Provider{logger: zap.NewNop()}
			p.initFotaUpdateIfNeeded(n)
			if got := n.Labels.Get(LabelFotaUpdate); got != tt.wantMode {
				t.Fatalf("fota_update = %q, want %q", got, tt.wantMode)
			}
			if tt.wantSet && !n.Labels.GetBool(LabelFotaUpdateAuto) {
				t.Fatal("fota_update_auto must be set when auto-initialised")
			}
			if !tt.wantSet && n.Labels.GetBool(LabelFotaUpdateAuto) {
				t.Fatal("fota_update_auto must not be set on manual value")
			}
		})
	}
}
